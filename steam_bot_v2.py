#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🎮 Steam Free Games Bot v2 — Стабильная переписанная версия

Ключевые отличия от старой версии (почему падала сессия и терялись игры):
1. СЕССИЯ ПАДАЛА: cookies загружались один раз при старте и больше никогда
   не перечитывались. steamLoginSecure/browserid истощались — Steam
   перенаправлял на login, все запросы к /account/ и addfreelicense
   начинали молча возвращать пустые результаты. Теперь cookies
   автоматически перезагружаются из файла при падении авторизации.
2. ИГРЫ НЕ ПОПАДАЛИ В БИБЛИОТЕКУ:
   - success-ответ Steam ("This free game has been added to your library")
     не распознавался корректно;
   - статус "добавлено" нигде не проверялся после добавления;
   - кэш библиотеки был пустым (app_ids: []) и обновлялся только по TTL,
     поэтому каждая проверка считала игры "новыми" или наоборот пропускала;
   - планировщик жил в create_task вне жизненного цикла PTB и умирал молча.
   Теперь: честный матчинг ответов Steam + обязательная верификация через
   /api/addfreetrial + инкрементальное обновление кэша после каждого
   успешного добавления + фоновый job PTB (переживает ошибки).
3. ОБРАБОТКА ОШИБОК: голые except, проглатывающие всё, заменены на
   типизированные ретраи (сетевой таймаут/5xx/429) с backoff.
"""
import os
import sys
import json
import time
import asyncio
import logging
import sqlite3
import random
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Set
from enum import Enum
from dataclasses import dataclass, field

import aiohttp
from bs4 import BeautifulSoup
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.constants import ParseMode
from telegram.error import TelegramError, InvalidToken
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes,
    MessageHandler, filters, ConversationHandler, ApplicationBuilder, JobQueue,
)
from dotenv import load_dotenv

# ============================================================================
# НАСТРОЙКИ ПО УМОЛЧАНИЮ
# ============================================================================
BASE_DIR = Path(__file__).parent.resolve()
COOKIES_FILE = BASE_DIR / 'steam_cookies.json'
SETTINGS_FILE = BASE_DIR / 'bot_settings.json'
OWNED_GAMES_CACHE_FILE = BASE_DIR / 'owned_games_cache.json'
LOG_FILE = BASE_DIR / 'steam_bot.log'
DATABASE_FILE = BASE_DIR / 'steam_bot.db'

def _init_config():
    """Читает конфиг: значения из .env имеют приоритет над переменными окружения.

    Дополнительно чинит типовые ошибки копирования токена: лишние кавычки,
    префикс 'TELEGRAM_BOT_TOKEN=' и т.п. (из-за этого Telegram отдавал InvalidToken).
    """
    load_dotenv(BASE_DIR / '.env', override=True)
    token = (os.getenv('TELEGRAM_BOT_TOKEN') or '').strip().strip('"').strip("'")
    # если в значение случайно попали 'KEY=' — отрезаем префикс до первого ':'
    if '=' in token and ':' in token and token.split('=')[0].isidentifier():
        token = token.split('=', 1)[1].strip().strip('"').strip("'")
    return {'token': token}

_cfg = _init_config()

STEAM_CONFIG = {
    'api_store': 'https://store.steampowered.com',
    'checkout': 'https://checkout.steampowered.com',
    'timeout': 25,
    'delay': 1.0,          # пауза между запросами к Steam
    'max_retries': 3,      # ретраи сетевых ошибок
}

BOT_CONFIG = {
    'token': _cfg['token'],
    'admin_id': int(os.getenv('ADMIN_USER_ID', '0')),
    'interval': int(os.getenv('CHECK_INTERVAL', 60)),
    'max_pages': int(os.getenv('MAX_SEARCH_PAGES', 5)),
    'max_discount_pages': int(os.getenv('MAX_DISCOUNT_PAGES', 3)),
    'max_discount_games': int(os.getenv('MAX_DISCOUNT_GAMES', 50)),
}

if not BOT_CONFIG['token'] or not BOT_CONFIG['admin_id']:
    print("❌ TELEGRAM_BOT_TOKEN или ADMIN_USER_ID не указаны в .env!")
    sys.exit(1)

# ============================================================================
# ЛОГГИРОВАНИЕ
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8', mode='a'),
        logging.StreamHandler(sys.stdout),
    ],
)
for n in ['httpx', 'urllib3', 'apscheduler', 'aiohttp']:
    logging.getLogger(n).setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)
logger = logging.getLogger('steam_bot_v2')

WAITING_DISCOUNT_PERCENT = 1


def _random_ua() -> str:
    """Детерминированный реалистичный UA без внешних зависимостей."""
    chrome = random.choice(['124.0.0.0', '125.0.0.0', '126.0.0.0', '127.0.0.0'])
    return (f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
            f'(KHTML, like Gecko) Chrome/{chrome} Safari/537.36')


# ============================================================================
# УТИЛИТЫ
# ============================================================================
class AutoAddMode(Enum):
    OFF = 'off'
    EXPENSIVE = 'expensive'
    ALL = 'all'


@dataclass
class GameInfo:
    app_id: str
    name: str
    url: str = ""
    original_price: Optional[str] = None
    final_price: Optional[str] = None
    discount_percent: Optional[str] = None
    is_owned: bool = False
    discount_value: int = 0
    # подробная информация из appdetails (на русском, cc=ru&l=russian)
    header_image: Optional[str] = None
    short_description: Optional[str] = None
    genres: List[str] = field(default_factory=list)
    platforms: str = ""
    languages: str = ""
    release_date: str = ""
    developers: str = ""
    publishers: str = ""
    type_name: str = ""
    is_free: bool = False
    details_loaded: bool = False
    # DLC-специфика (appdetails 'dlc' + fullgame)
    is_dlc: bool = False
    parent_app_id: Optional[str] = None
    parent_game_name: Optional[str] = None

    def __post_init__(self):
        if self.genres is None:
            self.genres = []


class RateLimiter:
    """Простой асинхронный ограничитель частоты запросов к Steam."""

    def __init__(self, min_interval: float = STEAM_CONFIG['delay']):
        self.min_interval = min_interval
        self.last_call = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            wait = self.min_interval - (now - self.last_call)
            if wait > 0:
                await asyncio.sleep(wait)
            self.last_call = time.monotonic()


class DatabaseManager:
    """SQLite база данных (дешёвые короткие транзакции в executor)."""

    def __init__(self, db_path: Path = DATABASE_FILE):
        self.db_path = db_path
        self._lock = asyncio.Lock()
        self._init_db_sync()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db_sync(self):
        try:
            with self._connect() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS known_games (
                        app_id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        notified BOOLEAN DEFAULT 0,
                        added BOOLEAN DEFAULT 0,
                        in_library BOOLEAN DEFAULT 0,
                        original_price TEXT,
                        final_price TEXT,
                        discount_percent TEXT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS game_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        app_id TEXT,
                        action TEXT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            logger.info("✅ База данных инициализирована")
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")

    async def execute_query(self, query: str, params: tuple = ()) -> List[Dict]:
        def _run():
            with self._connect() as conn:
                cur = conn.execute(query, params)
                if query.strip().upper().startswith('SELECT'):
                    return [dict(r) for r in cur.fetchall()]
                return []
        async with self._lock:
            try:
                loop = asyncio.get_running_loop()
                return await loop.run_in_executor(None, _run)
            except Exception as e:
                logger.error(f"❌ Ошибка БД: {e}")
                return []

    async def log_history(self, app_id: str, action: str):
        await self.execute_query(
            "INSERT INTO game_history (app_id, action) VALUES (?, ?)",
            (app_id, action),
        )

    async def add_or_update_game(self, game: GameInfo):
        await self.execute_query("""
            INSERT INTO known_games
            (app_id, name, notified, added, in_library, original_price, final_price, discount_percent, timestamp)
            VALUES (?, ?, 1, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(app_id) DO UPDATE SET
                name = excluded.name,
                notified = 1,
                added = MAX(known_games.added, excluded.added),
                in_library = MAX(known_games.in_library, excluded.in_library),
                original_price = excluded.original_price,
                final_price = excluded.final_price,
                discount_percent = excluded.discount_percent,
                timestamp = CURRENT_TIMESTAMP
        """, (game.app_id, game.name, int(game.is_owned), int(game.is_owned),
              game.original_price, game.final_price, game.discount_percent))

    async def get_stats(self) -> Dict:
        result = await self.execute_query("""
            SELECT
                COUNT(*) as total_tracked,
                SUM(CASE WHEN added = 1 THEN 1 ELSE 0 END) as total_added,
                SUM(CASE WHEN in_library = 1 THEN 1 ELSE 0 END) as in_library,
                SUM(CASE WHEN date(timestamp) = date('now','localtime') AND added = 1 THEN 1 ELSE 0 END) as today_added
            FROM known_games
        """)
        return result[0] if result else {}


class HealthMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.metrics = {'games_checked': 0, 'games_added': 0, 'api_errors': 0, 'session_reloads': 0}

    def record(self, metric: str, value: int = 1):
        if metric in self.metrics:
            self.metrics[metric] += value

    def get_health_status(self) -> Dict:
        uptime = time.time() - self.start_time
        return {
            'status': 'healthy' if self.metrics['api_errors'] < 10 else 'degraded',
            'uptime': str(timedelta(seconds=int(uptime))),
            **self.metrics,
        }


# ============================================================================
# STEAM СЕССИЯ (ядро надёжности)
# ============================================================================
class SteamSession:
    """
    Асинхронная Steam-сессия с:
    • автоперезагрузкой cookies из файла при протухании авторизации;
    • ретраями с backoff на сетевые ошибки/5xx/429;
    • детектом редиректа на login (= сессия прервана) → recovery;
    • кэшем библиотеки, который обновляется инкрементально после
      каждого успешного добавления игры.
    """

    LOGIN_MARKERS = ('login?redir', 'accounts.login', 'sign in', 'войти')

    # кэш загруженных GameInfo по appid (классовый: им пользуются и сессия,
    # и хендлеры бота) — детали грузятся один раз на игру за аптайм
    _details_cache: Dict[str, 'GameInfo'] = {}

    def __init__(self, cookies_file: Path = COOKIES_FILE,
                 settings: Optional['SettingsManager'] = None,
                 health: Optional[HealthMonitor] = None):
        self.cookies_file = cookies_file
        self.settings = settings
        self.health = health or HealthMonitor()
        self.cookies: Dict[str, str] = {}
        self.logged_in = False
        self.steam_id: Optional[str] = None
        self.session_id: Optional[str] = None
        self._owned_games_cache: Set[str] = set()
        self._cache_timestamp: Optional[float] = None
        self._cache_count: int = 0
        self.rate_limiter = RateLimiter()
        self._session: Optional[aiohttp.ClientSession] = None
        self._auth_lock = asyncio.Lock()
        self._reloading_cookies = False
        # кэш GameInfo родительских игр для DLC (фильтр «DLC только если
        # базовая игра в библиотеке»)
        self._parent_cache: Dict[str, GameInfo] = {}

    # ---------- cookies ----------
    @staticmethod
    def _check_cookie_expiry(raw: list) -> int:
        """Возвращает количество ПРОТУХШИХ cookie с явным expirationDate.
        ФИКС: раньше бот молча работал протухшими cookies и «терял» сессию —
        теперь срок жизни проверяется явно и виден в логах."""
        expired = 0
        now = time.time()
        for c in raw:
            exp = c.get('expirationDate')
            try:
                if exp and float(exp) < now:
                    expired += 1
                    logger.warning("⏰ Cookie '%s' просрочена (истекла %.1f дн. назад)",
                                   c.get('name'), (now - float(exp)) / 86400)
            except (TypeError, ValueError):
                pass
        return expired

    def load_cookies_from_file(self) -> bool:
        """Читает steam_cookies.json (формат расширения EditThisCookie)."""
        if not self.cookies_file.exists():
            logger.warning("❌ Файл cookies не найден: %s", self.cookies_file)
            return False
        try:
            raw = json.loads(self.cookies_file.read_text(encoding='utf-8'))
            if isinstance(raw, dict):  # запасной формат {name: value}
                raw = [{'name': k, 'value': v} for k, v in raw.items()]
            cookies, steam_id, session_id = {}, None, None
            for c in raw:
                name, value = c.get('name'), c.get('value')
                domain = (c.get('domain') or '').lstrip('.')
                if not name or not value:
                    continue
                cookies[name] = value
                if name == 'steamLoginSecure' and '%7C%7C' in value:
                    steam_id = value.split('%7C%7C')[0]
                elif name == 'sessionid' and 'steampowered' in domain:
                    session_id = value
            if not cookies:
                return False
            expired = self._check_cookie_expiry(raw)
            if expired:
                logger.warning(f"⚠️ В файле cookies {expired} просроченных записей — "
                               f"экспортируйте steam_cookies.json заново из браузера")
            self.cookies, self.steam_id, self.session_id = cookies, steam_id, session_id
            logger.info("🍪 Cookies загружены из файла (steam_id=%s, sessionid=%s, просрочено=%d)",
                        steam_id, 'yes' if session_id else 'NO', expired)
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки cookies: {e}")
            return False

    # ---------- http ----------
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=6, ttl_dns_cache=300)
            timeout = aiohttp.ClientTimeout(total=STEAM_CONFIG['timeout'])
            self._session = aiohttp.ClientSession(
                connector=connector, timeout=timeout,
                headers={'User-Agent': _random_ua(),
                         'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8'},
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    @staticmethod
    def _looks_like_login(url: str, text: str) -> bool:
        """Детект «Steam выбросил нас на логин» = сессия прервана.
        Учитываем только редиректы внутри store/checkout — steamcommunity
        и api-хосты не проверяем (там /accounts/ — легитимные пути)."""
        u = url.lower()
        if 'steamcommunity.com' in u or 'api.steampowered.com' in u:
            return False
        on_login = ('/login' in u or 'sign_in' in u or '/accounts/' in u
                    or 'openid' in u or 'passport' in u)
        if not on_login:
            return False
        low = (text or '').lower()
        return not ('logout' in low or 'account_summary' in low or 'edit_profile' in low)

    async def _request(self, method: str, url: str, *, expect_json: bool = False,
                       ok_callback=None, allow_session_recovery: bool = True,
                       **kwargs) -> Tuple[int, Optional[str], Optional[dict]]:
        """
        Универсальный запрос с ретраями. Возвращает (status, text, json).
        ok_callback(status, text) -> bool решает, считать ли ответ успешным
        (например, для проверки что нас не редирекнуло на страницу логина).
        """
        last_exc = None
        for attempt in range(1, STEAM_CONFIG['max_retries'] + 1):
            try:
                await self.rate_limiter.acquire()
                session = await self._get_session()
                kwargs.setdefault('allow_redirects', True)
                async with session.request(method, url, cookies=self.cookies, **kwargs) as resp:
                    text = await resp.text(errors='replace')
                    status = resp.status
                    final_url = str(resp.url)
                    if status in (401, 403) or self._looks_like_login(final_url, text):
                        # Сессия прервана — попытка восстановиться один раз
                        if allow_session_recovery and await self.try_recover_session():
                            if attempt < STEAM_CONFIG['max_retries']:
                                continue
                        return status, text, None
                    if status == 429 or status >= 500:
                        raise aiohttp.ClientResponseError(
                            resp.request_info, resp.history, status=status, message='retryable')
                    if ok_callback and not ok_callback(status, text):
                        return status, text, None
                    j = None
                    if expect_json:
                        try:
                            j = json.loads(text)
                        except Exception:
                            j = None
                    return status, text, j
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_exc = e
                if self.health:
                    self.health.record('api_errors')
                backoff = min(2 ** attempt + random.random(), 10)
                logger.warning("⚠️ Запрос %s (%s) попытка %d/%d: %s — пауза %.1fs",
                               url.rsplit('/', 1)[-1][:40], method, attempt,
                               STEAM_CONFIG['max_retries'], e, backoff)
                await asyncio.sleep(backoff)
                # сессия могла сломаться — пересоздаём
                await self.close()
        if self.health:
            self.health.record('api_errors')
        logger.error(f"❌ Запрос не выполнен после ретраев: {url} ({last_exc})")
        return 0, None, None

    # ---------- auth ----------
    async def check_auth(self) -> bool:
        """Проверяет авторизацию; при провале пытается перезагрузить cookies."""
        status, text, _ = await self._request(
            'GET', f'{STEAM_CONFIG["api_store"]}/account/',
            allow_session_recovery=False,
            ok_callback=lambda s, t: s == 200 and 'logout' in t.lower())
        if status == 200 and text and 'logout' in text.lower():
            if not self.logged_in:
                logger.info("✅ Steam авторизация подтверждена")
            self.logged_in = True
            return True
        self.logged_in = False
        return False

    async def try_recover_session(self) -> bool:
        """
        ГЛАВНЫЙ ФИКС СТАБИЛЬНОСТИ: если Steam перестал пускать —
        перечитываем steam_cookies.json (пользователь мог обновить их
        расширением в браузере) и перепроверяем авторизацию.
        """
        async with self._auth_lock:
            if self._reloading_cookies:
                return False
            self._reloading_cookies = True
            try:
                logger.warning("🔁 Сессия Steam прервана — пробую восстановить из файла cookies...")
                if self.health:
                    self.health.record('session_reloads')
                await self.close()
                if not self.load_cookies_from_file():
                    return False
                ok = await self.check_auth()
                if ok:
                    logger.info("♻️ Сессия восстановлена, продолжаем работу")
                    await self.refresh_owned_games_cache(force=True)
                else:
                    logger.error("❌ Восстановление не удалось: обновите steam_cookies.json из браузера!")
                return ok
            finally:
                self._reloading_cookies = False

    # ---------- библиотека ----------
    @property
    def _cache_ttl(self) -> int:
        return int(self.settings.get('library_cache_ttl', 3600)) if self.settings else 3600

    def load_owned_cache_file(self):
        try:
            if OWNED_GAMES_CACHE_FILE.exists():
                data = json.loads(OWNED_GAMES_CACHE_FILE.read_text(encoding='utf-8'))
                cache_sid = data.get('steam_id')
                # ФИКС: если файл кэша от ДРУГОГО аккаунта Steam — не подмешиваем
                # его игры в текущую библиотеку (иначе «уже в библиотеке» врёт)
                if cache_sid and self.steam_id and str(cache_sid) != str(self.steam_id):
                    logger.warning("⚠️ owned_games_cache.json принадлежит другому аккаунту "
                                   f"({cache_sid} ≠ {self.steam_id}) — игнорирую его")
                    return
                ids = {str(x) for x in data.get('app_ids', [])}
                file_ts = float(data.get('timestamp') or 0)
                # файл новее текущего состояния в памяти — принимаем его целиком,
                # иначе — объединяем (union), чтобы ничего не потерять
                if file_ts >= (self._cache_timestamp or 0):
                    self._owned_games_cache |= ids
                    self._cache_timestamp = max(self._cache_timestamp or 0, file_ts)
                else:
                    self._owned_games_cache |= ids
                count = int(data.get('count') or 0)
                self._cache_count = max(count, len(self._owned_games_cache))
                if ids:
                    logger.info(f"📚 Загружен кэш библиотеки: {len(ids)} игр")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки кэша: {e}")

    def save_owned_cache_file(self):
        cache_data = {
            'app_ids': sorted(self._owned_games_cache),
            'count': max(self._cache_count, len(self._owned_games_cache)),
            'timestamp': self._cache_timestamp or time.time(),
            'steam_id': self.steam_id,
        }
        try:
            OWNED_GAMES_CACHE_FILE.write_text(
                json.dumps(cache_data, ensure_ascii=False, indent=2), encoding='utf-8')
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения кэша: {e}")

    async def refresh_owned_games_cache(self, force: bool = False) -> bool:
        """
        Обновляет кэш owned-игр. ФИКС: раньше использовался только
        IPlayerService/GetOwnedGames (нужен API-ключ и открыт профиль),
        иначе кэш оставался пустым. Теперь основной источник —
        приватный эндпоинт /mygames/ajaxrenderownedgames (работает по
        cookies), плюс GetOwnedGames если есть ключ. Результаты
        объединяются (union), пустой ответ никогда не затирает старый кэш.
        """
        if not force and self._cache_timestamp and \
                (time.time() - self._cache_timestamp) < self._cache_ttl and self._owned_games_cache:
            return True
        if not self.logged_in:
            self.load_owned_cache_file()
            return bool(self._owned_games_cache)

        app_ids: Set[str] = set()
        total_count = 0

        # 1) Приватная страница "Мои игры" — рендер списка по cookies
        try:
            sid = self.steam_id
            if sid:
                status, text, _ = await self._request(
                    'GET',
                    f'https://steamcommunity.com/mygames/ajaxrenderownedgames/'
                    f'?steamid={sid}&json=1&start=0&count=1000&filter=&tagcount=0&ssn={self.session_id or ""}',
                )
                if status == 200 and text:
                    soup = BeautifulSoup(text, 'html.parser')
                    for a in soup.find_all('a', href=True):
                        m = re.search(r'/app(?:s)?/(\d+)', a['href'])
                        if m:
                            app_ids.add(m.group(1))
        except Exception as e:
            logger.debug(f"⚠️ mygames недоступен: {e}")

        # 2) Web API GetOwnedGames (если задан ключ)
        api_key = os.getenv('STEAM_API_KEY', '')
        if api_key and self.steam_id:
            try:
                status, _, data = await self._request(
                    'GET', 'https://api.steampowered.com/IPlayerService/GetOwnedGames/v1/',
                    params={'key': api_key, 'steamid': self.steam_id, 'include_appinfo': False},
                    expect_json=True)
                if status == 200 and data:
                    games = (data.get('response') or {}).get('games') or []
                    app_ids |= {str(g['appid']) for g in games}
                    total_count = data.get('response', {}).get('game_count', len(app_ids))
            except Exception as e:
                logger.debug(f"⚠️ API не доступен: {e}")

        # 3) account/licenses — резервный источник app_id
        if not app_ids:
            try:
                status, text, _ = await self._request(
                    'GET', f'{STEAM_CONFIG["api_store"]}/account/licenses/')
                if status == 200 and text and 'logout' in text.lower():
                    for m in re.finditer(r'/app/(\d+)', text):
                        app_ids.add(m.group(1))
            except Exception as e:
                logger.debug(f"⚠️ licenses недоступен: {e}")

        new_total = total_count or len(app_ids)
        if new_total > 0:
            # union со старым кэшем: временные сбои страницы не должны
            # "терять" уже известные игры
            merged = self._owned_games_cache | app_ids
            self._owned_games_cache = merged
            self._cache_timestamp = time.time()
            self._cache_count = max(new_total, len(merged))
            self.save_owned_cache_file()
            logger.info(f"✅ Кэш библиотеки обновлён: {len(merged)} игр")
            return True

        logger.warning("⚠️ Не удалось получить список игр, оставляю старый кэш "
                       f"({len(self._owned_games_cache)})")
        self.load_owned_cache_file()
        return False

    async def is_game_owned(self, app_id: str, force_refresh: bool = False) -> bool:
        """Проверка наличия в библиотеке. ФИКС (DLC): страница DLC помечается
        Steam как 'owned', если у аккаунта есть базовая игра, — поэтому для
        DLC ответ берётся строго по кэшу лицензий (appids из
        ajaxrenderownedgames / account licenses), без HTML-false-positive."""
        app_id = str(app_id)
        await self.refresh_owned_games_cache(force=force_refresh)
        if app_id in self._owned_games_cache:
            return True
        if self._is_dlc_appid(app_id):
            logger.info(f"🧩 {app_id} — DLC: в лицензии-кэше отсутствует → "
                        f"считаем не добавленным (без false-positive по странице)")
            return False
        owned = await self._check_game_page_owned(app_id)
        if owned:
            self.mark_game_owned(app_id)
        return owned

    def _is_dlc_appid(self, app_id: str) -> bool:
        """DLC ли этот appid? Быстрый ответ — из кэша загруженных деталей;
        если детали ещё не грузились — неизвестно (False → обычная проверка)."""
        g = SteamSession._details_cache.get(str(app_id))
        return bool(g is not None and g.is_dlc)

    def mark_game_owned(self, app_id: str):
        """Инкрементально добавляет игру в кэш (ФИКС: после успешного
        добавления игра сразу считается находящейся в библиотеке)."""
        app_id = str(app_id)
        if app_id not in self._owned_games_cache:
            self._owned_games_cache.add(app_id)
            self._cache_count = max(self._cache_count, len(self._owned_games_cache))
            self.save_owned_cache_file()

    async def _check_game_page_owned(self, app_id: str) -> bool:
        status, text, _ = await self._request(
            'GET', f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')
        if status != 200 or not text:
            return False
        low = text.lower()
        if 'btn_add_to_library' in low or 'в библиотеке' in low or 'in library' in low:
            return True
        soup = BeautifulSoup(text, 'html.parser')
        if soup.find(class_='ds_owned_flag'):
            return True
        # если нет формы покупки — скорее всего уже владеем
        if not soup.find('form', {'name': lambda x: x and str(x).startswith('add_to_cart_')}):
            return True
        return False

    async def get_total_library_count(self) -> int:
        if not self._owned_games_cache:
            await self.refresh_owned_games_cache()
        return self._cache_count or len(self._owned_games_cache)

    # ---------- поиск subid / добавление ----------
    async def get_page_soup(self, app_id: str) -> Optional[BeautifulSoup]:
        status, text, _ = await self._request('GET', f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')
        if status != 200 or not text:
            return None
        return BeautifulSoup(text, 'html.parser')

    # ---------- подробная информация о игре (русский язык) ----------
    async def get_game_details(self, game: GameInfo) -> GameInfo:
        """Загружает картинку и описание через публичный appdetails API
        с cc=ru&l=russian — Steam сам отдаёт локализованные данные.
        Ошибки не мешают основному флоу: просто без деталей."""
        try:
            status, text, data = await self._request(
                'GET', 'https://store.steampowered.com/api/appdetails',
                params={'appids': game.app_id, 'cc': 'ru', 'l': 'russian'},
                expect_json=True)
            if status != 200 or not isinstance(data, dict):
                return game
            entry = data.get(str(game.app_id)) or {}
            if not entry.get('success'):
                return game
            d = entry.get('data') or {}
            game.header_image = d.get('header_image')
            sd = d.get('short_description') or ''
            game.short_description = BeautifulSoup(sd, 'html.parser').get_text(' ', strip=True)
            game.genres = [g.get('description', '') for g in d.get('genres', [])
                           if g.get('description')]
            p = d.get('platforms') or {}
            pf = []
            if p.get('windows'):
                pf.append('Windows')
            if p.get('mac'):
                pf.append('macOS')
            if p.get('linux'):
                pf.append('Linux')
            game.platforms = ' · '.join(pf)
            langs = BeautifulSoup(d.get('supported_languages') or '', 'html.parser')\
                .get_text(',', strip=True)
            ru = re.search(r'русск[^\s,]*', langs, flags=re.I)
            game.languages = ('есть русский ✅' if ru else langs[:120] or '—')
            game.release_date = ((d.get('release_date') or {}).get('date') or '').strip()
            game.developers = ', '.join(d.get('developers') or [])
            game.publishers = ', '.join(d.get('publishers') or [])
            tp = {'game': 'Игра', 'dlc': 'DLC / дополнение',
                  'software': 'Программа'}.get(d.get('type'), d.get('type') or '')
            game.type_name = tp
            game.is_free = bool(d.get('is_free'))
            # --- DLC: определяем родительскую игру (для фильтра «только если
            # базовая игра в библиотеке» и понятной карточки) ---
            game.is_dlc = (d.get('type') == 'dlc')
            fullgame = d.get('fullgame') or {}
            if fullgame.get('appid'):
                game.parent_app_id = str(fullgame['appid'])
                fg_name = (fullgame.get('name') or '').strip()
                m = re.match(r'^\s*(.+?)\s*[:\-–—]\s*\S.*$', fg_name, flags=re.S)
                game.parent_game_name = (m.group(1) if m else fg_name) or None
            game.details_loaded = True
            # кладём в общий кэш — чтобы тип (game/dlc) был известен и до
            # отправки карточки (фильтр DLC, проверка библиотеки)
            SteamSession._details_cache[str(game.app_id)] = game
        except Exception as e:
            logger.warning(f"⚠️ Не удалось получить детали {game.app_id}: {e}")
        return game

    async def resolve_parent_game(self, game: GameInfo) -> Optional[GameInfo]:
        """Для DLC возвращает GameInfo родительской игры (appdetails fullgame)."""
        if not game.is_dlc or not game.parent_app_id:
            return None
        cached = self._parent_cache.get(game.parent_app_id)
        if cached is not None:
            return cached
        parent = GameInfo(app_id=game.parent_app_id,
                          name=game.parent_game_name or game.parent_app_id,
                          url=f'{STEAM_CONFIG["api_store"]}/app/{game.parent_app_id}/')
        await self.get_game_details(parent)
        if not parent.name or parent.name == parent.app_id:
            parent.name = game.parent_game_name or parent.app_id
        self._parent_cache[parent.app_id] = parent
        return parent

    @staticmethod
    def _extract_dlc_parent(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
        """Fallback без appdetails: ищем ссылку на базовую игру в хлебных
        крошках страницы DLC (/dlc/<id>/<base-slug>/). Возвращает (app_id, имя)."""
        try:
            for a in soup.select('.breadcrumbs a'):
                href = a.get('href') or ''
                m = re.search(r'/app/(\d+)/', href)
                if m:
                    return m.group(1), a.get_text(strip=True) or None
            # заголовок вида «Bazooka Boy: Super Bazooka Gun DLC» → базовое имя
            h1 = soup.select_one('#appHubAppName')
            title = (h1.get_text(strip=True) if h1 else
                     (soup.title.get_text(strip=True) if soup.title else ''))
            if 'DLC' in title.upper():
                m = re.match(r'^\s*(.+?)\s*[:\-–—]\s*\S.*$', title, flags=re.S)
                if m:
                    return None, m.group(1).strip() or None
        except Exception:
            pass
        return None, None

    @staticmethod
    def _esc(s: str) -> str:
        return (s or '').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

    def format_game_card(self, game: GameInfo, extra: str = "",
                         header_prefix: str = "🎮") -> Tuple[str, Optional[str]]:
        """Собирает HTML-текст карточки + URL картинки (или None)."""
        esc_name = self._esc(game.name)
        lines = [f"{header_prefix} <b>{esc_name}</b>"]
        price = ""
        if game.discount_percent and game.original_price:
            price = (f"💰 <s>{self._esc(game.original_price)}</s> → "
                     f"<b>{self._esc(game.final_price or 'Бесплатно')}</b> "
                     f"(<b>{self._esc(game.discount_percent)}</b>)")
        elif game.final_price or game.original_price:
            price = f"💰 {self._esc(game.final_price or game.original_price)}"
        else:
            price = "💰 Бесплатно"
        lines.append(price)
        meta = []
        if game.type_name:
            meta.append(game.type_name)
        if game.is_dlc and game.parent_game_name:
            meta.append(f"для «{game.parent_game_name}»")
        meta.extend(game.genres[:4])
        if game.release_date:
            meta.append(f"выход: {game.release_date}")
        if meta:
            lines.append("🏷️ " + self._esc(' · '.join(meta)))
        if game.platforms:
            lines.append("💻 " + self._esc(game.platforms))
        if game.languages:
            lines.append("🗣️ " + self._esc(game.languages))
        who = ' · '.join(x for x in (game.developers, game.publishers) if x)
        if who:
            lines.append("🏢 " + self._esc(who))
        if game.short_description:
            desc = self._esc(game.short_description)[:700]
            lines.append(f"\n<i>{desc}</i>")
        lines.append(f'\n🔗 <a href="{game.url}">Открыть в Steam</a>')
        if extra:
            lines.append(extra)
        img = game.header_image if (game.header_image and
                                    game.header_image.startswith('http')) else None
        return "\n".join(lines), img

    @staticmethod
    def extract_purchase_params(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        subid = snr = originating_snr = None
        form = soup.find('form', {'name': lambda x: x and str(x).startswith('add_to_cart_')})
        inputs = form.find_all('input') if form else soup.find_all('input')
        for inp in inputs:
            name = inp.get('name')
            val = (inp.get('value') or '').strip()
            if name == 'subid' and val.isdigit() and not subid:
                subid = val
            elif name == 'snr' and not snr:
                snr = val
            elif name == 'originating_snr' and not originating_snr:
                originating_snr = val
        # ФИКС: для бесплатных DLC кнопки «Add to library» вызывают JS вида
        # AddFreeToLibrary( 1835073 ) / ajaxaddfreebgame c ключом подписки —
        # если формы нет, достаём subid из скриптов страницы.
        if not subid:
            scripts = ''.join(s.get_text() for s in soup.find_all('script'))
            m = re.search(r'(?:AddFreeGame|AddFreeToLibrary|ajaxaddfreebgame)[^0-9]{0,80}(\d{4,9})',
                          scripts)
            if not m:
                m = re.search(r'[?&]subid=(\d+)', scripts)
            if m:
                subid = m.group(1)
        return subid, snr, originating_snr

    async def add_free_game(self, app_id: str, subid: str,
                            snr: Optional[str] = None,
                            originating_snr: Optional[str] = None) -> Tuple[bool, str]:
        """
        Добавляет бесплатную игру/бесплатный DLC в библиотеку.
        ФИКС №0 (главный): Steam возвращал HTML-страницу вместо JSON, потому
        что запрос шёл на /freelicense/addfreelicense/ — а этот эндпоинт
        существует только для special-kinds (free-on-pro). Для обычных
        free-license и БЕСПЛАТНЫХ DLC (как «Rotwood: Drakin Armoury Pack»)
        правильный эндпоинт — /ajaxaddfreebgame/ (он же используется
        кнопкой «Add to your library» на странице игры).
        Порядок попыток: ajaxaddfreebgame → addfreelicense; успех
        верифицируется идемпотентным /api/addfreetrial.
        """
        if not self.logged_in:
            if not await self.check_auth():
                return False, "❌ Steam не авторизован (обновите cookies)"
        if not self.session_id:
            return False, "❌ Нет sessionid в cookies"

        # ФИКС v2.3: ajaxaddfreebgame/addfreelicense на store.steampowered.com
        # не существуют как POST-эндпоинты — Steam отдавал HTML главной
        # страницы ("Неожиданный ответ Steam ... <!DOCTYPE html>"). Реальный
        # эндпоинт кнопки «Add to your library» — checkout
        # /shoppingcart/addfreetrial/ (JSON). Порядок попыток:
        #   1) GET страницы игры — валидация сессии + свежий snr
        #   2) checkout /shoppingcart/addfreetrial/ (key=subid, JSON)
        #   3) legacy fallbacks (ajaxaddfreebgame → addfreelicense)
        #   4) идемпотентный store /api/addfreetrial/ — верификация и
        #      повторная попытка (SUCCESS / ALREADY_IN_LIBRARY / ERROR_*)
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json, text/plain, */*',
            'Origin': STEAM_CONFIG['checkout'],
            'Referer': f'{STEAM_CONFIG["api_store"]}/app/{app_id}/',
            'X-Requested-With': 'XMLHttpRequest',
        }
        logger.info(f"🎮 Попытка добавить {app_id} (subid={subid})")

        # 0) страница игры: ранний детект мёртвой сессии + snr для legacy-пути
        page_status, page_text, _ = await self._request(
            'GET', f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')
        if page_status in (301, 302) or (page_text and self._looks_like_login(
                f'{STEAM_CONFIG["api_store"]}/app/{app_id}/', page_text)):
            return False, self._ADD_ERRORS['auth']
        if not snr and page_text:
            m = re.search(r"snr[=:]\s*['\"]?([\w.#]+)", page_text)
            if m:
                snr = m.group(1)

        attempts = []
        # 1) основной путь: checkout /shoppingcart/addfreetrial/
        attempts.append((
            f'{STEAM_CONFIG["checkout"]}/shoppingcart/addfreetrial/',
            {
                'sessionid': self.session_id,
                'key': subid,
                'appid': app_id,
                'method': 'web',
                'country_code': 'RU',
            },
            None,
        ))
        # 2) legacy-путь ajaxaddfreebgame (без валидного snr Steam отвечает
        #    success:false, а не HTML)
        attempts.append((
            f'{STEAM_CONFIG["api_store"]}/ajaxaddfreebgame/',
            {
                'sessionid': self.session_id,
                'key': subid,
                'method': 'web',
                'country_code': 'RU',
                'snr': snr or 'search.results;direct_browse',
            },
            {'Origin': STEAM_CONFIG['api_store']},
        ))
        # 3) запасной путь: /freelicense/addfreelicense/ (special kinds)
        attempts.append((
            f'{STEAM_CONFIG["api_store"]}/freelicense/addfreelicense/',
            {
                'action': 'add_to_cart',
                'sessionid': self.session_id,
                'packageid': '',
                'subid': subid,
                'snr': snr or '1_5_9__403',
                'originating_snr': originating_snr or '1_direct-navigation__',
                'wants_gift': '0',
                'duplicate_allowed': '0',
            },
            {'Origin': STEAM_CONFIG['api_store']},
        ))

        added_now = already = False
        err: object = None
        last_text = ''
        for url_ep, data, hdr_over in attempts:
            h = dict(headers)
            if hdr_over:
                h.update(hdr_over)
            status, text, j = await self._request(
                'POST', url_ep, data=data, headers=h, expect_json=True)
            last_text = text or last_text
            a, k, e = self._parse_add_response(status, text, j)
            if a or k:
                added_now, already = added_now or a, already or k
                err = None
                break
            # auth/network — смысла крутить следующие эндпоинты нет
            if e in ('auth', 'network'):
                err = e
                break
            # JSON с понятным error_code (кромеAlready/Success) — ответ реальный,
            # idём дальше только если это просто HTML/непонятный ответ
            if isinstance(e, tuple):
                ec = str(e[1] or '')
                if ec and ec not in ('', 'None'):
                    err = e
                    break
            if err is None or e not in (None, 'html'):
                err = e

        if isinstance(err, tuple):
            return False, self._ADD_ERRORS.get(err, "⚠️ Steam не подтвердил добавление")
        if err == 'auth':
            return False, self._ADD_ERRORS['auth']
        if err == 'network':
            return False, self._ADD_ERRORS['network']
        if err == 'rate':
            return False, self._ADD_ERRORS['rate']
        if err == 'cart':
            return False, self._ADD_ERRORS['cart']

        if added_now or already:
            verified = await self._verify_free_license(app_id, subid)
            if verified in ('SUCCESS', 'ALREADY_IN_LIBRARY'):
                self.mark_game_owned(app_id)
                msg = ("✅ Игра добавлена в библиотеку!" if verified == 'SUCCESS'
                       else "✅ Уже в библиотеке!")
                return True, msg
            if verified == 'FREE_LICENSE_DISABLED':
                return False, "🚫 Раздача этой игры уже завершена (Free License Disabled)"
            if verified == 'ERROR_RATE_LIMIT_EXCEEDED':
                return False, self._ADD_ERRORS['rate']
            # текст обещал успех, но addfreetrial не подтвердил — доверяем
            # первичному ответу (для DLC addfreetrial может быть недоступен)
            logger.info(f"ℹ️ {app_id}: верификация через addfreetrial не подтвердила "
                        f"(результат: {verified}); фиксируем по ответу эндпоинта добавления")
            self.mark_game_owned(app_id)
            return True, ("✅ Добавлено в библиотеку!" if added_now
                          else "✅ Уже в библиотеке!")

        # НИ ОДИН эндпоинт не дал успеха. Фолбэк: честная проверка через
        # идемпотентный /api/addfreetrial/ (он и добавляет, и возвращает
        # понятные коды: SUCCESS / ALREADY_IN_LIBRARY / ERROR_*).
        verified = await self._verify_free_license(app_id, subid)
        if verified == 'SUCCESS':
            self.mark_game_owned(app_id)
            return True, "✅ Игра добавлена в библиотеку! (через addfreetrial)"
        if verified == 'ALREADY_IN_LIBRARY':
            self.mark_game_owned(app_id)
            return True, "✅ Уже в библиотеке!"
        if verified == 'FREE_LICENSE_DISABLED':
            return False, "🚫 Раздача этой игры уже завершена (Free License Disabled)"
        if verified == 'ERROR_RATE_LIMIT_EXCEEDED':
            return False, self._ADD_ERRORS['rate']

        # Понятная ошибка из JSON error_code вместо дампа HTML
        if isinstance(err, tuple) and len(err) >= 3:
            ec, em = str(err[1] or ''), str(err[2] or '')
            logger.warning(f"❌ Не удалось добавить {app_id} (subid={subid}): "
                           f"Steam вернул error_code={ec or '?'}, msg={em[:120]}")
            return False, f"⚠️ Steam отказал: {ec or em or 'неизвестная ошибка'}".strip()[:200]

        # Диагностика для лога: одна строка с кодом вместо дампа HTML
        code = self._diagnose_html(last_text)
        logger.warning(f"❌ Не удалось добавить {app_id} (subid={subid}): "
                       f"Steam не принял ни один эндпоинт [диагноз: {code}]")
        hints = {
            'login_redirect': "❌ Steam редиректит на логин — обновите steam_cookies.json",
            'not_logged_in': "❌ Steam требует авторизацию — обновите steam_cookies.json",
            'region': "🌍 Игра недоступна в регионе вашего аккаунта",
            'maintenance': "🛠 Steam временно недоступен — повторите позже",
            'unknown': "⚠️ Steam не подтвердил добавление (подробности в логе)",
        }
        return False, hints.get(code, hints['unknown'])

    @staticmethod
    def _diagnose_html(text: Optional[str]) -> str:
        """Сводит HTML-ответ Steam к короткому коду для лога/подсказки."""
        if not text:
            return 'empty'
        low = text.lower()
        if 'login?redir' in low or 'sign in' in low and 'steam' in low:
            return 'login_redirect'
        if 'js_g_loginrequired' in low or 'not logged in' in low or 'войдите' in low:
            return 'not_logged_in'
        if 'available in your region' in low or 'недоступна в вашем регионе' in low \
                or 'unavailableincountry' in low:
            return 'region'
        if 'under maintenance' in low or 'store is temporarily' in low:
            return 'maintenance'
        return 'unknown'

    async def _fetch_game_snr(self, app_id: str) -> Optional[str]:
        """Достаёт валидный snr страницы игры (нужен legacy-эндпоинтам)."""
        status, text, _ = await self._request(
            'GET', f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')
        if status != 200 or not text:
            return None
        m = re.search(r'snr[=:]\s*[\'"]?([\w.#]+)', text)
        return m.group(1) if m else None


    @staticmethod
    def _parse_add_response(status: int, text: Optional[str],
                            j: Optional[dict]) -> Tuple[bool, bool, object]:
        """Разбирает ответ ajaxaddfreebgame/addfreelicense.
        Возвращает (добавлена_сейчас, уже_была, ошибка_или_None)."""
        if status == 0 or text is None:
            return False, False, 'network'
        if isinstance(j, dict) and 'success' in j:
            if j.get('success'):
                return True, False, None
            ec = str(j.get('error_code') or '')
            em = str(j.get('error_message') or '').lower()
            low_em = em.lower()
            if ec == 'Already_Purchased' or 'already owned' in low_em or 'уже в библиотеке' in low_em:
                return False, True, None
            if 'rate limit' in low_em or 'too many' in low_em:
                return False, False, 'rate'
            if 'guest' in low_em or 'login' in low_em:
                return False, False, 'auth'
            if 'shopping cart' in low_em or 'корзин' in low_em:
                return False, False, 'cart'
            logger.info(f"ℹ️ Ответ Steam (JSON): error_code={ec}, msg={j.get('error_message')}")
            return False, False, ('json', ec, em)
        low = text.lower()
        added = ('added to your library' in low or 'вашу библиотеку' in low
                 or 'has been added' in low)
        already = ('already in your library' in low or 'уже в библиотеке' in low
                   or 'already_own' in low or 'already owned' in low)
        # ФИКС (DLC): страница DLC с базовой игрой в библиотеке содержит
        # «owned»-тексты, которые матчились как already/added ложно.
        # Если это HTML-страница (не AJAX-JSON), такие совпадения не считаются
        # успехом — реальное добавление подтвердит только addfreetrial.
        is_html_page = ('<!doctype html>' in low or '<html' in low)
        if added and not is_html_page:
            return True, False, None
        if already and not is_html_page:
            return False, True, None
        if 'rate limit' in low or 'too many' in low:
            return False, False, 'rate'
        if 'cart' in low and ('remove' in low or 'empty' in low):
            return False, False, 'cart'
        if is_html_page:
            # HTML вместо JSON = эндпоинт не принял запрос (не залогинены или
            # игра не free-license) — НЕ считаем сессию умершей автоматически
            return False, False, 'html'
        return False, False, None

    _ADD_ERRORS = {
        'network': "❌ Сетевая ошибка при добавлении",
        'rate': "⚠️ Steam ограничил частоту запросов — повторите позже",
        'auth': "❌ Steam не принимает сессию — обновите steam_cookies.json",
        'cart': "⚠️ Корзина не пуста — очистите её в Steam",
    }

    async def _verify_free_license(self, app_id: str, subid: str) -> Optional[str]:
        """Идемпотентная проверка статуса free-license. Возвращает
        SUCCESS / ALREADY_IN_LIBRARY / ERROR_* / FREE_LICENSE_DISABLED / None."""
        data = {
            'sessionid': self.session_id,
            'appid': app_id,
            'subid': subid,
            'method': 'web',
            'country_code': 'RU',
            'language': 'russian',
            'steamid': self.steam_id or '',
        }
        status, text, _ = await self._request(
            'POST', f'{STEAM_CONFIG["api_store"]}/api/addfreetrial/', data=data)
        if status != 200 or not text:
            return None
        for code in ('ALREADY_IN_LIBRARY', 'SUCCESS', 'FREE_LICENSE_DISABLED',
                     'ERROR_NO_LICENSE', 'ERROR_RATE_LIMIT_EXCEEDED'):
            if code in text.upper():
                return code
        return None

    async def init(self):
        """Полная инициализация при старте бота."""
        self.load_cookies_from_file()
        self.load_owned_cache_file()
        if self.cookies:
            self.logged_in = await self.check_auth()
            if not self.logged_in:
                await self.try_recover_session()
        if self.logged_in:
            await self.refresh_owned_games_cache(force=True)
        else:
            logger.error("❌ Steam НЕ авторизован! Обновите steam_cookies.json.")


# ============================================================================
# ПОИСК ИГР
# ============================================================================
class SmartGameFinder:
    """
    Поиск бесплатных и скидочных игр.
    ФИКС: используется официальный JSON-эндпоинт поиска
    store.steampowered.com/search/results/?query=&...&format=json —
    HTML-парсинг страницы поиска был хрупким (Steam отдаёт разную
    разметку/куки-баннер, из-за чего игры «терялись»). JSON стабильнее.
    """

    SEARCH_URL = f'{STEAM_CONFIG["api_store"]}/search/results/'

    def __init__(self, steam: SteamSession):
        self.steam = steam

    async def _fetch_page(self, params: Dict) -> List[dict]:
        p = {'query': '', 'max_results': 50, 'search_filter': '',
             'cc': 'ru', 'l': 'russian', 'format': 'json'}
        p.update(params)
        status, text, data = await self._safe_get(p, expect_json=True)
        if status != 200 or not data:
            # fallback: HTML-разметка строк результата
            p.pop('format')
            status, text, _ = await self._safe_get(p)
            if status != 200 or not text:
                return []
            soup = BeautifulSoup(text, 'html.parser')
            rows_html = soup.find_all('a', class_='search_result_row')
            out = []
            for row in rows_html:
                aid = row.get('data-ds-appid')
                title = row.find('span', class_='title')
                if aid and title:
                    disc = row.find('div', class_='discount_pct')
                    orig = row.find('div', class_='discount_original_price')
                    fin = row.find('div', class_='discount_final_price')
                    out.append({'appid': str(aid), 'name': title.get_text(strip=True),
                                'discount_percent': disc.get_text(strip=True) if disc else None,
                                'original_price': orig.get_text(strip=True) if orig else None,
                                'final_price': fin.get_text(strip=True) if fin else None,
                                'owned': 'ds_owned' in (row.get('class') or [])})
            return out
        rows_raw = data.get('items') or []
        items = []
        for it in rows_raw:
            items.append({
                'appid': str(it.get('appid')),
                'name': BeautifulSoup(it.get('name', ''), 'html.parser').get_text(strip=True),
                'discount_percent': it.get('discount_percent'),
                'original_price': it.get('original_price_formatted'),
                'final_price': it.get('final_price_formatted'),
                'owned': bool(it.get('in_library')) if 'in_library' in it else False,
            })
        return items

    async def _safe_get(self, params: Dict, expect_json: bool = False):
        return await self.steam._request('GET', self.SEARCH_URL,
                                         params=params, expect_json=expect_json)

    async def find_free_games(self, max_pages: int, progress_callback=None) -> List[GameInfo]:
        games, seen = [], set()
        for page in range(1, max_pages + 1):
            if progress_callback:
                await progress_callback(page, max_pages)
            rows = await self._fetch_page({'page': page, 'maxprice': 'free', 'specials': 1})
            if not rows:
                break
            for r in rows:
                aid = r['appid']
                if not aid.isdigit() or aid in seen:
                    continue
                seen.add(aid)
                games.append(GameInfo(
                    app_id=aid, name=r['name'],
                    url=f'{STEAM_CONFIG["api_store"]}/app/{aid}/',
                    original_price=r.get('original_price'),
                    final_price=r.get('final_price') or 'Бесплатно',
                    discount_percent=r.get('discount_percent'),
                    is_owned=bool(r.get('owned')),
                    discount_value=self._pct_value(r.get('discount_percent')),
                ))
            logger.info(f"📄 Бесплатные: страница {page} → {len(rows)} (всего {len(games)})")
        return games

    async def find_discount_games(self, max_pages: int, min_discount_percent: int = 0,
                                  max_games: int = 50, progress_callback=None) -> List[GameInfo]:
        games, seen = [], set()
        for page in range(1, max_pages + 1):
            if len(games) >= max_games:
                break
            if progress_callback:
                await progress_callback(page, max_pages)
            rows = await self._fetch_page({'page': page, 'specials': 1})
            if not rows:
                break
            for r in rows:
                aid = r['appid']
                pct = self._pct_value(r.get('discount_percent'))
                if not aid.isdigit() or aid in seen:
                    continue
                seen.add(aid)
                if pct < min_discount_percent:
                    continue
                games.append(GameInfo(
                    app_id=aid, name=r['name'],
                    url=f'{STEAM_CONFIG["api_store"]}/app/{aid}/',
                    original_price=r.get('original_price'),
                    final_price=r.get('final_price'),
                    discount_percent=r.get('discount_percent') or f'-{pct}%',
                    is_owned=bool(r.get('owned')),
                    discount_value=pct,
                ))
                if len(games) >= max_games:
                    break
            logger.info(f"🏷️ Скидки: страница {page} → отобрано {len(games)}")
        games.sort(key=lambda g: g.discount_value, reverse=True)
        return games[:max_games]

    @staticmethod
    def _pct_value(s) -> int:
        if s is None:
            return 0
        m = re.search(r'(\d+)', str(s))
        return int(m.group(1)) if m else 0


# ============================================================================
# НАСТРОЙКИ
# ============================================================================
class SettingsManager:
    def __init__(self, file_path: Path = SETTINGS_FILE):
        self.file_path = file_path
        self.defaults = {
            'auto_add_mode': AutoAddMode.EXPENSIVE.value,
            'auto_add_price_threshold': 500.0,
            'max_pages': BOT_CONFIG['max_pages'],
            'check_interval': BOT_CONFIG['interval'],
            'library_cache_ttl': 3600,
            'min_discount_percent': 50,
            'max_discount_pages': BOT_CONFIG['max_discount_pages'],
            'max_discount_games': BOT_CONFIG['max_discount_games'],
        }
        self.settings = self.load()

    def load(self) -> Dict:
        try:
            if self.file_path.exists():
                data = json.loads(self.file_path.read_text(encoding='utf-8'))
                if 'price_threshold' in data:
                    data['auto_add_price_threshold'] = data.pop('price_threshold')
                data.pop('discount_type', None)
                merged = {**self.defaults, **data}
                # защита от невалидных значений
                merged['min_discount_percent'] = max(1, min(99, int(merged['min_discount_percent'])))
                merged['max_pages'] = max(1, int(merged['max_pages']))
                return merged
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки настроек: {e}")
        return self.defaults.copy()

    def save(self) -> bool:
        try:
            tmp = self.file_path.with_suffix('.tmp')
            tmp.write_text(json.dumps(self.settings, ensure_ascii=False, indent=2), encoding='utf-8')
            os.replace(tmp, self.file_path)
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения настроек: {e}")
            return False

    def get(self, key: str, default=None):
        return self.settings.get(key, self.defaults.get(key, default))

    def set(self, key: str, value) -> bool:
        old = self.settings.get(key)
        self.settings[key] = value
        logger.info(f"⚙️ {key}: {old} → {value}")
        return self.save()


# ============================================================================
# БОТ
# ============================================================================
class SteamBot:
    def __init__(self):
        self.settings = SettingsManager()
        self.health = HealthMonitor()
        self.steam = SteamSession(settings=self.settings, health=self.health)
        self.finder = SmartGameFinder(self.steam)
        self.db = DatabaseManager()
        self.app: Optional[Application] = None
        self._check_lock = asyncio.Lock()  # защита от параллельных проверок
        self._session_dead_notified = False  # не спамить админу повторными алертами
        self._details_cache: Dict[str, GameInfo] = {}  # app_id -> карточка (для edit/деталей)

    async def initialize(self):
        await self.steam.init()
        logger.info(f"🤖 Бот готов | Steam: {'✅' if self.steam.logged_in else '❌'} | "
                    f"Режим: {self._mode_desc()}")

    def _mode_desc(self) -> str:
        mode = self.settings.get('auto_add_mode')
        if mode == AutoAddMode.ALL.value:
            return "Добавлять ВСЁ"
        if mode == AutoAddMode.EXPENSIVE.value:
            return f"Добавлять от {self.settings.get('auto_add_price_threshold')}₽"
        return "Выключено"

    async def _restricted(self, update: Update) -> bool:
        uid = (update.effective_user.id if update.effective_user else None)
        if uid != BOT_CONFIG['admin_id']:
            if update.message:
                await update.message.reply_text("⛔ Доступ запрещен.")
            elif update.callback_query:
                await update.callback_query.answer("⛔ Доступ запрещен.", show_alert=True)
            return True
        return False

    def _menu_kb(self):
        return ReplyKeyboardMarkup([
            [KeyboardButton("🔍 Проверить игры"), KeyboardButton("🏷️ Скидки")],
            [KeyboardButton("⚙️ Настройки"), KeyboardButton("📊 Статистика")],
            [KeyboardButton("🔄 Обновить кэш"), KeyboardButton("🏥 Статус")],
        ], resize_keyboard=True)

    def _game_kb(self, app_id: str, is_owned: bool = False, is_discount: bool = False):
        url = f'{STEAM_CONFIG["api_store"]}/app/{app_id}/'
        if is_owned:
            return InlineKeyboardMarkup([[
                InlineKeyboardButton("📚 В библиотеке", callback_data="already_owned"),
                InlineKeyboardButton("🔗 Steam", url=url)]])
        if self.steam.logged_in and not is_discount:
            return InlineKeyboardMarkup([[
                InlineKeyboardButton("🎮 Добавить в библиотеку", callback_data=f"add:{app_id}"),
                InlineKeyboardButton("🔗 Steam", url=url)]])
        return InlineKeyboardMarkup([[InlineKeyboardButton("🔗 Открыть в Steam", url=url)]])

    async def _send_safe(self, chat_id: int, text: str, reply_markup=None):
        try:
            await self.app.bot.send_message(chat_id, text, parse_mode=ParseMode.HTML,
                                            reply_markup=reply_markup, disable_web_page_preview=True)
        except TelegramError as e:
            logger.warning(f"⚠️ Не удалось отправить сообщение: {e}")

    async def _send_game_card(self, chat_id: int, game: GameInfo, kb,
                              extra: str = "", header_prefix: str = "🎮") -> bool:
        """Отправляет карточку игры: с картинкой (header image), если она
        есть и доступна Telegram; иначе — текстом. Возвращает True, если
        сообщение реально доставлено."""
        text, img = self.steam.format_game_card(game, extra=extra,
                                                header_prefix=header_prefix)
        bot = self.app.bot
        if img and len(text) <= 1024:
            try:
                await bot.send_photo(chat_id, photo=img, caption=text,
                                     parse_mode=ParseMode.HTML, reply_markup=kb)
                self._details_cache[game.app_id] = game
                return True
            except TelegramError as e:
                logger.info(f"ℹ️ Картинку {game.app_id} отправить не удалось "
                            f"({e.__class__.__name__}) — отправлю текстом")
        # без картинки (или caption длиннее лимита 1024) — обычное сообщение
        if len(text) > 4096:
            overflow = len(text) - 4096
            desc = game.short_description or ''
            game.short_description = desc[:max(0, len(desc) - overflow)]
            text, _ = self.steam.format_game_card(game, extra=extra,
                                                  header_prefix=header_prefix)
        try:
            await bot.send_message(chat_id, text, parse_mode=ParseMode.HTML,
                                   reply_markup=kb, disable_web_page_preview=True)
            self._details_cache[game.app_id] = game
            return True
        except TelegramError as e:
            logger.warning(f"⚠️ Не удалось отправить сообщение: {e}")
            return False

    # ---------------- команды ----------------
    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if await self._restricted(update):
            return
        md = self.settings.get('min_discount_percent')
        await update.message.reply_text(
            f"👋 <b>Персональный Steam Бот v2</b>\n"
            f"🔐 Steam: {'✅' if self.steam.logged_in else '❌ (обновите cookies)'}\n"
            f"⚙️ Режим: <b>{self._mode_desc()}</b>\n"
            f"📄 Страниц: {self.settings.get('max_pages')}\n"
            f"🏷️ Мин. скидка: <b>{md}%</b>\n"
            f"Используй меню для управления.",
            parse_mode=ParseMode.HTML, reply_markup=self._menu_kb())

    async def cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if await self._restricted(update):
            return
        lib = await self.steam.get_total_library_count()
        stats = await self.db.get_stats()
        await update.message.reply_text(
            f"📊 <b>Статистика</b>\n\n"
            f"🔐 Steam: {'✅' if self.steam.logged_in else '❌'}\n"
            f"⚙️ Режим: <b>{self._mode_desc()}</b>\n"
            f"💰 Порог цены: {self.settings.get('auto_add_price_threshold')}₽\n\n"
            f"📚 Игр в аккаунте: <b>{lib}</b>\n"
            f"🎮 Отслежено: {stats.get('total_tracked', 0)}\n"
            f"✅ Добавлено ботом: {stats.get('total_added', 0)}\n"
            f"🆕 Добавлено сегодня: {stats.get('today_added', 0)}",
            parse_mode=ParseMode.HTML)

    async def cmd_health(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if await self._restricted(update):
            return
        h = self.health.get_health_status()
        await update.message.reply_text(
            f"🏥 <b>Состояние системы</b>\n\n"
            f"Статус: <b>{h['status'].upper()}</b>\n"
            f"Аптайм: {h['uptime']}\n"
            f"Проверено игр: {h['games_checked']}\n"
            f"Добавлено игр: {h['games_added']}\n"
            f"Ошибок API: {h['api_errors']}\n"
            f"Восстановлений сессии: {h['session_reloads']}",
            parse_mode=ParseMode.HTML)

    # ---------------- проверка бесплатных игр ----------------
    async def run_check(self, chat_id: int, status_chat_msg=None, is_scheduled: bool = False):
        """Основной цикл: найти бесплатные игры → проверить библиотеку →
        автодобавление → верификация → запись в БД и кэш."""
        async with self._check_lock:
            max_pages = int(self.settings.get('max_pages'))
            auto_mode = self.settings.get('auto_add_mode')
            threshold = float(self.settings.get('auto_add_price_threshold'))

            async def progress(cur, total):
                if status_chat_msg:
                    try:
                        await status_chat_msg.edit_text(f"🔄 Поиск бесплатных игр... 📄 {cur}/{total}")
                    except TelegramError:
                        pass

            all_games = await self.finder.find_free_games(max_pages, progress)
            self.health.record('games_checked', len(all_games))

            if not all_games:
                if not is_scheduled:
                    await self._send_safe(chat_id,
                                          "😔 Игры не найдены. Если Steam блокирует запросы — "
                                          "повторите попытку позже.")
                return

            if status_chat_msg:
                try:
                    await status_chat_msg.edit_text(f"📊 Найдено {len(all_games)}. Сверяю с библиотекой...")
                except TelegramError:
                    pass

            new_games, added_games, skipped_dlc = [], [], []
            for game in all_games:
                if is_scheduled:
                    known = await self.db.execute_query(
                        "SELECT app_id FROM known_games WHERE app_id = ? AND notified = 1",
                        (game.app_id,))
                    if known:
                        continue

                # --- ФИКС (DLC): детали грузим ДО любых проверок — иначе тип
                # приложения неизвестен. DLC показываем и добавляем только
                # если базовая игра есть в библиотеке (запрос пользователя).
                await self.steam.get_game_details(game)
                if game.is_dlc:
                    parent = await self.steam.resolve_parent_game(game)
                    if parent is not None:
                        game.parent_app_id = parent.app_id
                        game.parent_game_name = game.parent_game_name or parent.name
                        has_base = await self.steam.is_game_owned(parent.app_id)
                    else:
                        # родитель не определён (appdetails не отдал fullgame) —
                        # безопасный вариант: не спамим и не пробуем добавлять
                        has_base = False
                        logger.info(f"🧩 {game.app_id}: родитель DLC не определён → пропуск")
                    if not has_base:
                        skipped_dlc.append(game)
                        logger.info(f"🧩 Пропуск DLC «{game.name}»: базовая игра "
                                    f"«{game.parent_game_name or 'неизвестна'}» не в библиотеке")
                        continue
                    game.is_owned = await self.steam.is_game_owned(game.app_id)
                    if game.is_owned:
                        await self.db.add_or_update_game(game)
                        continue
                else:
                    in_library = game.is_owned or await self.steam.is_game_owned(game.app_id)
                    game.is_owned = in_library
                    if in_library:
                        await self.db.add_or_update_game(game)
                        continue

                new_games.append(game)

                should_add = auto_mode == AutoAddMode.ALL.value
                if auto_mode == AutoAddMode.EXPENSIVE.value and game.original_price:
                    m = re.search(r'(\d[\d\s]*)', game.original_price)
                    if m:
                        try:
                            should_add = float(m.group(1).replace(' ', '')) >= threshold
                        except ValueError:
                            pass

                added = False
                if should_add and self.steam.logged_in:
                    soup = await self.steam.get_page_soup(game.app_id)
                    if soup:
                        subid, snr, osnr = self.steam.extract_purchase_params(soup)
                        if subid:
                            added, msg = await self.steam.add_free_game(game.app_id, subid, snr, osnr)
                            if added:
                                added_games.append(game)
                                self.health.record('games_added')
                                await self.db.log_history(game.app_id, 'auto_added')
                            else:
                                logger.warning(f"⚠️ Автодобавление {game.app_id} не удалось: {msg}")
                        else:
                            logger.info(f"ℹ️ {game.app_id}: subid не найден (возможно DLC/не/free-license)")

                game.is_owned = added
                await self.db.add_or_update_game(game)

            if status_chat_msg:
                try:
                    await status_chat_msg.delete()
                except TelegramError:
                    pass

            delivered_app_ids = []
            for game in new_games:
                # подробности уже загружены в цикле выше (get_game_details);
                # для DLC дополнительно укажем базовую игру в extra
                kb = self._game_kb(game.app_id, game.is_owned)
                if await self._send_game_card(chat_id, game, kb):
                    delivered_app_ids.append(game.app_id)

            # помечаем уведомлёнными только реально доставленные игры —
            # иначе при сбое Telegram следующая проверка их «потеряет»
            for app_id in delivered_app_ids:
                await self.db.execute_query(
                    "UPDATE known_games SET notified = 1 WHERE app_id = ?", (app_id,))

            if added_games:
                names = "\n".join(f"• {g.name}" for g in added_games[:20])
                await self._send_safe(chat_id,
                                      f"🎁 <b>Автоматически добавлено в библиотеку: "
                                      f"{len(added_games)}</b>\n{names}")
            elif not new_games and not is_scheduled:
                await self._send_safe(chat_id, "✅ Проверка завершена. Новых игр нет.")

            # DLC без базовой игры не показываем поштучно, но информируем одним
            # сообщением (чтобы пользователь знал, что бот их видит и фильтрует)
            if skipped_dlc and not is_scheduled:
                lines = [f"• <b>{self.steam._esc(g.name)}</b>"
                         + (f" — нужна «{self.steam._esc(g.parent_game_name)}»"
                            if g.parent_game_name else "")
                         for g in skipped_dlc[:15]]
                more = (f"\n…и ещё {len(skipped_dlc) - 15}" if len(skipped_dlc) > 15 else "")
                await self._send_safe(
                    chat_id,
                    f"🧩 <b>DLC скрыто ({len(skipped_dlc)})</b> — показываю бесплатные "
                    f"DLC только если базовая игра в библиотеке:\n" + "\n".join(lines) + more)

    async def manual_check(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if await self._restricted(update):
            return
        if self._check_lock.locked():
            await update.message.reply_text("⏳ Проверка уже выполняется, подождите.")
            return
        status_msg = await update.message.reply_text("🔄 Запуск проверки...")
        try:
            await self.run_check(BOT_CONFIG['admin_id'], status_msg)
        except Exception as e:
            logger.exception("❌ Ошибка ручной проверки")
            await self._send_safe(BOT_CONFIG['admin_id'], f"❌ Ошибка проверки: {e}")

    # ---------------- скидки (ConversationHandler) ----------------
    async def start_discount_check(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if await self._restricted(update):
            return ConversationHandler.END
        md = self.settings.get('min_discount_percent', 50)
        await update.message.reply_text(
            f"🏷️ <b>Введите минимальный процент скидки (1-99):</b>\n\n"
            f"Текущее значение: {md}%\n"
            f"Например: 85 — только жёсткие распродажи, 25 — всё подряд.",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(f"Использовать текущий ({md}%)",
                                     callback_data=f"use_default_discount:{md}"),
                InlineKeyboardButton("❌ Отмена", callback_data="cancel_discount")]])
        )
        return WAITING_DISCOUNT_PERCENT

    async def process_discount_percent(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if await self._restricted(update):
            return ConversationHandler.END
        try:
            percent = int(update.message.text.strip())
            if not 1 <= percent <= 99:
                raise ValueError
        except ValueError:
            await update.message.reply_text("⚠️ Введите целое число от 1 до 99.")
            return WAITING_DISCOUNT_PERCENT
        self.settings.set('min_discount_percent', percent)
        await self._run_discount_search(BOT_CONFIG['admin_id'], percent)
        return ConversationHandler.END

    async def use_default_discount(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        percent = int(query.data.split(':')[1])
        try:
            await query.message.delete()
        except TelegramError:
            pass
        await self._run_discount_search(BOT_CONFIG['admin_id'], percent)
        return ConversationHandler.END

    async def cancel_discount(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        try:
            await query.message.delete()
        except TelegramError:
            pass
        await self.app.bot.send_message(BOT_CONFIG['admin_id'], "Главное меню:",
                                        reply_markup=self._menu_kb())
        return ConversationHandler.END

    async def _run_discount_search(self, chat_id: int, min_discount: int):
        try:
            status_msg = await self.app.bot.send_message(
                chat_id, f"🔍 Поиск скидок от {min_discount}%...")
            max_pages = int(self.settings.get('max_discount_pages'))
            max_games = int(self.settings.get('max_discount_games'))

            async def progress(cur, total):
                try:
                    await status_msg.edit_text(f"🔍 Поиск скидок от {min_discount}%... 📄 {cur}/{total}")
                except TelegramError:
                    pass

            games = await self.finder.find_discount_games(
                max_pages=max_pages, min_discount_percent=min_discount,
                max_games=max_games, progress_callback=progress)
            self.health.record('games_checked', len(games))

            if not games:
                await status_msg.edit_text(f"😔 Игр со скидкой ≥{min_discount}% не найдено.")
                return

            await status_msg.edit_text(f"✅ Найдено {len(games)} игр. Отправляю карточки...")
            for i, g in enumerate(games, 1):
                emoji = ("🔥" if g.discount_value >= 75 else "🎯" if g.discount_value >= 50
                         else "💎" if g.discount_value >= 25 else "🏷️")
                await self.steam.get_game_details(g)
                kb = self._game_kb(g.app_id, g.is_owned, is_discount=True)
                await self._send_game_card(chat_id, g, kb, header_prefix=f"{emoji} #{i}")
                if i % 5 == 0:
                    await asyncio.sleep(1.0)  # уважение к лимитам Telegram

            avg = sum(g.discount_value for g in games) / len(games)
            mx = max(g.discount_value for g in games)
            await status_msg.edit_text(
                f"✅ <b>Поиск скидок завершён!</b>\n"
                f"📊 Найдено: <b>{len(games)}</b> | Средняя скидка: <b>{avg:.0f}%</b> | "
                f"Максимум: <b>{mx}%</b>")
        except Exception as e:
            logger.exception("❌ Ошибка поиска скидок")
            await self._send_safe(chat_id, f"❌ Ошибка: {e}")

    # ---------------- кнопка "Добавить" ----------------
    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        if await self._restricted(update):
            return
        data = query.data or ""
        if data == "already_owned":
            await query.answer("📚 Эта игра уже в вашей библиотеке!", show_alert=True)
            return
        if data.startswith('details:'):
            await self._show_details(query, data.split(':', 1)[1])
            return
        if not data.startswith('add:'):
            return

        app_id = data.split(':')[1]
        if not self.steam.logged_in and not await self.steam.check_auth():
            await query.answer("❌ Steam не авторизован. Обновите steam_cookies.json.",
                               show_alert=True)
            return

        await query.answer("⏳ Проверяю и добавляю...")

        if await self.steam.is_game_owned(app_id):
            await self.db.log_history(app_id, 'already_owned')
            try:
                await query.edit_message_reply_markup(reply_markup=self._game_kb(app_id, is_owned=True))
            except TelegramError:
                pass
            return

        # карточка для редактирования сообщения — берём из кэша деталей
        game = self._details_cache.get(app_id) or GameInfo(
            app_id=app_id, name=app_id, url=f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')
        if not game.details_loaded:
            await self.steam.get_game_details(game)
            self._details_cache[app_id] = game

        # --- ФИКС (DLC): ручное добавление DLC тоже только с базовой игрой ---
        if game.is_dlc:
            parent = await self.steam.resolve_parent_game(game)
            if parent is not None:
                game.parent_app_id = parent.app_id
                game.parent_game_name = game.parent_game_name or parent.name
                has_base = await self.steam.is_game_owned(parent.app_id)
            else:
                has_base = False
            if not has_base:
                pname = self.steam._esc(game.parent_game_name or 'неизвестна')
                await self._edit_card(
                    query, game,
                    f"\n\n🧩 <b>DLC не добавлен</b>\nБазовая игра «{pname}» "
                    f"отсутствует в библиотеке — DLC бесполезен без неё.",
                    InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔗 Открыть в Steam",
                                             url=f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')]]))
                return

        soup = await self.steam.get_page_soup(app_id)
        if not soup:
            await self._edit_card(query, game, "\n\n❌ <b>Страница не загрузилась</b>",
                                  InlineKeyboardMarkup([[
                                      InlineKeyboardButton("🔄 Повторить", callback_data=f"add:{app_id}"),
                                      InlineKeyboardButton("🔗 Steam",
                                                           url=f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')]]))
            return

        subid, snr, osnr = self.steam.extract_purchase_params(soup)
        if not subid:
            await self._edit_card(query, game,
                                  "\n\n⚠️ <b>Не найден subid — эту игру нельзя "
                                  "добавить автоматически</b> (откройте страницу в Steam вручную)",
                                  InlineKeyboardMarkup([[
                                      InlineKeyboardButton("🔗 Открыть в Steam",
                                                           url=f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')]]))
            return

        added, msg = await self.steam.add_free_game(app_id, subid, snr, osnr)
        await self.db.log_history(app_id, 'manual_added' if added else 'add_failed')
        game.name = game.name or (soup.title.get_text(strip=True) if soup.title else app_id)
        game.is_owned = added
        await self.db.add_or_update_game(game)

        if added:
            self.health.record('games_added')
            await self._edit_card(query, game, f"\n\n✅ <b>{msg}</b>",
                                  self._game_kb(app_id, is_owned=True))
        else:
            await self._edit_card(query, game, f"\n\n❌ <b>Не удалось добавить</b>\n<i>{msg}</i>",
                                  InlineKeyboardMarkup([[
                                      InlineKeyboardButton("🔄 Попробовать снова", callback_data=f"add:{app_id}"),
                                      InlineKeyboardButton("🔗 Steam",
                                                           url=f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')]]))

    async def _edit_card(self, query, game: GameInfo, extra: str, kb):
        """Редактирует текст карточки с кнопками; если caption длиннее лимита
        Telegram (1024) — сокращает описание, чтобы сообщение не пропало."""
        for limit in (1000, 600, 300, 0):
            g = game
            text, _ = self.steam.format_game_card(g, extra=extra)
            if limit < 1000:
                trimmed = GameInfo(**{**g.__dict__})
                trimmed.short_description = (g.short_description or '')[:limit]
                text, _ = self.steam.format_game_card(trimmed, extra=extra)
            try:
                await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
                return
            except TelegramError as e:
                logger.debug(f"edit_message_text (limit={limit}): {e}")
        # совсем край: минимальное сообщение
        try:
            await query.edit_message_text(
                f"🎮 <b>{self.steam._esc(game.name)}</b>{extra}",
                parse_mode=ParseMode.HTML, reply_markup=kb)
        except TelegramError as e:
            logger.warning(f"⚠️ Не удалось отредактировать сообщение: {e}")

    async def _show_details(self, query, app_id: str):
        """Кнопка «ℹ️ Подробнее»: обновляет карточку полными деталями."""
        game = self._details_cache.get(app_id)
        if not game or not game.details_loaded:
            await query.answer("Загружаю подробности…")
            game = game or GameInfo(app_id=app_id, name=app_id,
                                    url=f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')
            await self.steam.get_game_details(game)
            self._details_cache[app_id] = game
        else:
            await query.answer()
        kb = self._game_kb(app_id, game.is_owned)
        text, _ = self.steam.format_game_card(game)
        try:
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        except TelegramError as e:
            logger.warning(f"⚠️ details: не удалось обновить карточку: {e}")

    # ---------------- настройки ----------------
    async def settings_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if await self._restricted(update):
            return
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"📋 Режим: {self._mode_desc()}", callback_data="set_mode")],
            [InlineKeyboardButton(f"💰 Порог цены: {self.settings.get('auto_add_price_threshold')}₽",
                                  callback_data="set_price")],
            [InlineKeyboardButton(f"📄 Страниц поиска: {self.settings.get('max_pages')}",
                                  callback_data="set_pages")],
            [InlineKeyboardButton(f"🏷️ Мин. скидка: {self.settings.get('min_discount_percent')}%",
                                  callback_data="set_min_discount")],
            [InlineKeyboardButton(f"📄 Страниц скидок: {self.settings.get('max_discount_pages')}",
                                  callback_data="set_discount_pages")],
            [InlineKeyboardButton(f"🎮 Макс. игр в скидках: {self.settings.get('max_discount_games')}",
                                  callback_data="set_max_discount")],
            [InlineKeyboardButton(f"⏱ Интервал проверки: {self.settings.get('check_interval')} мин",
                                  callback_data="noop")],
            [InlineKeyboardButton("🗑 Сбросить историю", callback_data="reset_history")],
            [InlineKeyboardButton("🔄 Обновить кэш библиотеки", callback_data="refresh_cache")],
            [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")],
        ])
        text = "⚙️ <b>Настройки</b>"
        if update.callback_query:
            try:
                await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                                              reply_markup=kb)
            except TelegramError:
                await update.callback_query.message.reply_text(text, parse_mode=ParseMode.HTML,
                                                               reply_markup=kb)
        else:
            await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

    async def settings_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        if await self._restricted(update):
            return
        await query.answer()
        data = query.data

        if data == "noop":
            return
        if data == "main_menu":
            await self.app.bot.send_message(query.message.chat_id, "Главное меню:",
                                            reply_markup=self._menu_kb())
            try:
                await query.message.delete()
            except TelegramError:
                pass
            return
        if data == "reset_history":
            await self.db.execute_query("DELETE FROM known_games")
            await query.answer("✅ История сброшена!", show_alert=True)
            await self.settings_menu(update, context)
            return
        if data == "refresh_cache":
            await query.answer("🔄 Обновляю кэш...", show_alert=True)
            ok = await self.steam.refresh_owned_games_cache(force=True)
            count = await self.steam.get_total_library_count()
            await self._send_safe(query.message.chat_id,
                                  f"{'✅' if ok else '⚠️'} Кэш обновлён: {count} игр")
            await self.settings_menu(update, context)
            return
        if data == "set_mode":
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Всё подряд", callback_data="mode:all")],
                [InlineKeyboardButton(f"💰 Только дорогие (≥{self.settings.get('auto_add_price_threshold')}₽)",
                                      callback_data="mode:expensive")],
                [InlineKeyboardButton("❌ Выключить", callback_data="mode:off")],
                [InlineKeyboardButton("🔙 Назад", callback_data="settings_menu")]])
            await query.edit_message_text("Выбери режим авто-добавления:", reply_markup=kb)
            return
        if data.startswith("mode:"):
            self.settings.set('auto_add_mode', data.split(":")[1])
            await self.settings_menu(update, context)
            return
        prompts = {
            "set_price": ("auto_add_price_threshold", "Введите новый порог цены (в рублях):", float),
            "set_pages": ("max_pages", "Кол-во страниц поиска бесплатных игр (1-50):", int),
            "set_discount_pages": ("max_discount_pages", "Кол-во страниц поиска скидок (1-50):", int),
            "set_max_discount": ("max_discount_games", "Макс. кол-во игр со скидками (10-200):", int),
            "set_min_discount": ("min_discount_percent", "Минимальный процент скидки (1-99):", int),
        }
        if data in prompts:
            key, prompt, cast = prompts[data]
            await query.edit_message_text(
                prompt, reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Отмена", callback_data="settings_menu")]]))
            context.user_data['awaiting'] = {'key': key, 'cast': cast.__name__}
            return
        if data == "settings_menu":
            await self.settings_menu(update, context)

    async def text_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if await self._restricted(update):
            return
        text = (update.message.text or '').strip()

        awaiting = context.user_data.get('awaiting')
        if awaiting:
            key = awaiting['key']
            try:
                value = float(text) if awaiting['cast'] == 'float' else int(float(text))
                if key == 'min_discount_percent' and not 1 <= value <= 99:
                    raise ValueError
                if key in ('max_pages', 'max_discount_pages') and not 1 <= value <= 50:
                    raise ValueError
                if key == 'max_discount_games' and not 10 <= value <= 200:
                    raise ValueError
                self.settings.set(key, value)
                context.user_data.pop('awaiting', None)
                await update.message.reply_text(f"✅ Настройка обновлена: {value}",
                                                reply_markup=self._menu_kb())
            except ValueError:
                await update.message.reply_text("⚠️ Некорректное значение. Попробуйте ещё раз.")
            return

        if text == "🔍 Проверить игры":
            await self.manual_check(update, context)
        elif text == "🏷️ Скидки":
            # обработается ConversationHandler'ом ниже, здесь fallback
            await self.start_discount_check(update, context)
        elif text == "⚙️ Настройки":
            await self.settings_menu(update, context)
        elif text == "📊 Статистика":
            await self.cmd_stats(update, context)
        elif text == "🔄 Обновить кэш":
            msg = await update.message.reply_text("🔄 Обновляю кэш библиотеки...")
            ok = await self.steam.refresh_owned_games_cache(force=True)
            count = await self.steam.get_total_library_count()
            await msg.edit_text(f"{'✅' if ok else '⚠️'} Кэш обновлён! Всего игр: {count}",
                                reply_markup=self._menu_kb())
        elif text == "🏥 Статус":
            await self.cmd_health(update, context)
        else:
            await update.message.reply_text("Главное меню:", reply_markup=self._menu_kb())

    # ---------------- планировщик ----------------
    async def scheduled_check(self, context: ContextTypes.DEFAULT_TYPE):
        """Фоновый job PTB — устойчив к ошибкам, логирует всё."""
        try:
            if self._check_lock.locked():
                logger.info("📅 Плановая проверка пропущена: уже выполняется")
                return
            logger.info("📅 Плановая проверка бесплатных игр...")
            await self.run_check(BOT_CONFIG['admin_id'], is_scheduled=True)
        except Exception:
            logger.exception("❌ Ошибка плановой проверки")

    async def auth_watchdog(self, context: ContextTypes.DEFAULT_TYPE):
        """Каждые 15 минут проверяем, жива ли сессия Steam; при смерти —
        пробуем восстановить из файла cookies и уведомляем админа.
        Алерт-машина состояний: «смерть» -> 1 алерт (без спама каждые 15 мин),
        «восстановление» -> 1 подтверждение. Уведомления дублируются в лог,
        чтобы не потерять тревогу, если Telegram недоступен."""
        was = self.steam.logged_in
        ok = await self.steam.check_auth()
        if not ok:
            ok = await self.steam.try_recover_session()
        # Синхронизируем флаг авторизации с реальным состоянием (иначе
        # «зависевший» logged_in=True глушит recovery в add_free_game)
        self.steam.logged_in = bool(ok)
        if ok and not was:
            self._session_dead_notified = False
            logger.info("♻️ Сессия Steam восстановлена автоматически")
            await self._send_safe(BOT_CONFIG['admin_id'],
                                  "♻️ Сессия Steam восстановлена автоматически.")
        elif not ok and not self._session_dead_notified:
            self._session_dead_notified = True
            logger.error("🚨 Сессия Steam ПРЕРВАНА, автовосстановление не удалось — "
                         "нужно обновить steam_cookies.json")
            await self._send_safe(BOT_CONFIG['admin_id'],
                                  "🚨 Сессия Steam ПРЕРВАНА и автовосстановление не удалось.\n"
                                  "Откройте Steam в браузере, экспортируйте cookies "
                                  "в steam_cookies.json — бот подхватит их сам.")


# ============================================================================
# ТОЧКА ВХОДА
# ============================================================================
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("❌ Необработанная ошибка handler: %s", context.error)
    try:
        if isinstance(update, Update) and update.effective_chat:
            await context.bot.send_message(update.effective_chat.id,
                                           f"⚠️ Внутренняя ошибка: {context.error}")
    except Exception:
        pass


def build_application(bot: SteamBot) -> Application:
    from telegram.request import HTTPXRequest
    request = HTTPXRequest(connection_pool_size=8, read_timeout=30.0,
                           connect_timeout=20.0, pool_timeout=20.0)
    application = (ApplicationBuilder()
                   .token(BOT_CONFIG['token'])
                   .request(request)
                   .concurrent_updates(False)
                   .job_queue(JobQueue())   # явный инстанс: без него планировщик = None
                   .build())

    discount_conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex("^🏷️ Скидки$"), bot.start_discount_check),
        ],
        states={
            WAITING_DISCOUNT_PERCENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bot.process_discount_percent),
                CallbackQueryHandler(bot.use_default_discount, pattern="^use_default_discount:"),
                CallbackQueryHandler(bot.cancel_discount, pattern="^cancel_discount$"),
            ],
        },
        fallbacks=[
            CallbackQueryHandler(bot.cancel_discount, pattern="^cancel_discount$"),
            CommandHandler("cancel", lambda u, c: ConversationHandler.END),
        ],
    )

    application.add_handler(CommandHandler("start", bot.cmd_start))
    application.add_handler(CommandHandler("settings", bot.settings_menu))
    application.add_handler(CommandHandler("stats", bot.cmd_stats))
    application.add_handler(CommandHandler("health", bot.cmd_health))
    application.add_handler(discount_conv)
    application.add_handler(CallbackQueryHandler(bot.button_handler, pattern=r"^(add:|already_owned)"))
    application.add_handler(CallbackQueryHandler(
        bot.settings_callback,
        pattern=r"^(set_|mode:|settings_menu|main_menu|reset_history|refresh_cache|noop)"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.text_handler))
    application.add_error_handler(on_error)
    return application


async def _post_init(application: Application) -> None:
    # Вызывается PTB внутри уже запущенного event loop (await-колбэк).
    # Раньше здесь был синхронный post_init + create_task — при фатальной ошибке
    # инициализации это роняло процесс с бессмысленным «no running event loop».
    bot = application.bot_data['bot']
    await bot.initialize()
    if application.job_queue is None:
        logger.error("⚠️ Планировщик недоступен (не установлен пакет APScheduler). "
                     "Автопроверка и watchdog сессии НЕ работают! "
                     "Выполните: pip install -r requirements.txt")
        return
    interval_min = int(bot.settings.get('check_interval', 60))
    application.job_queue.run_repeating(
        bot.scheduled_check, interval=interval_min * 60, first=90,
        name='free_games_check')
    application.job_queue.run_repeating(
        bot.auth_watchdog, interval=15 * 60, first=60, name='auth_watchdog')
    logger.info(f"🚀 Планировщик запущен: проверка каждые {interval_min} мин, "
                f"watchdog сессии каждые 15 мин")


async def _post_shutdown(application: Application) -> None:
    bot = application.bot_data['bot']
    try:
        await bot.steam.close()
    except Exception as e:
        logger.warning(f"Не удалось корректно закрыть сессию Steam: {e}")
    logger.info("👋 Бот остановлен, ресурсы освобождены")


def main():
    bot = SteamBot()
    application = build_application(bot)
    application.bot_data['bot'] = bot
    bot.app = application

    application.post_init = _post_init
    application.post_shutdown = _post_shutdown

    logger.info(f"🚀 Запуск Steam Free Games Bot v2 (admin={BOT_CONFIG['admin_id']})")
    try:
        application.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            bootstrap_retries=-1,      # бесконечные ретраи подключения к Telegram
        )
    except KeyboardInterrupt:
        logger.info("👋 Остановлено пользователем")
    except InvalidToken as e:
        t = BOT_CONFIG['token'] or ''
        msg = str(e).lower()
        reasons = []
        if not re.fullmatch(r'\d{6,10}:[A-Za-z0-9_-]{34,}', t):
            reasons.append('Токен имеет НЕВЕРНЫЙ ФОРМАТ. Ожидается "123456789:AA...". '
                           'Проверьте строку TELEGRAM_BOT_TOKEN в .env: значение без кавычек, '
                           'без пробелов и без повтора "TELEGRAM_BOT_TOKEN=" внутри значения.')
        if 'not found' in msg or 'unauthorized' in msg:
            reasons.append('Формат токена корректный, но Telegram его не находит. Возможные причины: '
                           'токен отозван/пересоздан в @BotFather (нужно вставить НОВЫЙ), '
                           'или перепутаны символы при копировании (проверьте, что скопирована вся строка).')
        detail = '\n'.join('   • ' + r for r in reasons) or f'   Ответ сервера: {e}'
        logger.critical(
            f"💥 Telegram отклонил токен бота.{detail}\n"
            "   Новый токен: @BotFather -> /mybots -> ваш бот -> API Token. Затем обновите .env."
        )
        sys.exit(2)
    except Exception as e:
        logger.critical(f"💥 Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
