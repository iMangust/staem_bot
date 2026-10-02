#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🎮 Steam Free Games Bot — Fixed Event Loop Edition
• Исправлена проблема с event loop
• Корректная работа с Telegram Application
• Стабильная асинхронная архитектура
• Добавлен поиск игр со скидками с ручным вводом процента
"""
import os
import sys
import json
import time
import asyncio
import logging
import sqlite3
import io
import random
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any, Set
from enum import Enum
from collections import deque
from dataclasses import dataclass, field

import aiohttp
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters, ConversationHandler
from dotenv import load_dotenv

# ============================================================================
# НАСТРОЙКИ ПО УМОЛЧАНИЮ
# ============================================================================
load_dotenv()
BASE_DIR = Path(__file__).parent.resolve()
COOKIES_FILE = BASE_DIR / 'steam_cookies.json'
SETTINGS_FILE = BASE_DIR / 'bot_settings.json'
OWNED_GAMES_CACHE_FILE = BASE_DIR / 'owned_games_cache.json'
LOG_FILE = BASE_DIR / 'steam_bot.log'
DATABASE_FILE = BASE_DIR / 'steam_bot.db'

STEAM_CONFIG = {
    'store': 'store.steampowered.com',
    'api_store': 'https://store.steampowered.com',
    'timeout': 25,
    'delay': 1,
}

BOT_CONFIG = {
    'token': os.getenv('TELEGRAM_BOT_TOKEN'),
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
        logging.StreamHandler(sys.stdout)
    ]
)
for n in ['telegram', 'httpx', 'urllib3', 'apscheduler', 'fake_useragent', 'aiohttp']:
    logging.getLogger(n).setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ============================================================================
# КОНСТАНТЫ ДЛЯ CONVERSATION HANDLER
# ============================================================================
WAITING_DISCOUNT_PERCENT = 1

# ============================================================================
# УТИЛИТЫ
# ============================================================================
class AutoAddMode(Enum):
    OFF = 'off'
    EXPENSIVE = 'expensive'
    ALL = 'all'

@dataclass
class GameInfo:
    """Информация об игре"""
    app_id: str
    name: str
    url: str = ""
    original_price: Optional[str] = None
    final_price: Optional[str] = None
    discount_percent: Optional[str] = None
    is_owned: bool = False
    discount_value: int = 0  # числовое значение скидки для сортировки

class CircuitBreaker:
    """Защита от частых ошибок API"""
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "CLOSED"
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
    
    async def execute(self, func, *args, **kwargs):
        if self.state == "OPEN":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "HALF_OPEN"
                logger.info("Circuit breaker: HALF_OPEN")
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = await func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                logger.warning(f"Circuit breaker: OPEN")
            raise

class RateLimiter:
    """Ограничитель частоты запросов"""
    def __init__(self, calls_per_second: float = 0.5):
        self.calls_per_second = calls_per_second
        self.last_call = 0
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        async with self._lock:
            now = time.time()
            wait = max(0, (1.0 / self.calls_per_second) - (now - self.last_call))
            if wait > 0:
                await asyncio.sleep(wait)
            self.last_call = time.time()

class DatabaseManager:
    """SQLite база данных"""
    def __init__(self, db_path: Path = DATABASE_FILE):
        self.db_path = db_path
        self._lock = asyncio.Lock()
        self._init_db_sync()
    
    def _init_db_sync(self):
        try:
            with sqlite3.connect(str(self.db_path)) as conn:
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
                conn.commit()
            logger.info("✅ База данных инициализирована")
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации БД: {e}")
    
    async def execute_query(self, query: str, params: tuple = ()) -> List[Dict]:
        async with self._lock:
            try:
                with sqlite3.connect(str(self.db_path)) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.execute(query, params)
                    conn.commit()
                    if query.strip().upper().startswith('SELECT'):
                        return [dict(row) for row in cursor.fetchall()]
                    return []
            except Exception as e:
                logger.error(f"❌ Ошибка БД: {e}")
                return []
    
    async def add_or_update_game(self, game: GameInfo):
        await self.execute_query("""
            INSERT OR REPLACE INTO known_games 
            (app_id, name, notified, added, in_library, original_price, final_price, discount_percent, timestamp)
            VALUES (?, ?, 1, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (game.app_id, game.name, game.is_owned, game.is_owned,
              game.original_price, game.final_price, game.discount_percent))
    
    async def get_stats(self) -> Dict:
        result = await self.execute_query("""
            SELECT 
                COUNT(*) as total_tracked,
                SUM(CASE WHEN added = 1 THEN 1 ELSE 0 END) as total_added,
                SUM(CASE WHEN in_library = 1 THEN 1 ELSE 0 END) as in_library,
                SUM(CASE WHEN date(timestamp) = date('now') AND added = 1 THEN 1 ELSE 0 END) as today_added
            FROM known_games
        """)
        return result[0] if result else {}

class HealthMonitor:
    """Мониторинг здоровья системы"""
    def __init__(self):
        self.start_time = time.time()
        self.metrics = {
            'games_checked': 0,
            'games_added': 0,
            'api_errors': 0,
        }
        self._lock = asyncio.Lock()
    
    async def record_metric(self, metric: str, value: int = 1):
        async with self._lock:
            if metric in self.metrics:
                self.metrics[metric] += value
    
    async def get_health_status(self) -> Dict:
        uptime = time.time() - self.start_time
        return {
            'status': 'healthy' if self.metrics['api_errors'] < 10 else 'degraded',
            'uptime': str(timedelta(seconds=int(uptime))),
            'games_checked': self.metrics['games_checked'],
            'games_added': self.metrics['games_added'],
            'api_errors': self.metrics['api_errors'],
        }

# ============================================================================
# STEAM СЕССИЯ
# ============================================================================
class SteamSession:
    """Асинхронная Steam сессия"""
    def __init__(self, cookies_file: Path = COOKIES_FILE):
        self.cookies_file = cookies_file
        self.cookies = {}
        self.logged_in = False
        self.steam_id: Optional[str] = None
        self.session_id: Optional[str] = None
        self._owned_games_cache: Set[str] = set()
        self._cache_timestamp: Optional[float] = None
        self._cache_count: int = 0
        self.circuit_breaker = CircuitBreaker()
        self.rate_limiter = RateLimiter()
        self.user_agents = UserAgent()
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def init_session(self):
        self._load_cookies()
        if self.session_id:
            self.logged_in = await self._check_auth()
        if self.logged_in:
            logger.info("✅ Steam авторизация успешна")
            await self._refresh_owned_games_cache()
        else:
            self._load_owned_games_cache()
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
    
    def _load_cookies(self):
        if not self.cookies_file.exists():
            logger.warning("❌ Файл cookies не найден")
            return
        
        try:
            with open(self.cookies_file, 'r') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                return
            
            self.cookies = {}
            for c in data:
                name = c.get('name')
                value = c.get('value')
                domain = c.get('domain', '').lstrip('.')
                
                if not name or not value:
                    continue
                
                self.cookies[name] = value
                
                if name == 'steamLoginSecure' and '%7C%7C' in value:
                    self.steam_id = value.split('%7C%7C')[0]
                elif name == 'sessionid' and 'store' in domain:
                    self.session_id = value
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки cookies: {e}")
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if not self._session or self._session.closed:
            connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
            timeout = aiohttp.ClientTimeout(total=STEAM_CONFIG['timeout'])
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers=self._get_headers()
            )
        return self._session
    
    def _get_headers(self) -> Dict:
        return {
            'User-Agent': self.user_agents.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8',
        }
    
    async def _check_auth(self) -> bool:
        try:
            session = await self._get_session()
            await self.rate_limiter.acquire()
            
            async with session.get(
                f'{STEAM_CONFIG["api_store"]}/account/',
                cookies=self.cookies,
                allow_redirects=True
            ) as resp:
                text = await resp.text()
                return resp.status == 200 and 'logout' in text.lower()
        except Exception as e:
            logger.error(f"❌ Ошибка проверки авторизации: {e}")
            return False
    
    async def _refresh_owned_games_cache(self) -> bool:
        app_ids = set()
        total_count = 0
        
        # Steam Web API
        api_key = os.getenv('STEAM_API_KEY', '')
        if api_key and self.steam_id:
            try:
                session = await self._get_session()
                await self.rate_limiter.acquire()
                
                async with session.get(
                    'https://api.steampowered.com/IPlayerService/GetOwnedGames/v1/',
                    params={'key': api_key, 'steamid': self.steam_id, 'include_appinfo': False}
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if 'response' in data and 'games' in data['response']:
                            for game in data['response']['games']:
                                app_ids.add(str(game['appid']))
                            total_count = len(app_ids)
                            logger.info(f"📚 API: найдено {total_count} игр")
            except Exception as e:
                logger.debug(f"⚠️ API не доступен: {e}")
        
        # Парсинг лицензий
        if total_count == 0:
            try:
                session = await self._get_session()
                await self.rate_limiter.acquire()
                
                async with session.get(
                    f'{STEAM_CONFIG["api_store"]}/account/licenses/',
                    cookies=self.cookies
                ) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        soup = BeautifulSoup(text, 'html.parser')
                        table = soup.find('table', class_='account_table')
                        if table:
                            rows = table.find_all('tr')
                            total_count = len(rows) - 1
                            logger.info(f"📚 Парсинг: найдено ~{total_count} лицензий")
                        
                        for link in soup.find_all('a', href=True):
                            match = re.search(r'/app/(\d+)', link['href'])
                            if match:
                                app_ids.add(match.group(1))
            except Exception as e:
                logger.debug(f"⚠️ Не удалось спарсить лицензии: {e}")
        
        if total_count > 0 or app_ids:
            self._owned_games_cache = app_ids
            self._cache_timestamp = time.time()
            self._cache_count = total_count
            
            cache_data = {
                'app_ids': list(app_ids),
                'count': total_count,
                'timestamp': self._cache_timestamp,
                'steam_id': self.steam_id
            }
            
            try:
                with open(OWNED_GAMES_CACHE_FILE, 'w', encoding='utf-8') as f:
                    json.dump(cache_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.error(f"❌ Ошибка сохранения кэша: {e}")
            
            logger.info(f"✅ Кэш обновлён: {total_count} элементов")
            return True
        
        self._load_owned_games_cache()
        return False
    
    def _load_owned_games_cache(self):
        try:
            if OWNED_GAMES_CACHE_FILE.exists():
                with open(OWNED_GAMES_CACHE_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if data and 'app_ids' in data:
                        self._owned_games_cache = set(str(x) for x in data['app_ids'])
                        self._cache_timestamp = data.get('timestamp', 0)
                        self._cache_count = data.get('count', len(self._owned_games_cache))
                        logger.info(f"📚 Загружен кэш: {self._cache_count} игр")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки кэша: {e}")
    
    async def is_game_owned(self, app_id: str, force_refresh: bool = False) -> bool:
        ttl = 3600
        if force_refresh or not self._cache_timestamp or (time.time() - self._cache_timestamp) > ttl:
            await self._refresh_owned_games_cache()
        
        if str(app_id) in self._owned_games_cache:
            return True
        
        return await self._check_game_page_owned(app_id)
    
    async def _check_game_page_owned(self, app_id: str) -> bool:
        try:
            session = await self._get_session()
            await self.rate_limiter.acquire()
            
            async with session.get(
                f'{STEAM_CONFIG["api_store"]}/app/{app_id}/',
                cookies=self.cookies
            ) as resp:
                if resp.status != 200:
                    return False
                
                text = await resp.text()
                soup = BeautifulSoup(text, 'html.parser')
                
                btn = soup.find('div', class_='btnv6_blue_hoverfade btn_medium')
                if btn and ('в библиотеке' in btn.get_text().lower() or 'in library' in btn.get_text().lower()):
                    return True
                
                if not soup.find('form', {'name': lambda x: x and x.startswith('add_to_cart_')}):
                    return True
                
                if soup.find(class_='ds_owned_flag'):
                    return True
                
                return False
        except:
            return False
    
    async def get_total_library_count(self) -> int:
        if not self._owned_games_cache:
            await self._refresh_owned_games_cache()
        return self._cache_count or len(self._owned_games_cache)
    
    async def get_page(self, app_id: str) -> Optional[BeautifulSoup]:
        try:
            session = await self._get_session()
            await self.rate_limiter.acquire()
            
            async with session.get(
                f'{STEAM_CONFIG["api_store"]}/app/{app_id}/',
                cookies=self.cookies
            ) as resp:
                if resp.status != 200 or 'login' in str(resp.url).lower():
                    if 'login' in str(resp.url).lower():
                        self.logged_in = False
                    return None
                
                text = await resp.text()
                return BeautifulSoup(text, 'html.parser')
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки страницы {app_id}: {e}")
            return None
    
    def extract_info(self, soup: BeautifulSoup, app_id: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        subid, snr, originating_snr = None, None, None
        form = soup.find('form', {'name': lambda x: x and x.startswith('add_to_cart_')})
        if form:
            for inp in form.find_all('input'):
                name = inp.get('name')
                if name == 'subid':
                    subid = inp.get('value', '').strip()
                elif name == 'snr':
                    snr = inp.get('value', '').strip()
                elif name == 'originating_snr':
                    originating_snr = inp.get('value', '').strip()
        
        if not subid:
            for inp in soup.find_all('input', {'name': 'subid'}):
                if inp.get('value', '').strip().isdigit():
                    subid = inp.get('value', '').strip()
                    break
        
        return subid, snr, originating_snr
    
    async def add_free_game(self, app_id: str, subid: str, snr: Optional[str] = None, originating_snr: Optional[str] = None) -> Tuple[bool, str]:
        if not all([self.logged_in, self.session_id]):
            return False, "❌ Не авторизован"
        
        try:
            session = await self._get_session()
            await self.rate_limiter.acquire()
            
            data = {
                'action': 'add_to_cart',
                'sessionid': self.session_id,
                'subid': subid,
                'snr': snr or '1_5_9__403',
                'originating_snr': originating_snr or '1_direct-navigation__',
            }
            
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': STEAM_CONFIG['api_store'],
                'Referer': f'{STEAM_CONFIG["api_store"]}/app/{app_id}/',
            }
            
            logger.info(f"🎮 Добавление {app_id}")
            async with session.post(
                f'{STEAM_CONFIG["api_store"]}/freelicense/addfreelicense/',
                data=data,
                cookies=self.cookies,
                headers=headers
            ) as resp:
                text = await resp.text()
                text_lower = text.lower()
                
                if 'успешно' in text_lower or 'success' in text_lower:
                    return True, "✅ Добавлено!"
                if 'already in library' in text_lower or 'уже в библиотеке' in text_lower:
                    return True, "✅ Уже есть!"
                return False, "⚠️ Ошибка добавления"
        except Exception as e:
            logger.error(f"❌ Ошибка добавления: {e}")
            return False, f"❌ Ошибка"

# ============================================================================
# ПОИСК ИГР
# ============================================================================
class SmartGameFinder:
    """Поисковик игр"""
    def __init__(self, steam_session: SteamSession):
        self.steam = steam_session
    
    async def find_all_pages(self, max_pages: int, progress_callback=None) -> List[GameInfo]:
        all_games = []
        seen_app_ids = set()
        
        for page in range(1, max_pages + 1):
            if progress_callback:
                await progress_callback(page, max_pages)
            
            try:
                logger.info(f"📄 Страница {page}...")
                session = await self.steam._get_session()
                await self.steam.rate_limiter.acquire()
                
                async with session.get(
                    f'{STEAM_CONFIG["api_store"]}/search/',
                    params={
                        'maxprice': 'free',
                        'specials': '1',
                        'page': page,
                        'cc': 'ru',
                        'l': 'russian'
                    }
                ) as resp:
                    if resp.status != 200:
                        break
                    
                    text = await resp.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    rows = soup.find_all('a', class_='search_result_row')
                    
                    if not rows:
                        break
                    
                    for row in rows:
                        app_id = row.get('data-ds-appid')
                        if not app_id:
                            href = row.get('href', '')
                            match = re.search(r'/app/(\d+)', href)
                            if match:
                                app_id = match.group(1)
                            else:
                                continue
                        
                        if app_id in seen_app_ids:
                            continue
                        seen_app_ids.add(app_id)
                        
                        game = self._parse_game_row(row, str(app_id))
                        if game:
                            all_games.append(game)
                    
                    logger.info(f"  Найдено {len(rows)} игр (всего: {len(all_games)})")
                    await asyncio.sleep(STEAM_CONFIG['delay'])
                    
            except Exception as e:
                logger.error(f"❌ Ошибка на странице {page}: {e}")
                break
        
        return all_games
    
    async def find_discount_games(self, max_pages: int, min_discount_percent: int = 0,
                                  max_games: int = 50, progress_callback=None) -> List[GameInfo]:
        """Поиск игр со скидками с фильтром по минимальному проценту"""
        all_games = []
        seen_app_ids = set()
        
        for page in range(1, max_pages + 1):
            if len(all_games) >= max_games:
                break
                
            if progress_callback:
                await progress_callback(page, max_pages)
            
            try:
                logger.info(f"🏷️ Страница скидок {page}...")
                session = await self.steam._get_session()
                await self.steam.rate_limiter.acquire()
                
                async with session.get(
                    f'{STEAM_CONFIG["api_store"]}/search/',
                    params={
                        'specials': '1',
                        'page': page,
                        'cc': 'ru',
                        'l': 'russian'
                    }
                ) as resp:
                    if resp.status != 200:
                        break
                    
                    text = await resp.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    rows = soup.find_all('a', class_='search_result_row')
                    
                    if not rows:
                        break
                    
                    for row in rows:
                        if len(all_games) >= max_games:
                            break
                            
                        app_id = row.get('data-ds-appid')
                        if not app_id:
                            href = row.get('href', '')
                            match = re.search(r'/app/(\d+)', href)
                            if match:
                                app_id = match.group(1)
                            else:
                                continue
                        
                        if app_id in seen_app_ids:
                            continue
                        seen_app_ids.add(app_id)
                        
                        game = self._parse_game_row(row, str(app_id), check_discount=True)
                        if game and game.discount_value >= min_discount_percent:
                            all_games.append(game)
                    
                    logger.info(f"  Найдено {len(rows)} игр (отобрано: {len(all_games)})")
                    await asyncio.sleep(STEAM_CONFIG['delay'])
                    
            except Exception as e:
                logger.error(f"❌ Ошибка на странице {page}: {e}")
                break
        
        # Сортировка по величине скидки (от большей к меньшей)
        all_games.sort(key=lambda x: x.discount_value, reverse=True)
        
        # Ограничение по количеству
        if len(all_games) > max_games:
            all_games = all_games[:max_games]
        
        return all_games
    
    def _parse_game_row(self, row, app_id: str, check_discount: bool = False) -> Optional[GameInfo]:
        try:
            title_elem = row.find('span', class_='title') or row.find('div', class_='col search_name ellipsis')
            if not title_elem:
                return None
            
            name = title_elem.get_text(strip=True)
            is_owned = row.get('class') and 'ds_owned' in row.get('class', [])
            
            discount_block = row.find('div', class_='discount_block')
            
            if check_discount and not discount_block:
                # Если ищем скидки и блок скидки отсутствует - пропускаем
                return None
            
            original_price, discount_percent, final_price, discount_value = self._parse_price(discount_block)
            
            return GameInfo(
                app_id=app_id,
                name=name,
                url=f'{STEAM_CONFIG["api_store"]}/app/{app_id}/',
                original_price=original_price,
                discount_percent=discount_percent,
                final_price=final_price,
                is_owned=is_owned,
                discount_value=discount_value
            )
        except Exception as e:
            logger.debug(f"Ошибка парсинга: {e}")
            return None
    
    def _parse_price(self, discount_block) -> Tuple[Optional[str], Optional[str], Optional[str], int]:
        if not discount_block:
            return None, None, None, 0
        
        original = discount_block.find('div', class_='discount_original_price')
        final = discount_block.find('div', class_='discount_final_price')
        pct = discount_block.find('div', class_='discount_pct')
        
        discount_value = 0
        if pct:
            pct_text = pct.get_text(strip=True)
            # Извлекаем число из строки типа "-50%"
            match = re.search(r'(\d+)', pct_text)
            if match:
                discount_value = int(match.group(1))
        
        return (
            original.get_text(strip=True) if original else None,
            pct.get_text(strip=True) if pct else '0%',
            final.get_text(strip=True) if final else '0 руб.',
            discount_value
        )

# ============================================================================
# НАСТРОЙКИ
# ============================================================================
class SettingsManager:
    """Менеджер настроек"""
    def __init__(self, file_path: Path = SETTINGS_FILE):
        self.file_path = file_path
        self.defaults = {
            'auto_add_mode': AutoAddMode.EXPENSIVE.value,
            'auto_add_price_threshold': 500.0,
            'max_pages': BOT_CONFIG['max_pages'],
            'check_interval': BOT_CONFIG['interval'],
            'library_cache_ttl': 3600,
            'min_discount_percent': 50,  # Минимальный процент скидки по умолчанию
            'max_discount_pages': BOT_CONFIG['max_discount_pages'],
            'max_discount_games': BOT_CONFIG['max_discount_games'],
        }
        self.settings = self.load()
        logger.info(f"⚙️ Настройки загружены: {len(self.settings)} параметров")
    
    def load(self) -> Dict:
        try:
            if self.file_path.exists():
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if 'price_threshold' in data:
                        data['auto_add_price_threshold'] = data.pop('price_threshold')
                    # Удаляем старый ключ discount_type если есть
                    if 'discount_type' in data:
                        del data['discount_type']
                    return {**self.defaults, **data}
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки настроек: {e}")
        return self.defaults.copy()
    
    def save(self) -> bool:
        try:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.file_path, 'w', encoding='utf-8') as f:
                json.dump(self.settings, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения: {e}")
            return False
    
    def get(self, key: str, default=None):
        return self.settings.get(key, self.defaults.get(key, default))
    
    def set(self, key: str, value) -> bool:
        old = self.settings.get(key)
        self.settings[key] = value
        logger.info(f"⚙️ {key}: {old} → {value}")
        return self.save()

# ============================================================================
# ОСНОВНОЙ БОТ
# ============================================================================
class SteamBot:
    """Основной класс бота"""
    def __init__(self):
        self.steam = SteamSession()
        self.finder = SmartGameFinder(self.steam)
        self.settings = SettingsManager()
        self.db = DatabaseManager()
        self.health = HealthMonitor()
        self.app: Optional[Application] = None
        self._is_running = False
        
        logger.info(f"🤖 Бот инициализирован | Режим: {self._get_mode_description()}")
    
    async def initialize(self):
        """Инициализация компонентов"""
        await self.steam.init_session()
        logger.info(f"🤖 Бот готов | Steam: {'✅' if self.steam.logged_in else '❌'} | Режим: {self._get_mode_description()}")
    
    async def shutdown(self):
        """Корректное завершение"""
        self._is_running = False
        await self.steam.close()
        if self.app:
            await self.app.shutdown()
        logger.info("👋 Бот остановлен")
    
    def _get_mode_description(self):
        mode = self.settings.get('auto_add_mode')
        if mode == AutoAddMode.ALL.value:
            return "Добавлять ВСЁ"
        elif mode == AutoAddMode.EXPENSIVE.value:
            return f"Добавлять от {self.settings.get('auto_add_price_threshold')}₽"
        else:
            return "Выключено"
    
    async def _restricted(self, update: Update) -> bool:
        if update.effective_user.id != BOT_CONFIG['admin_id']:
            if update.message:
                await update.message.reply_text("⛔ Доступ запрещен.")
            elif update.callback_query:
                await update.callback_query.answer("⛔ Доступ запрещен.", show_alert=True)
            return True
        return False
    
    def _get_menu_keyboard(self):
        keyboard = [
            [KeyboardButton("🔍 Проверить игры"), KeyboardButton("🏷️ Скидки")],
            [KeyboardButton("⚙️ Настройки"), KeyboardButton("📊 Статистика")],
            [KeyboardButton("🔄 Обновить кэш"), KeyboardButton("🏥 Статус")]
        ]
        return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    
    def _get_game_keyboard(self, app_id: str, is_owned: bool = False, is_discount: bool = False) -> Optional[InlineKeyboardMarkup]:
        """Создает клавиатуру для игры с учетом статуса владения и типа игры"""
        if is_owned:
            # Если игра уже в библиотеке - показываем только ссылку
            return InlineKeyboardMarkup([[
                InlineKeyboardButton("📚 В библиотеке", callback_data="already_owned"),
                InlineKeyboardButton("🔗 Steam", url=f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')
            ]])
        elif self.steam.logged_in and not is_discount:
            # Для бесплатных игр - кнопка добавления
            return InlineKeyboardMarkup([[
                InlineKeyboardButton("🎮 Добавить в библиотеку", callback_data=f"add:{app_id}"),
                InlineKeyboardButton("🔗 Steam", url=f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')
            ]])
        else:
            # Для платных игр или без авторизации - только ссылка
            return InlineKeyboardMarkup([[
                InlineKeyboardButton("🔗 Открыть в Steam", url=f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')
            ]])
    
    async def _send_or_edit(self, update: Update, text: str, reply_markup=None, parse_mode='HTML'):
        try:
            if update.callback_query:
                await update.callback_query.edit_message_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
            elif update.message:
                await update.message.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
        except Exception as e:
            logger.debug(f"⚠️ Ошибка отправки: {e}")
    
    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if await self._restricted(update):
            return
        
        min_discount = self.settings.get('min_discount_percent', 50)
        
        await update.message.reply_text(
            f"👋 <b>Персональный Steam Бот</b>\n"
            f"🔐 Steam: {'✅' if self.steam.logged_in else '❌'}\n"
            f"⚙️ Режим: <b>{self._get_mode_description()}</b>\n"
            f"📄 Страниц: {self.settings.get('max_pages')}\n"
            f"🏷️ Мин. скидка: <b>{min_discount}%</b>\n"
            f"Используй меню для управления.",
            parse_mode='HTML',
            reply_markup=self._get_menu_keyboard()
        )
    
    async def _show_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if await self._restricted(update):
            return
        await self._send_or_edit(update, "Главное меню:", reply_markup=self._get_menu_keyboard())
    
    async def start_discount_check(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Начало процесса поиска скидок - запрос процента"""
        if await self._restricted(update):
            return ConversationHandler.END
        
        min_discount = self.settings.get('min_discount_percent', 50)
        
        keyboard = [[InlineKeyboardButton(f"Использовать текущий ({min_discount}%)", callback_data=f"use_default_discount:{min_discount}")]]
        
        await update.message.reply_text(
            f"🏷️ <b>Введите минимальный процент скидки (1-99):</b>\n\n"
            f"Текущее значение: {min_discount}%\n\n"
            f"Например:\n"
            f"• 85 - показывать скидки от 85% и выше\n"
            f"• 50 - показывать скидки от 50% и выше\n"
            f"• 25 - показывать скидки от 25% и выше\n\n"
            f"Или нажмите кнопку для использования текущего значения.",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        
        return WAITING_DISCOUNT_PERCENT
    
    async def process_discount_percent(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка введенного процента скидки"""
        if await self._restricted(update):
            return ConversationHandler.END
        
        try:
            percent = int(update.message.text.strip())
            
            if percent < 1 or percent > 99:
                await update.message.reply_text(
                    "⚠️ Пожалуйста, введите число от 1 до 99.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("❌ Отмена", callback_data="cancel_discount")
                    ]])
                )
                return WAITING_DISCOUNT_PERCENT
            
            # Сохраняем значение
            self.settings.set('min_discount_percent', percent)
            
            # Запускаем поиск скидок
            await self._run_discount_check(update, context, min_discount=percent)
            
            return ConversationHandler.END
            
        except ValueError:
            await update.message.reply_text(
                "⚠️ Пожалуйста, введите число от 1 до 99.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("❌ Отмена", callback_data="cancel_discount")
                ]])
            )
            return WAITING_DISCOUNT_PERCENT
    
    async def use_default_discount(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Использование текущего значения процента скидки"""
        query = update.callback_query
        await query.answer()
        
        percent = int(query.data.split(':')[1])
        
        # Запускаем поиск с текущим значением
        await query.message.delete()
        await self._run_discount_check_callback(update, context, min_discount=percent)
        
        return ConversationHandler.END
    
    async def cancel_discount(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отмена поиска скидок"""
        query = update.callback_query
        await query.answer()
        
        await query.message.delete()
        await self._show_menu(update, context)
        
        return ConversationHandler.END
    
    async def _run_discount_check_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE, min_discount: int = None):
        """Запуск поиска скидок из callback query"""
        if min_discount is None:
            min_discount = self.settings.get('min_discount_percent', 50)
        
        chat_id = BOT_CONFIG['admin_id']
        
        status_msg = await self.app.bot.send_message(chat_id, "🔍 Поиск игр со скидками...")
        
        max_pages = self.settings.get('max_discount_pages', BOT_CONFIG['max_discount_pages'])
        max_games = self.settings.get('max_discount_games', BOT_CONFIG['max_discount_games'])
        
        async def progress_callback(current, total):
            try:
                await status_msg.edit_text(f"🔍 Поиск скидок от {min_discount}%... 📄 {current}/{total}")
            except:
                pass
        
        discount_games = await self.finder.find_discount_games(
            max_pages=max_pages,
            min_discount_percent=min_discount,
            max_games=max_games,
            progress_callback=progress_callback
        )
        
        await self.health.record_metric('games_checked', len(discount_games))
        
        if not discount_games:
            await status_msg.edit_text(f"😔 Игр со скидкой от {min_discount}% не найдено.")
            return
        
        await status_msg.edit_text(f"✅ Найдено {len(discount_games)} игр со скидкой ≥{min_discount}%. Отправляю...")
        
        # Отправляем сводку
        summary = (
            f"🏷️ <b>Найдено игр со скидкой ≥{min_discount}%: {len(discount_games)}</b>\n"
            f"📄 Проверено страниц: {max_pages}\n"
            f"━━━━━━━━━━━━━━━━━━━━━"
        )
        await self.app.bot.send_message(chat_id, summary, parse_mode='HTML')
        
        # Отправляем каждую игру отдельной карточкой
        for i, game in enumerate(discount_games, 1):
            # Определяем эмодзи для скидки
            discount_emoji = "🔥" if game.discount_value >= 75 else "🎯" if game.discount_value >= 50 else "💎" if game.discount_value >= 25 else "🏷️"
            
            game_text = (
                f"{discount_emoji} <b>#{i} {game.name}</b>\n"
                f"💰 <s>{game.original_price or 'N/A'}</s> → <b>{game.final_price or 'N/A'}</b>\n"
                f"📉 Скидка: <b>{game.discount_percent or '0%'}</b>\n"
                f"🔗 <a href='{game.url}'>Открыть в Steam</a>"
            )
            
            keyboard = self._get_game_keyboard(game.app_id, game.is_owned, is_discount=True)
            
            await self.app.bot.send_message(
                chat_id,
                game_text,
                parse_mode='HTML',
                reply_markup=keyboard
            )
            
            # Небольшая задержка между сообщениями
            if i % 5 == 0:
                await asyncio.sleep(0.5)
        
        # Удаляем статусное сообщение
        await status_msg.delete()
        
        # Отправляем итоговое сообщение
        total_discount_value = sum(g.discount_value for g in discount_games)
        avg_discount = total_discount_value / len(discount_games) if discount_games else 0
        
        final_summary = (
            f"✅ <b>Проверка скидок завершена!</b>\n"
            f"📊 Всего найдено: <b>{len(discount_games)}</b> игр\n"
            f"📈 Средняя скидка: <b>{avg_discount:.1f}%</b>\n"
            f"🔥 Максимальная скидка: <b>{max(g.discount_value for g in discount_games)}%</b>\n"
            f"🏷️ Минимальный порог: <b>{min_discount}%</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 Используй кнопки под каждой игрой для перехода в Steam"
        )
        await self.app.bot.send_message(chat_id, final_summary, parse_mode='HTML')
    
    async def _run_discount_check(self, update: Update, context: ContextTypes.DEFAULT_TYPE, min_discount: int = None):
        """Запуск поиска скидок из message"""
        if min_discount is None:
            min_discount = self.settings.get('min_discount_percent', 50)
        
        chat_id = BOT_CONFIG['admin_id']
        
        status_msg = await update.message.reply_text("🔍 Поиск игр со скидками...")
        
        max_pages = self.settings.get('max_discount_pages', BOT_CONFIG['max_discount_pages'])
        max_games = self.settings.get('max_discount_games', BOT_CONFIG['max_discount_games'])
        
        async def progress_callback(current, total):
            try:
                await status_msg.edit_text(f"🔍 Поиск скидок от {min_discount}%... 📄 {current}/{total}")
            except:
                pass
        
        discount_games = await self.finder.find_discount_games(
            max_pages=max_pages,
            min_discount_percent=min_discount,
            max_games=max_games,
            progress_callback=progress_callback
        )
        
        await self.health.record_metric('games_checked', len(discount_games))
        
        if not discount_games:
            await status_msg.edit_text(f"😔 Игр со скидкой от {min_discount}% не найдено.")
            return
        
        await status_msg.edit_text(f"✅ Найдено {len(discount_games)} игр со скидкой ≥{min_discount}%. Отправляю...")
        
        # Отправляем сводку
        summary = (
            f"🏷️ <b>Найдено игр со скидкой ≥{min_discount}%: {len(discount_games)}</b>\n"
            f"📄 Проверено страниц: {max_pages}\n"
            f"━━━━━━━━━━━━━━━━━━━━━"
        )
        await update.message.reply_text(summary, parse_mode='HTML')
        
        # Отправляем каждую игру отдельной карточкой
        for i, game in enumerate(discount_games, 1):
            # Определяем эмодзи для скидки
            discount_emoji = "🔥" if game.discount_value >= 75 else "🎯" if game.discount_value >= 50 else "💎" if game.discount_value >= 25 else "🏷️"
            
            game_text = (
                f"{discount_emoji} <b>#{i} {game.name}</b>\n"
                f"💰 <s>{game.original_price or 'N/A'}</s> → <b>{game.final_price or 'N/A'}</b>\n"
                f"📉 Скидка: <b>{game.discount_percent or '0%'}</b>\n"
                f"🔗 <a href='{game.url}'>Открыть в Steam</a>"
            )
            
            keyboard = self._get_game_keyboard(game.app_id, game.is_owned, is_discount=True)
            
            await self.app.bot.send_message(
                chat_id,
                game_text,
                parse_mode='HTML',
                reply_markup=keyboard
            )
            
            # Небольшая задержка между сообщениями
            if i % 5 == 0:
                await asyncio.sleep(0.5)
        
        # Удаляем статусное сообщение
        await status_msg.delete()
        
        # Отправляем итоговое сообщение
        total_discount_value = sum(g.discount_value for g in discount_games)
        avg_discount = total_discount_value / len(discount_games) if discount_games else 0
        
        final_summary = (
            f"✅ <b>Проверка скидок завершена!</b>\n"
            f"📊 Всего найдено: <b>{len(discount_games)}</b> игр\n"
            f"📈 Средняя скидка: <b>{avg_discount:.1f}%</b>\n"
            f"🔥 Максимальная скидка: <b>{max(g.discount_value for g in discount_games)}%</b>\n"
            f"🏷️ Минимальный порог: <b>{min_discount}%</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"💡 Используй кнопки под каждой игрой для перехода в Steam"
        )
        await update.message.reply_text(final_summary, parse_mode='HTML')
    
    async def _run_check(self, update: Update, context: ContextTypes.DEFAULT_TYPE, is_scheduled=False):
        chat_id = BOT_CONFIG['admin_id']
        if not self.app:
            return
        
        status_msg = None
        if not is_scheduled and update and update.message:
            status_msg = await update.message.reply_text("🔄 Запуск проверки...")
        
        max_pages = self.settings.get('max_pages')
        auto_mode = self.settings.get('auto_add_mode')
        price_threshold = self.settings.get('auto_add_price_threshold')
        
        async def progress_callback(current, total):
            if status_msg:
                try:
                    await status_msg.edit_text(f"🔄 Поиск... 📄 {current}/{total}")
                except:
                    pass
        
        all_games = await self.finder.find_all_pages(max_pages, progress_callback)
        await self.health.record_metric('games_checked', len(all_games))
        
        if not all_games:
            if not is_scheduled and status_msg:
                await status_msg.edit_text("😔 Ничего не найдено.")
            return
        
        if status_msg:
            await status_msg.edit_text(f"📊 Найдено {len(all_games)}. Проверяю библиотеку...")
        
        new_games = []
        added_games = []
        
        for game in all_games:
            if is_scheduled:
                known = await self.db.execute_query(
                    "SELECT * FROM known_games WHERE app_id = ? AND notified = 1",
                    (game.app_id,)
                )
                if known:
                    continue
            
            in_library = game.is_owned or await self.steam.is_game_owned(game.app_id)
            game.is_owned = in_library
            
            if in_library:
                await self.db.add_or_update_game(game)
                continue
            
            new_games.append(game)
            
            should_add = False
            if auto_mode != AutoAddMode.OFF.value:
                if auto_mode == AutoAddMode.ALL.value:
                    should_add = True
                elif auto_mode == AutoAddMode.EXPENSIVE.value:
                    if game.original_price:
                        try:
                            price_val = float(game.original_price.replace(' ', '').replace('руб.', '').replace(',', '.'))
                            should_add = price_val >= price_threshold
                        except ValueError:
                            pass
            
            added = False
            if should_add:
                soup = await self.steam.get_page(game.app_id)
                if soup:
                    subid, snr, originating_snr = self.steam.extract_info(soup, game.app_id)
                    if subid:
                        added, _ = await self.steam.add_free_game(game.app_id, subid, snr, originating_snr)
                        if added:
                            added_games.append(game)
                            await self.health.record_metric('games_added')
            
            game.is_owned = added or in_library
            await self.db.add_or_update_game(game)
        
        # Удаляем статусное сообщение перед отправкой результатов
        if not is_scheduled and status_msg:
            await status_msg.delete()
            status_msg = None
        
        if new_games or not is_scheduled:
            if is_scheduled and not new_games:
                logger.info("📅 Плановая проверка: новых игр не найдено")
                return
            
            # Отправляем каждую новую игру отдельным сообщением с кнопками
            for game in new_games:
                game_text = (
                    f"🎮 <b>{game.name}</b>\n"
                    f"💰 {game.original_price or 'N/A'} → {game.final_price or '0 руб.'} ({game.discount_percent or '0%'})\n"
                    f"🔗 <a href='{game.url}'>Открыть в Steam</a>"
                )
                
                keyboard = self._get_game_keyboard(game.app_id, game.is_owned)
                
                if not is_scheduled and chat_id:
                    await self.app.bot.send_message(
                        chat_id,
                        game_text,
                        parse_mode='HTML',
                        reply_markup=keyboard
                    )
                elif is_scheduled and chat_id:
                    await self.app.bot.send_message(
                        chat_id,
                        game_text,
                        parse_mode='HTML',
                        reply_markup=keyboard
                    )
            
            # Отправляем сводку об автоматически добавленных играх
            if added_games:
                summary = f"🎁 <b>Автоматически добавлено игр: {len(added_games)}</b>"
                if not is_scheduled and chat_id:
                    await self.app.bot.send_message(chat_id, summary, parse_mode='HTML')
                elif is_scheduled and chat_id:
                    await self.app.bot.send_message(chat_id, summary, parse_mode='HTML')
            
            # Если новых игр нет и это ручная проверка
            if not new_games and not is_scheduled:
                await self.app.bot.send_message(
                    chat_id,
                    "✅ Проверка завершена! Новых игр не найдено.",
                    parse_mode='HTML'
                )
    
    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик нажатий на кнопки"""
        query = update.callback_query
        
        if query.data == "already_owned":
            await query.answer("📚 Эта игра уже в вашей библиотеке!", show_alert=True)
            return
        
        if not query.data.startswith('add:'):
            return
        
        await query.answer("⏳ Добавляю игру...")
        
        app_id = query.data.split(':')[1]
        
        if not self.steam.logged_in:
            await query.answer("❌ Steam не авторизован!", show_alert=True)
            return
        
        if await self.steam.is_game_owned(app_id):
            await query.answer("📚 Игра уже в библиотеке!", show_alert=True)
            # Обновляем клавиатуру
            new_keyboard = self._get_game_keyboard(app_id, is_owned=True)
            try:
                await query.edit_message_reply_markup(reply_markup=new_keyboard)
            except Exception as e:
                logger.debug(f"⚠️ Ошибка обновления клавиатуры: {e}")
            return
        
        # Получаем оригинальный текст сообщения
        original_text = query.message.text_html if query.message.text else query.message.caption
        
        # Временно меняем сообщение для отображения прогресса
        await query.edit_message_text(
            f"{original_text}\n\n⏳ <i>Добавление в библиотеку...</i>",
            parse_mode='HTML',
            reply_markup=None
        )
        
        soup = await self.steam.get_page(app_id)
        if not soup:
            await query.edit_message_text(
                f"{original_text}\n\n❌ <b>Ошибка загрузки страницы</b>",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔄 Попробовать снова", callback_data=f"add:{app_id}"),
                    InlineKeyboardButton("🔗 Steam", url=f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')
                ]])
            )
            return
        
        subid, snr, originating_snr = self.steam.extract_info(soup, app_id)
        if not subid:
            await query.edit_message_text(
                f"{original_text}\n\n⚠️ <b>Не удалось определить subid игры</b>",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔗 Открыть в Steam", url=f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')
                ]])
            )
            return
        
        added, msg = await self.steam.add_free_game(app_id, subid, snr, originating_snr)
        
        if added:
            await self.health.record_metric('games_added')
            # Обновляем клавиатуру - убираем кнопку добавления
            new_keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ В библиотеке", callback_data="already_owned"),
                InlineKeyboardButton("🔗 Steam", url=f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')
            ]])
            
            await query.edit_message_text(
                f"{original_text}\n\n✅ <b>Игра успешно добавлена в библиотеку!</b>",
                parse_mode='HTML',
                reply_markup=new_keyboard
            )
        else:
            await query.edit_message_text(
                f"{original_text}\n\n❌ <b>Не удалось добавить игру</b>\n<i>{msg}</i>",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔄 Попробовать снова", callback_data=f"add:{app_id}"),
                    InlineKeyboardButton("🔗 Steam", url=f'{STEAM_CONFIG["api_store"]}/app/{app_id}/')
                ]])
            )
    
    async def settings_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if await self._restricted(update):
            return
        
        min_discount = self.settings.get('min_discount_percent', 50)
        
        keyboard = [
            [InlineKeyboardButton(f"📋 Режим: {self._get_mode_description()}", callback_data="set_mode")],
            [InlineKeyboardButton(f"💰 Порог цены: {self.settings.get('auto_add_price_threshold')}₽", callback_data="set_price")],
            [InlineKeyboardButton(f"📄 Страниц поиска: {self.settings.get('max_pages')}", callback_data="set_pages")],
            [InlineKeyboardButton(f"🏷️ Мин. скидка: {min_discount}%", callback_data="set_min_discount")],
            [InlineKeyboardButton(f"📄 Страниц скидок: {self.settings.get('max_discount_pages')}", callback_data="set_discount_pages")],
            [InlineKeyboardButton(f"🎮 Макс. игр в скидках: {self.settings.get('max_discount_games')}", callback_data="set_max_discount")],
            [InlineKeyboardButton("🗑 Сбросить историю", callback_data="reset_history")],
            [InlineKeyboardButton("🔄 Обновить кэш библиотеки", callback_data="refresh_cache")],
            [InlineKeyboardButton("🔙 Назад", callback_data="main_menu")]
        ]
        
        await self._send_or_edit(update, "⚙️ <b>Настройки</b>", reply_markup=InlineKeyboardMarkup(keyboard))
    
    async def settings_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()
        data = query.data
        
        if data == "main_menu":
            await query.message.delete()
            await self._show_menu(update, context)
            return
        
        if data == "reset_history":
            await self.db.execute_query("DELETE FROM known_games")
            await query.answer("✅ История сброшена!", show_alert=True)
            await self.settings_menu(update, context)
            return
        
        if data == "refresh_cache":
            await query.answer("🔄 Обновление кэша...", show_alert=True)
            success = await self.steam._refresh_owned_games_cache()
            count = await self.steam.get_total_library_count()
            await query.answer(f"{'✅' if success else '⚠️'} Кэш: {count} игр", show_alert=True)
            await self.settings_menu(update, context)
            return
        
        if data == "set_mode":
            keyboard = [
                [InlineKeyboardButton("✅ Всё подряд", callback_data="mode:all")],
                [InlineKeyboardButton(f"💰 Только дорогие (≥{self.settings.get('auto_add_price_threshold')}₽)", callback_data="mode:expensive")],
                [InlineKeyboardButton("❌ Выключить", callback_data="mode:off")],
                [InlineKeyboardButton("🔙 Назад", callback_data="settings_menu")]
            ]
            await query.edit_message_text("Выбери режим авто-добавления:", reply_markup=InlineKeyboardMarkup(keyboard))
        
        elif data.startswith("mode:"):
            self.settings.set('auto_add_mode', data.split(":")[1])
            await query.answer(f"Режим изменен на: {self._get_mode_description()}")
            await self.settings_menu(update, context)
        
        elif data == "set_min_discount":
            current = self.settings.get('min_discount_percent', 50)
            await query.edit_message_text(
                f"🏷️ <b>Введите минимальный процент скидки (1-99):</b>\n\n"
                f"Текущее значение: {current}%\n\n"
                f"Например:\n"
                f"• 85 - показывать скидки от 85% и выше\n"
                f"• 50 - показывать скидки от 50% и выше",
                parse_mode='HTML',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Отмена", callback_data="settings_menu")]])
            )
            context.user_data['awaiting'] = 'min_discount_percent'
        
        elif data in ["set_price", "set_pages", "set_discount_pages", "set_max_discount"]:
            prompts = {
                "set_price": "Введите новый порог цены (в рублях):",
                "set_pages": f"Введите кол-во страниц для поиска бесплатных игр (1-50):",
                "set_discount_pages": f"Введите кол-во страниц для поиска скидок (1-50):",
                "set_max_discount": f"Введите максимальное кол-во игр со скидками (10-200):"
            }
            keys = {
                "set_price": "auto_add_price_threshold",
                "set_pages": "max_pages",
                "set_discount_pages": "max_discount_pages",
                "set_max_discount": "max_discount_games"
            }
            
            await query.edit_message_text(
                prompts[data],
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Отмена", callback_data="settings_menu")]])
            )
            context.user_data['awaiting'] = keys[data]
        
        elif data == "settings_menu":
            await query.message.delete()
            await self.settings_menu(update, context)
    
    async def text_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if await self._restricted(update):
            return
        
        text = update.message.text
        
        if context.user_data.get('awaiting'):
            try:
                if context.user_data['awaiting'] == 'min_discount_percent':
                    value = int(text)
                    if value < 1 or value > 99:
                        await update.message.reply_text("⚠️ Пожалуйста, введите число от 1 до 99.")
                        return
                else:
                    value = float(text) if 'price' in context.user_data['awaiting'] else int(text)
                
                setting_key = context.user_data['awaiting']
                
                if self.settings.set(setting_key, value):
                    await update.message.reply_text(f"✅ Настройка обновлена: {value}")
                else:
                    await update.message.reply_text("❌ Ошибка обновления настройки")
                
                context.user_data.pop('awaiting', None)
                await self._show_menu(update, context)
                return
            
            except ValueError:
                await update.message.reply_text("⚠️ Некорректное значение. Попробуйте еще раз.")
                return
        
        if text == "🔍 Проверить игры":
            await self._run_check(update, context)
        elif text == "🏷️ Скидки":
            await self.start_discount_check(update, context)
        elif text == "⚙️ Настройки":
            await self.settings_menu(update, context)
        elif text == "📊 Статистика":
            await self.cmd_stats(update, context)
        elif text == "🔄 Обновить кэш":
            await update.message.reply_text("🔄 Обновление кэша библиотеки...")
            success = await self.steam._refresh_owned_games_cache()
            count = await self.steam.get_total_library_count()
            await update.message.reply_text(
                f"{'✅' if success else '⚠️'} Кэш обновлен! Всего игр: {count}",
                reply_markup=self._get_menu_keyboard()
            )
        elif text == "🏥 Статус":
            await self.cmd_health(update, context)
        else:
            await self._show_menu(update, context)
    
    async def cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if await self._restricted(update):
            return
        
        total_in_library = await self.steam.get_total_library_count()
        stats = await self.db.get_stats()
        min_discount = self.settings.get('min_discount_percent', 50)
        
        await update.message.reply_text(
            f"📊 <b>Расширенная статистика</b>\n\n"
            f"🔐 Steam: {'✅' if self.steam.logged_in else '❌'}\n"
            f"⚙️ Режим: <b>{self._get_mode_description()}</b>\n"
            f"🏷️ Мин. скидка: <b>{min_discount}%</b>\n"
            f"📄 Страниц поиска: {self.settings.get('max_pages')}\n"
            f"📄 Страниц скидок: {self.settings.get('max_discount_pages')}\n"
            f"💰 Порог цены: {self.settings.get('auto_add_price_threshold')}₽\n\n"
            f"📚 <b>Библиотека:</b>\n"
            f"• Всего игр в аккаунте: <b>{total_in_library}</b>\n\n"
            f"🎮 <b>Мониторинг:</b>\n"
            f"• Отслежено игр: {stats.get('total_tracked', 0)}\n"
            f"• Добавлено ботом: {stats.get('total_added', 0)}\n"
            f"• Найдено в библиотеке: {stats.get('in_library', 0)}\n"
            f"• Добавлено сегодня: {stats.get('today_added', 0)}",
            parse_mode='HTML'
        )
    
    async def cmd_health(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if await self._restricted(update):
            return
        
        health = await self.health.get_health_status()
        
        await update.message.reply_text(
            f"🏥 <b>Состояние системы</b>\n\n"
            f"✅ Статус: <b>{health['status'].upper()}</b>\n"
            f"⏱ Аптайм: {health['uptime']}\n"
            f"📊 Проверено игр: {health.get('games_checked', 0)}\n"
            f"✅ Добавлено игр: {health.get('games_added', 0)}\n"
            f"❌ Ошибок API: {health.get('api_errors', 0)}",
            parse_mode='HTML'
        )
    
    async def _scheduled_task(self):
        try:
            logger.info("📅 Плановая проверка...")
            if self.app:
                await self._run_check(update=None, context=None, is_scheduled=True)
        except Exception as e:
            logger.error(f"❌ Ошибка в scheduled task: {e}")
    
    async def _run_scheduler(self):
        while self._is_running:
            try:
                interval = self.settings.get('check_interval', BOT_CONFIG['interval'])
                await asyncio.sleep(interval * 60)
                if self._is_running:
                    await self._scheduled_task()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Ошибка планировщика: {e}")
                await asyncio.sleep(60)

# ============================================================================
# ТОЧКА ВХОДА
# ============================================================================
def main():
    """Главная функция"""
    bot = SteamBot()
    
    # Создаем приложение
    application = Application.builder().token(BOT_CONFIG['token']).build()
    
    # Создаем ConversationHandler для поиска скидок
    discount_conv_handler = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(bot.start_discount_check, pattern="^start_discount$"),
            MessageHandler(filters.Regex("^🏷️ Скидки$"), bot.start_discount_check)
        ],
        states={
            WAITING_DISCOUNT_PERCENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, bot.process_discount_percent),
                CallbackQueryHandler(bot.use_default_discount, pattern="^use_default_discount:"),
                CallbackQueryHandler(bot.cancel_discount, pattern="^cancel_discount$")
            ],
        },
        fallbacks=[CallbackQueryHandler(bot.cancel_discount, pattern="^cancel_discount$")],
    )
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", bot.cmd_start))
    application.add_handler(CommandHandler("settings", bot.settings_menu))
    application.add_handler(CommandHandler("stats", bot.cmd_stats))
    application.add_handler(CommandHandler("health", bot.cmd_health))
    application.add_handler(discount_conv_handler)
    application.add_handler(CallbackQueryHandler(bot.button_handler, pattern="^(add:|already_owned)"))
    application.add_handler(CallbackQueryHandler(
        bot.settings_callback,
        pattern="^(set_|mode:|settings_menu|main_menu|reset_history|refresh_cache)"
    ))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, bot.text_handler))
    
    bot.app = application
    
    # Запуск бота
    logger.info(f"🚀 Запуск Steam Free Games Bot для пользователя {BOT_CONFIG['admin_id']}")
    
    # Инициализация и планировщик через post_init
    async def post_init(app):
        await bot.initialize()
        bot._is_running = True
        asyncio.create_task(bot._run_scheduler())
    
    async def post_shutdown(app):
        await bot.shutdown()
    
    application.post_init = post_init
    application.post_shutdown = post_shutdown
    
    # Запуск
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    except KeyboardInterrupt:
        logger.info("👋 Программа остановлена пользователем")
    except Exception as e:
        logger.critical(f"💥 Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()