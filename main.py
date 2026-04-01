import logging
import os
import re
import asyncio
from datetime import datetime, date
from typing import Optional, Dict, Any, List
import aiosqlite
import colorlog
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes, ConversationHandler

# ==================== LOAD ENVIRONMENT VARIABLES ====================
load_dotenv()

# ==================== CONFIGURATION ====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID", "8201358407"))

# Required channels (3 channels)
REQUIRED_CHANNELS = [
    {"id": "@paymentwaterfestival", "name": "💸 Payment Channel", "link": "https://t.me/paymentwaterfestival"},
    {"id": "@thingyanmyanmarfr", "name": "🎉 Our Channel", "link": "https://t.me/thingyanmyanmarfr"},
    {"id": "@chukiechukieempire", "name": "👑 Chukie Empire", "link": "https://t.me/chukiechukieempire"}
]

PAYMENT_CHANNEL_ID = "@paymentwaterfestival"

STARTING_BALANCE = 0
REFERRAL_BONUS = 5000
WITHDRAWAL_THRESHOLD = 50000

DATABASE_PATH = "bot_database.db"

# Broadcast queue
BROADCAST_QUEUE = asyncio.Queue()
WORKER_COUNT = 3

# Conversation states
METHOD, PHONE, ACCOUNT_NAME, AMOUNT, CONFIRM = range(5)

# ==================== PREMIUM EMOJI IDs ====================
# All emojis from your list - using the IDs you provided

# ========== MENU BUTTON PREMIUM EMOJIS ==========
MENU_BUTTON_EMOJI = {
    "money": "5224257782013769471",           # 💰
    "people": "5256134032852278918",          # 👥
    "money_wings": "5334818215967076232",     # 💸
    "chart": "5231200819986047254",           # 📊
    "info": "5334544901428229844",            # ℹ️
    "rocket": "6206080111809140698",          # 🚀
    "home": "5416041192905265756",            # 🏠
    "back": "6190380760801743385",            # 🔙
    "phone": "5290017777174722330",           # 📱
    "check": "6300734173036417878",           # ✅
    "cross": "6255900302218628064",           # ❌
    "link": "6253758805755038090",            # 🔗
}

# ========== MESSAGE TEXT PREMIUM EMOJIS ==========
MESSAGE_EMOJI = {
    "cherry_blossom": "5463122435425448565",   # 🌸
    "dollar": "5334818215967076232",           # 💵
    "people": "5256134032852278918",           # 👥
    "water": "5210956306952758910",            # 💦
    "money_wings": "5373174941095050893",      # 💸
    "phone": "6127369664969314573",            # 📱
    "check": "6127307976354044793",            # ✅
    "cross": "5454350746407419714",            # ❌
    "back": "5253997076169115797",             # 🔙
    "home": "5416041192905265756",             # 🏠
    "chart": "5231200819986047254",            # 📊
    "info": "5334544901428229844",             # ℹ️
    "rocket": "6206080111809140698",           # 🚀
    "pray": "6190488555890938601",             # 🙏
    "link": "5271604874419647061",             # 🔗
    "firework": "5449816553727998023",         # 🎇
    "warning": "6265015769008969527",          # ⚠️
    "crown": "5433758796289685818",            # 👑
    "bank": "5228878926306101271",             # 🏦
    "new": "5361939671720926182",              # 🆕
    "one": "5382357040008021292",              # 1️⃣
    "two": "5192988444413938411",              # 2️⃣
    "three": "5794085850681710530",            # 3️⃣
    "party": "5451897194799980108",            # 🎉
    "money": "5224257782013769471",            # 💰
}

# ==================== LOGGING SETUP ====================
def setup_logging():
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        }
    ))
    logger = colorlog.getLogger()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

logger = setup_logging()

# ==================== DATABASE CLASS ====================
class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = None

    async def connect(self):
        self.connection = await aiosqlite.connect(self.db_path)
        await self.create_tables()
        logger.info("Database connected")

    async def create_tables(self):
        await self.connection.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                balance INTEGER DEFAULT 0,
                referral_code TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_active TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        await self.connection.execute('''
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (referrer_id) REFERENCES users(user_id),
                FOREIGN KEY (referred_id) REFERENCES users(user_id)
            )
        ''')

        await self.connection.execute('''
            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                amount INTEGER,
                method TEXT,
                phone TEXT,
                account_name TEXT,
                status TEXT DEFAULT 'completed',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        ''')

        await self.connection.execute('''
            CREATE TABLE IF NOT EXISTS daily_stats (
                date DATE PRIMARY KEY,
                new_users INTEGER DEFAULT 0,
                total_referrals INTEGER DEFAULT 0,
                total_withdrawals INTEGER DEFAULT 0,
                withdrawal_amount INTEGER DEFAULT 0
            )
        ''')

        await self.connection.commit()
        logger.info("Tables created successfully")

    async def get_user(self, user_id: int) -> Optional[Dict]:
        try:
            async with self.connection.execute(
                'SELECT user_id, username, balance, created_at FROM users WHERE user_id = ?',
                (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return {
                        'user_id': row[0],
                        'username': row[1],
                        'balance': row[2],
                        'created_at': row[3]
                    }
        except Exception as e:
            logger.error(f"Error getting user {user_id}: {e}")
        return None

    async def create_user(self, user_id: int, username: str) -> Dict:
        referral_code = f"ref_{user_id}"
        try:
            await self.connection.execute(
                'INSERT INTO users (user_id, username, balance, referral_code) VALUES (?, ?, ?, ?)',
                (user_id, username, STARTING_BALANCE, referral_code)
            )
            await self.connection.commit()

            today = date.today().isoformat()
            await self.connection.execute('''
                INSERT INTO daily_stats (date, new_users) 
                VALUES (?, 1) 
                ON CONFLICT(date) DO UPDATE SET new_users = new_users + 1
            ''', (today,))
            await self.connection.commit()
            logger.info(f"New user created: {user_id} - {username}")
        except Exception as e:
            logger.error(f"Error creating user {user_id}: {e}")

        return {'user_id': user_id, 'username': username, 'balance': STARTING_BALANCE}

    async def add_balance(self, user_id: int, amount: int) -> int:
        try:
            await self.connection.execute(
                'UPDATE users SET balance = balance + ?, last_active = CURRENT_TIMESTAMP WHERE user_id = ?',
                (amount, user_id)
            )
            await self.connection.commit()

            async with self.connection.execute(
                'SELECT balance FROM users WHERE user_id = ?',
                (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            logger.error(f"Error adding balance to {user_id}: {e}")
            return 0

    async def deduct_balance(self, user_id: int, amount: int) -> int:
        try:
            await self.connection.execute(
                'UPDATE users SET balance = balance - ?, last_active = CURRENT_TIMESTAMP WHERE user_id = ?',
                (amount, user_id)
            )
            await self.connection.commit()

            async with self.connection.execute(
                'SELECT balance FROM users WHERE user_id = ?',
                (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            logger.error(f"Error deducting balance from {user_id}: {e}")
            return 0

    async def add_referral(self, referrer_id: int, referred_id: int) -> bool:
        try:
            async with self.connection.execute(
                'SELECT id FROM referrals WHERE referred_id = ?',
                (referred_id,)
            ) as cursor:
                if await cursor.fetchone():
                    return False

            await self.connection.execute(
                'INSERT INTO referrals (referrer_id, referred_id) VALUES (?, ?)',
                (referrer_id, referred_id)
            )
            await self.add_balance(referrer_id, REFERRAL_BONUS)

            today = date.today().isoformat()
            await self.connection.execute('''
                INSERT INTO daily_stats (date, total_referrals) 
                VALUES (?, 1) 
                ON CONFLICT(date) DO UPDATE SET total_referrals = total_referrals + 1
            ''', (today,))
            await self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"Error adding referral: {e}")
            return False

    async def get_referrals_count(self, user_id: int) -> int:
        try:
            async with self.connection.execute(
                'SELECT COUNT(*) FROM referrals WHERE referrer_id = ?',
                (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            logger.error(f"Error getting referrals count: {e}")
            return 0

    async def add_withdrawal(self, user_id: int, username: str, amount: int, method: str, phone: str, account_name: str):
        try:
            await self.connection.execute(
                '''INSERT INTO withdrawals (user_id, username, amount, method, phone, account_name, status) 
                   VALUES (?, ?, ?, ?, ?, ?, 'completed')''',
                (user_id, username, amount, method, phone, account_name)
            )

            today = date.today().isoformat()
            await self.connection.execute('''
                INSERT INTO daily_stats (date, total_withdrawals, withdrawal_amount) 
                VALUES (?, 1, ?) 
                ON CONFLICT(date) DO UPDATE SET 
                    total_withdrawals = total_withdrawals + 1,
                    withdrawal_amount = withdrawal_amount + ?
            ''', (today, amount, amount))
            await self.connection.commit()
            logger.info(f"Withdrawal recorded: {username} - {amount}")
        except Exception as e:
            logger.error(f"Error adding withdrawal: {e}")

    async def get_withdrawals(self, limit: int = 20) -> List[Dict]:
        try:
            async with self.connection.execute(
                'SELECT username, amount, method, created_at FROM withdrawals ORDER BY created_at DESC LIMIT ?',
                (limit,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [{'name': row[0], 'amount': row[1], 'method': row[2], 'date': row[3]} for row in rows]
        except Exception as e:
            logger.error(f"Error getting withdrawals: {e}")
            return []

    async def get_all_users(self) -> List[int]:
        try:
            async with self.connection.execute('SELECT user_id FROM users') as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"Error getting all users: {e}")
            return []

    async def get_total_users(self) -> int:
        try:
            async with self.connection.execute('SELECT COUNT(*) FROM users') as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            logger.error(f"Error getting total users: {e}")
            return 0

    async def get_total_balance(self) -> int:
        try:
            async with self.connection.execute('SELECT SUM(balance) FROM users') as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            logger.error(f"Error getting total balance: {e}")
            return 0

    async def get_total_referrals(self) -> int:
        try:
            async with self.connection.execute('SELECT COUNT(*) FROM referrals') as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            logger.error(f"Error getting total referrals: {e}")
            return 0

    async def get_total_withdrawals_amount(self) -> int:
        try:
            async with self.connection.execute('SELECT SUM(amount) FROM withdrawals') as cursor:
                row = await cursor.fetchone()
                return row[0] if row else 0
        except Exception as e:
            logger.error(f"Error getting total withdrawals: {e}")
            return 0

    async def get_today_stats(self) -> Dict:
        today = date.today().isoformat()
        try:
            async with self.connection.execute(
                'SELECT new_users, total_referrals, total_withdrawals, withdrawal_amount FROM daily_stats WHERE date = ?',
                (today,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return {
                        'new_users': row[0] or 0,
                        'total_referrals': row[1] or 0,
                        'total_withdrawals': row[2] or 0,
                        'withdrawal_amount': row[3] or 0
                    }
        except Exception as e:
            logger.error(f"Error getting today stats: {e}")
        return {'new_users': 0, 'total_referrals': 0, 'total_withdrawals': 0, 'withdrawal_amount': 0}

    async def update_username(self, user_id: int, username: str):
        try:
            await self.connection.execute(
                'UPDATE users SET username = ?, last_active = CURRENT_TIMESTAMP WHERE user_id = ?',
                (username, user_id)
            )
            await self.connection.commit()
        except Exception as e:
            logger.error(f"Error updating username: {e}")

db = Database(DATABASE_PATH)

# ==================== BROADCAST WORKER ====================

async def broadcast_worker(app: Application):
    while True:
        try:
            job = await BROADCAST_QUEUE.get()
            user_id = job["user_id"]
            message = job["message"]
            original_msg = job.get("original_msg")

            try:
                if original_msg:
                    await app.bot.forward_message(
                        chat_id=user_id,
                        from_chat_id=original_msg.chat_id,
                        message_id=original_msg.message_id
                    )
                else:
                    await app.bot.send_message(
                        chat_id=user_id,
                        text=message,
                        parse_mode='HTML'
                    )
                logger.info(f"Broadcast sent to {user_id}")
            except Exception as e:
                logger.error(f"Failed to send broadcast to {user_id}: {e}")

            await asyncio.sleep(0.05)
            BROADCAST_QUEUE.task_done()
        except Exception as e:
            logger.error(f"Broadcast worker error: {e}")
            await asyncio.sleep(1)

async def start_broadcast_workers(app: Application):
    for i in range(WORKER_COUNT):
        asyncio.create_task(broadcast_worker(app))
        logger.info(f"Broadcast worker {i+1} started")

# ==================== MEMBERSHIP CHECK ====================

async def check_membership(user_id, context):
    """Check all 3 channels and return list of not joined channels"""
    not_joined = []

    for channel in REQUIRED_CHANNELS:
        try:
            chat = await context.bot.get_chat(chat_id=channel["id"])
            logger.info(f"Channel {channel['id']} found: {chat.title if chat else 'unknown'}")

            member = await context.bot.get_chat_member(chat_id=channel["id"], user_id=user_id)
            logger.info(f"Channel {channel['id']} status for user {user_id}: {member.status}")

            if member.status in ["left", "kicked"]:
                not_joined.append(channel)
                logger.info(f"User {user_id} NOT joined {channel['id']}")
            else:
                logger.info(f"User {user_id} joined {channel['id']}")

        except Exception as e:
            logger.error(f"Error checking channel {channel['id']}: {e}")
            not_joined.append(channel)
            logger.warning(f"Adding {channel['id']} to not_joined due to error")

    logger.info(f"Final not_joined channels for user {user_id}: {[ch['name'] for ch in not_joined]}")
    return not_joined

# ==================== BUTTON FUNCTIONS (Using MENU_BUTTON_EMOJI) ====================

def get_main_menu():
    """Main menu with colored buttons and premium emojis"""
    keyboard = [
        [
            InlineKeyboardButton(
                text="ငွေစရင်း",
                callback_data="balance",
                style="primary",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["money"]
            )
        ],
        [
            InlineKeyboardButton(
                text="ဖိတ်ခေါ်မယ်",
                callback_data="invite",
                style="success",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["people"]
            )
        ],
        [
            InlineKeyboardButton(
                text="ငွေထုတ်မယ်",
                callback_data="withdraw",
                style="success",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["money_wings"]
            )
        ],
        [
            InlineKeyboardButton(
                text="မှတ်တမ်း",
                callback_data="history",
                style="primary",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["chart"]
            )
        ],
        [
            InlineKeyboardButton(
                text="အကူအညီ",
                callback_data="help",
                style="danger",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["info"]
            )
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_invite_menu(referral_link):
    """Invite menu with colored buttons and premium emojis"""
    keyboard = [
        [
            InlineKeyboardButton(
                text="ဖိတ်ခေါ်မည်",
                url=f"https://t.me/share/url?url={referral_link}&text=🎉 သင်္ကြန်မုန့်ဖိုး {REFERRAL_BONUS} ကျပ် ရယူလိုက်ပါ။",
                style="success",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["rocket"]
            )
        ],
        [
            InlineKeyboardButton(
                text="ပင်မစာမျက်နှာ",
                callback_data="main_menu",
                style="primary",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["home"]
            )
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_back_button():
    """Back button with premium emoji"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                text="ပင်မစာမျက်နှာ",
                callback_data="main_menu",
                style="primary",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["home"]
            )
        ]
    ])

def get_method_keyboard():
    """Payment methods with colored buttons and premium emojis"""
    keyboard = [
        [
            InlineKeyboardButton(
                text="KPay",
                callback_data="method_KPay",
                style="success",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["money_wings"]
            )
        ],
        [
            InlineKeyboardButton(
                text="Wave Pay",
                callback_data="method_Wave Pay",
                style="success",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["money_wings"]
            )
        ],
        [
            InlineKeyboardButton(
                text="AYA Pay",
                callback_data="method_AYA Pay",
                style="success",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["money_wings"]
            )
        ],
        [
            InlineKeyboardButton(
                text="CB Pay",
                callback_data="method_CB Pay",
                style="success",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["money_wings"]
            )
        ],
        [
            InlineKeyboardButton(
                text="Phone Bill",
                callback_data="method_Phone Bill",
                style="primary",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["phone"]
            )
        ],
        [
            InlineKeyboardButton(
                text="ပြန်သွားမည်",
                callback_data="main_menu",
                style="danger",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["back"]
            )
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_withdraw_confirm_keyboard():
    """Withdrawal confirmation with colored buttons and premium emojis"""
    keyboard = [
        [
            InlineKeyboardButton(
                text="အတည်ပြုမည်",
                callback_data="confirm_withdraw",
                style="success",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["check"]
            ),
            InlineKeyboardButton(
                text="ပယ်ဖျက်မည်",
                callback_data="withdraw",
                style="danger",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["cross"]
            )
        ]
    ]
    return InlineKeyboardMarkup(keyboard)

def get_force_join_keyboard(not_joined_channels):
    """Force join keyboard with colored buttons and premium emojis"""
    keyboard = []
    for ch in not_joined_channels:
        keyboard.append([
            InlineKeyboardButton(
                text=f"{ch['name']} ကို Join ပါ",
                url=ch["link"],
                style="primary",
                icon_custom_emoji_id=MENU_BUTTON_EMOJI["link"]
            )
        ])
    keyboard.append([
        InlineKeyboardButton(
            text="Join ပြီးပါပြီ (Check)",
            callback_data="check_join",
            style="success",
            icon_custom_emoji_id=MENU_BUTTON_EMOJI["check"]
        )
    ])
    return InlineKeyboardMarkup(keyboard)

# ==================== MESSAGE FUNCTIONS (Using MESSAGE_EMOJI with HTML) ====================

async def send_payment_announcement(context, username, phone, account_name, method, amount):
    """Payment Channel မှာ Message ပို့မယ် (Premium Emojis with HTML)"""

    if len(username) >= 4:
        username_masked = username[:3] + "***"
    elif len(username) >= 2:
        username_masked = username[:1] + "***"
    else:
        username_masked = "***"

    if len(account_name) >= 3:
        account_name_masked = account_name[0] + "***" + account_name[-1]
    elif len(account_name) == 2:
        account_name_masked = account_name[0] + "*" + account_name[-1]
    else:
        account_name_masked = "***"

    phone_clean = re.sub(r'\D', '', phone)
    if len(phone_clean) >= 8:
        phone_masked = phone_clean[:3] + "*****" + phone_clean[-2:]
    elif len(phone_clean) >= 5:
        phone_masked = phone_clean[:2] + "***" + phone_clean[-2:]
    else:
        phone_masked = "*******"

    message = f"""<tg-emoji emoji-id='{MESSAGE_EMOJI["water"]}'>💦</tg-emoji> <b>အတာသင်္ကြန် ငွေထုတ်မှု အောင်မြင်ပါသည်</b> <tg-emoji emoji-id='{MESSAGE_EMOJI["water"]}'>💦</tg-emoji>
━━━━━━━━━━━━━━━━━━
<tg-emoji emoji-id='{MESSAGE_EMOJI["cherry_blossom"]}'>🌸</tg-emoji> မင်္ဂလာရှိသော အတာသင်္ကြန်အခါသမယမှာ အသုံးပြုသူများအားလုံး စိတ်၏ချမ်းသာခြင်း၊ ကိုယ်၏ကျန်းမာခြင်းများနှင့် ပြည့်စုံပြီး လိုအင်ဆန္ဒများ တစ်လုံးတစ်ဝတည်း ပြည့်ဝကြပါစေကြောင်း ဆုမွန်ကောင်းတောင်းအပ်ပါသည်။ <tg-emoji emoji-id='{MESSAGE_EMOJI["pray"]}'>🙏</tg-emoji>

<tg-emoji emoji-id='{MESSAGE_EMOJI["info"]}'>👤</tg-emoji> <b>အမည်:</b> <code>{username_masked}</code>
<tg-emoji emoji-id='{MESSAGE_EMOJI["bank"]}'>🏦</tg-emoji> <b>ထုတ်ယူနည်း:</b> <code>{method}</code>
<tg-emoji emoji-id='{MESSAGE_EMOJI["info"]}'>👤</tg-emoji> <b>အကောင့်အမည်:</b> <code>{account_name_masked}</code>
<tg-emoji emoji-id='{MESSAGE_EMOJI["phone"]}'>📱</tg-emoji> <b>အကောင့်နံပါတ်:</b> <code>{phone_masked}</code>
<tg-emoji emoji-id='{MESSAGE_EMOJI["money_wings"]}'>💸</tg-emoji> <b>ငွေပမာဏ:</b> <code>{amount} ကျပ်</code>
━━━━━━━━━━━━━━━━━━
<tg-emoji emoji-id='{MESSAGE_EMOJI["party"]}'>🎉</tg-emoji> <b>Thingyan Special Rewards</b> <tg-emoji emoji-id='{MESSAGE_EMOJI["party"]}'>🎉</tg-emoji>
<tg-emoji emoji-id='{MESSAGE_EMOJI["firework"]}'>🎇</tg-emoji> အားလုံးပဲ ပျော်ရွှင်စရာ အတာသင်္ကြန် ဖြစ်ပါစေဗျာ။ <tg-emoji emoji-id='{MESSAGE_EMOJI["firework"]}'>🎇</tg-emoji>"""

    try:
        await context.bot.send_message(chat_id=PAYMENT_CHANNEL_ID, text=message, parse_mode='HTML')
        logger.info(f"Payment announcement sent for {username}")
    except Exception as e:
        logger.error(f"Failed to send payment announcement: {e}")

async def show_main_menu(update, context, user_id):
    user = await db.get_user(user_id)
    if not user:
        return

    total_balance = user['balance']
    referrals_count = await db.get_referrals_count(user_id)

    text = f"""<tg-emoji emoji-id='{MESSAGE_EMOJI["cherry_blossom"]}'>🌸</tg-emoji> <b>သင့်၏ သင်္ကြန်မုန့်ဖိုး ငွေစာရင်း</b> <tg-emoji emoji-id='{MESSAGE_EMOJI["cherry_blossom"]}'>🌸</tg-emoji>

<tg-emoji emoji-id='{MESSAGE_EMOJI["dollar"]}'>💵</tg-emoji> <b>လက်ကျန်ငွေ:</b> <code>{total_balance} ကျပ်</code>
<tg-emoji emoji-id='{MESSAGE_EMOJI["people"]}'>👥</tg-emoji> <b>ဖိတ်ခေါ်ထားသူ:</b> <code>{referrals_count} ယောက်</code>

<tg-emoji emoji-id='{MESSAGE_EMOJI["water"]}'>💦</tg-emoji> <i>လူ ၁၀ ယောက်ဖိတ်ခေါ်ပြီး ၅၀,၀၀၀ ကျပ် ပြည့်ပါက ငွေထုတ်ယူနိုင်ပါသည်။</i> <tg-emoji emoji-id='{MESSAGE_EMOJI["water"]}'>💦</tg-emoji>"""

    if update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=get_main_menu(), parse_mode='HTML')
    else:
        await update.message.reply_text(text, reply_markup=get_main_menu(), parse_mode='HTML')

# ==================== START COMMAND ====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    username = update.effective_user.username or update.effective_user.first_name

    not_joined = await check_membership(user_id, context)

    if not_joined:
        channel_list = "\n".join([f"• {ch['name']}" for ch in not_joined])
        channel_count = len(not_joined)

        force_join_text = f"""<tg-emoji emoji-id='{MESSAGE_EMOJI["cherry_blossom"]}'>🌸</tg-emoji> ပျော်ရွှင်ဖွယ်ရာ မြန်မာ့ရိုးရာ သင်္ကြန်အခါသမယလေး ဖြစ်ပါစေ <tg-emoji emoji-id='{MESSAGE_EMOJI["water"]}'>💦</tg-emoji>

ဒီ Bot လေးကနေ သူငယ်ချင်းတွေကို ဖိတ်ခေါ်ပြီး သင်္ကြန်မုန့်ဖိုးတွေ ရယူနိုင်ပါတယ်။ <tg-emoji emoji-id='{MESSAGE_EMOJI["money_wings"]}'>💸</tg-emoji>

<tg-emoji emoji-id='{MESSAGE_EMOJI["warning"]}'>⚠️</tg-emoji> Bot ကိုစတင်သုံးနိုင်ရန် အောက်ပါ Channel ({channel_count}) ခုကို မဖြစ်မနေ Join ပေးပါ။ <tg-emoji emoji-id='{MESSAGE_EMOJI["crown"]}'>👑</tg-emoji>

{channel_list}"""

        await update.message.reply_text(force_join_text, reply_markup=get_force_join_keyboard(not_joined), parse_mode='HTML')
        return

    args = context.args
    user = await db.get_user(user_id)

    if args and args[0].startswith("ref_"):
        try:
            referrer_id = int(args[0].replace("ref_", ""))
            if referrer_id != user_id:
                referrer_exists = await db.get_user(referrer_id)
                if referrer_exists:
                    success = await db.add_referral(referrer_id, user_id)
                    if success:
                        try:
                            await context.bot.send_message(
                                chat_id=referrer_id,
                                text=f"🎉 ဝမ်းသွားပါတယ်။\n👤 {username} က သင့် referral link ကနေ ဝင်ရောက်လာပါတယ်။\n💰 {REFERRAL_BONUS} ကျပ် ထပ်တိုးသွားပါတယ်။"
                            )
                        except Exception as e:
                            logger.error(f"Failed to notify referrer: {e}")
        except Exception as e:
            logger.error(f"Error processing referral: {e}")

    if not user:
        await db.create_user(user_id, username)
        logger.info(f"New user: {user_id} - {username}")
    else:
        await db.update_username(user_id, username)

    await show_main_menu(update, context, user_id)

# ==================== CALLBACK HANDLERS ====================

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id

    # Force join check - EVERY BUTTON CLICK
    not_joined = await check_membership(user_id, context)

    if not_joined:
        channel_list = "\n".join([f"• {ch['name']}" for ch in not_joined])
        channel_count = len(not_joined)

        text = f"""<tg-emoji emoji-id='{MESSAGE_EMOJI["cherry_blossom"]}'>🌸</tg-emoji> ပျော်ရွှင်ဖွယ်ရာ မြန်မာ့ရိုးရာ သင်္ကြန်အခါသမယလေး ဖြစ်ပါစေ <tg-emoji emoji-id='{MESSAGE_EMOJI["water"]}'>💦</tg-emoji>

⚠️ Bot ကိုဆက်လက်သုံးရန် အောက်ပါ Channel ({channel_count}) ခုကို မဖြစ်မနေ Join ပေးပါ။ 👇

{channel_list}"""

        await query.edit_message_text(text, reply_markup=get_force_join_keyboard(not_joined), parse_mode='HTML')
        return

    if query.data == "check_join":
        not_joined = await check_membership(user_id, context)
        if not_joined:
            channel_list = "\n".join([f"• {ch['name']}" for ch in not_joined])
            await query.edit_message_text(
                f"⚠️ အောက်ပါ Channel ({len(not_joined)}) ခုကို ကျေးဇူးပြု၍ Join ပေးပါ။\n\n{channel_list}",
                reply_markup=get_force_join_keyboard(not_joined)
            )
        else:
            user = await db.get_user(user_id)
            if not user:
                username = query.from_user.username or query.from_user.first_name
                await db.create_user(user_id, username)
            await show_main_menu(update, context, user_id)
        return

    if query.data == "main_menu":
        await show_main_menu(update, context, user_id)
        return

    if query.data == "balance":
        user = await db.get_user(user_id)
        if not user:
            await query.edit_message_text("ကျေးဇူးပြု၍ /start ကိုပြန်နှိပ်ပါ။")
            return
        referrals_count = await db.get_referrals_count(user_id)

        text = f"""<tg-emoji emoji-id='{MESSAGE_EMOJI["cherry_blossom"]}'>🌸</tg-emoji> <b>သင့်၏ သင်္ကြန်မုန့်ဖိုး ငွေစာရင်း</b> <tg-emoji emoji-id='{MESSAGE_EMOJI["cherry_blossom"]}'>🌸</tg-emoji>

<tg-emoji emoji-id='{MESSAGE_EMOJI["dollar"]}'>💵</tg-emoji> <b>လက်ကျန်ငွေ:</b> <code>{user['balance']} ကျပ်</code>
<tg-emoji emoji-id='{MESSAGE_EMOJI["people"]}'>👥</tg-emoji> <b>ဖိတ်ခေါ်ထားသူ:</b> <code>{referrals_count} ယောက်</code>

<tg-emoji emoji-id='{MESSAGE_EMOJI["water"]}'>💦</tg-emoji> <i>လူ ၁၀ ယောက်ဖိတ်ခေါ်ပြီး ၅၀,၀၀၀ ကျပ် ပြည့်ပါက ငွေထုတ်ယူနိုင်ပါသည်။</i> <tg-emoji emoji-id='{MESSAGE_EMOJI["water"]}'>💦</tg-emoji>"""

        await query.edit_message_text(text, reply_markup=get_back_button(), parse_mode='HTML')
        return

    if query.data == "invite":
        user = await db.get_user(user_id)
        if not user:
            await query.edit_message_text("ကျေးဇူးပြု၍ /start ကိုပြန်နှိပ်ပါ။")
            return
        referral_link = f"https://t.me/{context.bot.username}?start=ref_{user_id}"

        text = f"""<tg-emoji emoji-id='{MESSAGE_EMOJI["water"]}'>💦</tg-emoji> <b>သင်္ကြန်မုန့်ဖိုး ရယူရန် သူငယ်ချင်းများကို ဖိတ်ခေါ်ပါ</b> <tg-emoji emoji-id='{MESSAGE_EMOJI["cherry_blossom"]}'>🌸</tg-emoji>

သင့်ရဲ့ ဖိတ်ခေါ်လင့် (Referral Link) မှတဆင့် သူငယ်ချင်းတစ်ယောက် Join တိုင်း သင်္ကြန်မုန့်ဖိုး <b>{REFERRAL_BONUS} ကျပ်</b> ရရှိမှာ ဖြစ်ပါတယ်။ <tg-emoji emoji-id='{MESSAGE_EMOJI["money_wings"]}'>💸</tg-emoji>

<tg-emoji emoji-id='{MESSAGE_EMOJI["link"]}'>🔗</tg-emoji> <b>သင့်ဖိတ်ခေါ်လင့်:</b>
<code>{referral_link}</code>

<tg-emoji emoji-id='{MESSAGE_EMOJI["rocket"]}'>🚀</tg-emoji> အောက်ပါ 'ဖိတ်ခေါ်မည်' Button ကိုနှိပ်၍ သူငယ်ချင်းများ၊ Group များထံသို့ တိုက်ရိုက် ပေးပို့ဖိတ်ခေါ်နိုင်ပါပြီ။ <tg-emoji emoji-id='{MESSAGE_EMOJI["party"]}'>🎉</tg-emoji>"""

        await query.edit_message_text(text, reply_markup=get_invite_menu(referral_link), parse_mode='HTML')
        return

    if query.data == "withdraw":
        user = await db.get_user(user_id)
        if not user:
            await query.edit_message_text("ကျေးဇူးပြု၍ /start ကိုပြန်နှိပ်ပါ။")
            return

        balance = user['balance']
        if balance < WITHDRAWAL_THRESHOLD:
            text = f"""<tg-emoji emoji-id='{MESSAGE_EMOJI["cross"]}'>❌</tg-emoji> <b>ငွေထုတ်ရန်မလုံလောက်ပါ။</b> <tg-emoji emoji-id='{MESSAGE_EMOJI["cross"]}'>❌</tg-emoji>

ငွေထုတ်ယူရန် အနည်းဆုံး <b>{WITHDRAWAL_THRESHOLD} ကျပ်</b> ရှိရပါမည်။ သူငယ်ချင်းများကို ဆက်လက်ဖိတ်ခေါ်ပါ။ <tg-emoji emoji-id='{MESSAGE_EMOJI["cherry_blossom"]}'>🌸</tg-emoji><tg-emoji emoji-id='{MESSAGE_EMOJI["water"]}'>💦</tg-emoji>

<tg-emoji emoji-id='{MESSAGE_EMOJI["money"]}'>💰</tg-emoji> <b>လက်ရှိငွေ:</b> <code>{balance} ကျပ်</code>"""

            await query.edit_message_text(text, reply_markup=get_back_button(), parse_mode='HTML')
            return

        context.user_data["withdraw_balance"] = balance
        await query.edit_message_text(
            "💸 ငွေထုတ်ယူခြင်း\n\n"
            "ကျေးဇူးပြု၍ ငွေထုတ်ယူမည့် နည်းလမ်းကို ရွေးချယ်ပါ။",
            reply_markup=get_method_keyboard()
        )
        return METHOD

    if query.data.startswith("method_"):
        method = query.data.replace("method_", "")
        context.user_data["withdraw_method"] = method
        await query.edit_message_text(
            f"✅ ရွေးချယ်ထားသော နည်းလမ်း: {method}\n\n"
            "📱 ကျေးဇူးပြု၍ သင့်ဖုန်းနံပါတ်ကို ရေးထည့်ပါ။\n"
            "(ဥပမာ: 09712345678)",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    text="ပြန်သွားမည်",
                    callback_data="withdraw",
                    style="danger",
                    icon_custom_emoji_id=MENU_BUTTON_EMOJI["back"]
                )
            ]])
        )
        return PHONE

    if query.data == "history":
        withdrawals = await db.get_withdrawals(10)
        if not withdrawals:
            await query.edit_message_text("📊 ငွေထုတ်မှတ်တမ်း မရှိသေးပါ။", reply_markup=get_back_button())
            return
        text = "💦 *Thingyan Lucky Withdrawals* 🌸\n━━━━━━━━━━━━━━━━━━\n📊 ယနေ့ အောင်မြင်စွာ ငွေထုတ်ယူပြီးသူများ\n\n"
        for w in withdrawals:
            text += f"👤 {w['name']}\n💵 Amount: *{w['amount']} Ks*\n🏦 Via: _{w['method']}_ ✅\n──────────────\n"
        text += "\n⚡ *Real-time payouts are ongoing...*\n👥 လူများစွာ နေ့စဉ် ထုတ်ယူနေကြပါပြီ!\n\n🎁 *Invite friends & claim your Thingyan money now!* 💦"
        await query.edit_message_text(text, reply_markup=get_back_button(), parse_mode='Markdown')
        return

    if query.data == "help":
        text = f"""<tg-emoji emoji-id='{MESSAGE_EMOJI["info"]}'>ℹ️</tg-emoji> <b>အကူအညီ (Help & Information)</b> <tg-emoji emoji-id='{MESSAGE_EMOJI["info"]}'>ℹ️</tg-emoji>

<tg-emoji emoji-id='{MESSAGE_EMOJI["one"]}'>1️⃣</tg-emoji> <b>Bot အသုံးပြုရန်</b> သတ်မှတ်ထားသော Channel များကို မဖြစ်မနေ Join ရပါမည်။ <tg-emoji emoji-id='{MESSAGE_EMOJI["warning"]}'>⚠️</tg-emoji>

<tg-emoji emoji-id='{MESSAGE_EMOJI["two"]}'>2️⃣</tg-emoji> <b>သူငယ်ချင်း ၁ ယောက်ကို ဖိတ်ခေါ်တိုင်း</b> <code>{REFERRAL_BONUS} ကျပ်</code> ရရှိပါမည်။ <tg-emoji emoji-id='{MESSAGE_EMOJI["people"]}'>👥</tg-emoji>

<tg-emoji emoji-id='{MESSAGE_EMOJI["three"]}'>3️⃣</tg-emoji> <b>အနည်းဆုံး <code>{WITHDRAWAL_THRESHOLD} ကျပ်</code></b> (လူ ၁၀ ယောက်) ပြည့်ပါက KPay, Wave Pay, AYA Pay, CB Pay, Phone Bill တို့ဖြင့် ထုတ်ယူနိုင်ပါသည်။ <tg-emoji emoji-id='{MESSAGE_EMOJI["money_wings"]}'>💸</tg-emoji>

<tg-emoji emoji-id='{MESSAGE_EMOJI["water"]}'>💦</tg-emoji> <i>ပျော်ရွှင်စရာ သင်္ကြန်လေး ဖြစ်ပါစေ!</i> <tg-emoji emoji-id='{MESSAGE_EMOJI["cherry_blossom"]}'>🌸</tg-emoji>"""

        await query.edit_message_text(text, reply_markup=get_back_button(), parse_mode='HTML')
        return

    return None

# ==================== CONVERSATION HANDLERS ====================

async def get_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    not_joined = await check_membership(user_id, context)
    if not_joined:
        channel_list = "\n".join([f"• {ch['name']}" for ch in not_joined])
        channel_count = len(not_joined)

        text = f"""<tg-emoji emoji-id='{MESSAGE_EMOJI["cherry_blossom"]}'>🌸</tg-emoji> ပျော်ရွှင်ဖွယ်ရာ မြန်မာ့ရိုးရာ သင်္ကြန်အခါသမယလေး ဖြစ်ပါစေ <tg-emoji emoji-id='{MESSAGE_EMOJI["water"]}'>💦</tg-emoji>

⚠️ Bot ကိုဆက်လက်သုံးရန် အောက်ပါ Channel ({channel_count}) ခုကို မဖြစ်မနေ Join ပေးပါ။ 👇

{channel_list}"""

        await update.message.reply_text(text, reply_markup=get_force_join_keyboard(not_joined), parse_mode='HTML')
        return PHONE

    phone = update.message.text.strip()

    if not re.match(r'^[0-9]{9,11}$', phone.replace("+", "").replace("-", "")):
        await update.message.reply_text(
            "❌ ဖုန်းနံပါတ် ပုံစံမှားနေပါသည်။\n"
            "ကျေးဇူးပြု၍ ပြန်လည်ရေးထည့်ပါ။ (ဥပမာ: 09712345678)"
        )
        return PHONE

    context.user_data["withdraw_phone"] = phone
    await update.message.reply_text(
        f"📱 ဖုန်းနံပါတ်: {phone}\n\n"
        "👤 ကျေးဇူးပြု၍ သင့်အကောင့်အမည် (Account Name) ကို ရေးထည့်ပါ။"
    )
    return ACCOUNT_NAME

async def get_account_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    not_joined = await check_membership(user_id, context)
    if not_joined:
        channel_list = "\n".join([f"• {ch['name']}" for ch in not_joined])
        channel_count = len(not_joined)

        text = f"""<tg-emoji emoji-id='{MESSAGE_EMOJI["cherry_blossom"]}'>🌸</tg-emoji> ပျော်ရွှင်ဖွယ်ရာ မြန်မာ့ရိုးရာ သင်္ကြန်အခါသမယလေး ဖြစ်ပါစေ <tg-emoji emoji-id='{MESSAGE_EMOJI["water"]}'>💦</tg-emoji>

⚠️ Bot ကိုဆက်လက်သုံးရန် အောက်ပါ Channel ({channel_count}) ခုကို မဖြစ်မနေ Join ပေးပါ။ 👇

{channel_list}"""

        await update.message.reply_text(text, reply_markup=get_force_join_keyboard(not_joined), parse_mode='HTML')
        return ACCOUNT_NAME

    account_name = update.message.text.strip()
    if len(account_name) < 2:
        await update.message.reply_text(
            "❌ အကောင့်အမည် အနည်းဆုံး ၂ လုံးထက်ပိုရပါမည်။\n"
            "ကျေးဇူးပြု၍ ပြန်လည်ရေးထည့်ပါ။"
        )
        return ACCOUNT_NAME

    context.user_data["withdraw_account_name"] = account_name

    balance = context.user_data.get("withdraw_balance", 0)
    await update.message.reply_text(
        f"👤 အကောင့်အမည်: {account_name}\n\n"
        f"💰 ကျေးဇူးပြု၍ ငွေထုတ်ယူမည့် ပမာဏကို ရေးထည့်ပါ။\n"
        f"(အနည်းဆုံး {WITHDRAWAL_THRESHOLD} ကျပ်၊ လက်ရှိငွေ: {balance} ကျပ်)"
    )
    return AMOUNT

async def get_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    not_joined = await check_membership(user_id, context)
    if not_joined:
        channel_list = "\n".join([f"• {ch['name']}" for ch in not_joined])
        channel_count = len(not_joined)

        text = f"""<tg-emoji emoji-id='{MESSAGE_EMOJI["cherry_blossom"]}'>🌸</tg-emoji> ပျော်ရွှင်ဖွယ်ရာ မြန်မာ့ရိုးရာ သင်္ကြန်အခါသမယလေး ဖြစ်ပါစေ <tg-emoji emoji-id='{MESSAGE_EMOJI["water"]}'>💦</tg-emoji>

⚠️ Bot ကိုဆက်လက်သုံးရန် အောက်ပါ Channel ({channel_count}) ခုကို မဖြစ်မနေ Join ပေးပါ။ 👇

{channel_list}"""

        await update.message.reply_text(text, reply_markup=get_force_join_keyboard(not_joined), parse_mode='HTML')
        return AMOUNT

    try:
        amount = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text(
            "❌ ကျေးဇူးပြု၍ ဂဏန်းဖြင့်သာ ရေးထည့်ပါ။\n"
            f"(ဥပမာ: {WITHDRAWAL_THRESHOLD})"
        )
        return AMOUNT

    user = await db.get_user(user_id)
    user_balance = user['balance'] if user else 0

    if amount < WITHDRAWAL_THRESHOLD:
        await update.message.reply_text(
            f"❌ ငွေထုတ်ယူရန် အနည်းဆုံး {WITHDRAWAL_THRESHOLD} ကျပ် ရှိရပါမည်။\n"
            f"သင်၏ လက်ကျန်ငွေ: {user_balance} ကျပ်\n\n"
            f"ကျေးဇူးပြု၍ ပြန်လည်ရေးထည့်ပါ။"
        )
        return AMOUNT

    if amount > user_balance:
        await update.message.reply_text(
            f"❌ သင့်တွင် လက်ကျန်ငွေ {user_balance} ကျပ်သာ ရှိပါသည်။\n"
            f"{amount} ကျပ် ထုတ်ယူ၍မရပါ။\n\n"
            f"ကျေးဇူးပြု၍ ပြန်လည်ရေးထည့်ပါ။"
        )
        return AMOUNT

    context.user_data["withdraw_amount_final"] = amount

    method = context.user_data.get("withdraw_method")
    phone = context.user_data.get("withdraw_phone")
    account_name = context.user_data.get("withdraw_account_name")

    text = f"""📋 *ငွေထုတ်ယူမှု အချက်အလက် အတည်ပြုချက်*
━━━━━━━━━━━━━━━━━━
🏦 နည်းလမ်း: {method}
📱 ဖုန်းနံပါတ်: {phone}
👤 အကောင့်အမည်: {account_name}
💰 ငွေပမာဏ: {amount} ကျပ်
━━━━━━━━━━━━━━━━━━

✅ အတည်ပြုပြီး ငွေထုတ်ယူရန် "✅ အတည်ပြုမည်" ကိုနှိပ်ပါ။
❌ ပြန်လည်ပြင်ဆင်ရန် "🔙 ပြန်သွားမည်" ကိုနှိပ်ပါ။"""

    await update.message.reply_text(text, reply_markup=get_withdraw_confirm_keyboard(), parse_mode='Markdown')
    return CONFIRM

async def confirm_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    username = query.from_user.username or query.from_user.first_name

    not_joined = await check_membership(user_id, context)
    if not_joined:
        channel_list = "\n".join([f"• {ch['name']}" for ch in not_joined])
        channel_count = len(not_joined)

        text = f"""<tg-emoji emoji-id='{MESSAGE_EMOJI["cherry_blossom"]}'>🌸</tg-emoji> ပျော်ရွှင်ဖွယ်ရာ မြန်မာ့ရိုးရာ သင်္ကြန်အခါသမယလေး ဖြစ်ပါစေ <tg-emoji emoji-id='{MESSAGE_EMOJI["water"]}'>💦</tg-emoji>

⚠️ Bot ကိုဆက်လက်သုံးရန် အောက်ပါ Channel ({channel_count}) ခုကို မဖြစ်မနေ Join ပေးပါ။ 👇

{channel_list}"""

        await query.edit_message_text(text, reply_markup=get_force_join_keyboard(not_joined), parse_mode='HTML')
        return ConversationHandler.END

    method = context.user_data.get("withdraw_method")
    phone = context.user_data.get("withdraw_phone")
    account_name = context.user_data.get("withdraw_account_name")
    amount = context.user_data.get("withdraw_amount_final")

    if not all([method, phone, account_name, amount]):
        await query.edit_message_text("❌ ငွေထုတ်ယူမှု အချက်အလက် မပြည့်စုံပါ။ ပြန်လည်စတင်ပါ။")
        return ConversationHandler.END

    user = await db.get_user(user_id)
    if not user:
        await query.edit_message_text("ကျေးဇူးပြု၍ /start ကိုပြန်နှိပ်ပါ။")
        return ConversationHandler.END

    if user['balance'] < amount:
        await query.edit_message_text("❌ လက်ကျန်ငွေ မလုံလောက်ပါ။ ပြန်လည်စတင်ပါ။")
        return ConversationHandler.END

    new_balance = await db.deduct_balance(user_id, amount)
    await db.add_withdrawal(user_id, username, amount, method, phone, account_name)

    await send_payment_announcement(context, username, phone, account_name, method, amount)

    success_text = f"""<tg-emoji emoji-id='{MESSAGE_EMOJI["check"]}'>✅</tg-emoji> <b>ငွေထုတ်ယူမှု အောင်မြင်ပါသည်။</b> <tg-emoji emoji-id='{MESSAGE_EMOJI["check"]}'>✅</tg-emoji>

<tg-emoji emoji-id='{MESSAGE_EMOJI["money_wings"]}'>💸</tg-emoji> <b>ထုတ်ယူငွေ:</b> <code>{amount} ကျပ်</code>
<tg-emoji emoji-id='{MESSAGE_EMOJI["bank"]}'>🏦</tg-emoji> <b>ထုတ်ယူနည်း:</b> <code>{method}</code>
<tg-emoji emoji-id='{MESSAGE_EMOJI["phone"]}'>📱</tg-emoji> <b>ဖုန်းနံပါတ်:</b> <code>{phone}</code>
<tg-emoji emoji-id='{MESSAGE_EMOJI["info"]}'>👤</tg-emoji> <b>အကောင့်အမည်:</b> <code>{account_name}</code>
━━━━━━━━━━━━━━━━━━
<tg-emoji emoji-id='{MESSAGE_EMOJI["money"]}'>💰</tg-emoji> <b>လက်ကျန်ငွေ:</b> <code>{new_balance} ကျပ်</code>

<tg-emoji emoji-id='{MESSAGE_EMOJI["pray"]}'>🙏</tg-emoji> ကျေးဇူးတင်ပါတယ်။ <tg-emoji emoji-id='{MESSAGE_EMOJI["water"]}'>💦</tg-emoji>"""

    await query.edit_message_text(success_text, reply_markup=get_back_button(), parse_mode='HTML')

    context.user_data.clear()
    return ConversationHandler.END

async def cancel_withdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ ငွေထုတ်ယူခြင်းကို ဖျက်သိမ်းလိုက်ပါသည်။", reply_markup=get_back_button())
    context.user_data.clear()
    return ConversationHandler.END

# ==================== OWNER COMMANDS ====================

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("⛔ ဒီ command ကို Owner သာ သုံးခွင့်ရှိပါတယ်။")
        return

    user_ids = await db.get_all_users()
    if not user_ids:
        await update.message.reply_text("❌ Broadcast လုပ်ရန် user မရှိပါ။")
        return

    if update.message.reply_to_message:
        original_msg = update.message.reply_to_message
        await update.message.reply_text(f"📤 Broadcasting to {len(user_ids)} users...")

        success_count = 0
        for user_id in user_ids:
            try:
                await BROADCAST_QUEUE.put({
                    "user_id": user_id,
                    "original_msg": original_msg,
                    "message": None
                })
                success_count += 1
            except Exception as e:
                logger.error(f"Queue error: {e}")
            await asyncio.sleep(0.01)

        await update.message.reply_text(f"✅ Broadcast queued! Queued: {success_count} users")
        return

    if not context.args:
        await update.message.reply_text("❌ Usage:\n1️⃣ /broadcast <message>\n2️⃣ Reply to a message with /broadcast")
        return

    message = " ".join(context.args)
    await update.message.reply_text(f"📤 Broadcasting to {len(user_ids)} users...")

    water_emoji_id = MESSAGE_EMOJI["water"]

    success_count = 0
    for user_id in user_ids:
        try:
            await BROADCAST_QUEUE.put({
                "user_id": user_id,
                "original_msg": None,
                "message": f"📢 <b>Broadcast Message</b>\n━━━━━━━━━━━━━━━━━━\n{message}\n━━━━━━━━━━━━━━━━━━\n<tg-emoji emoji-id='{water_emoji_id}'>💦</tg-emoji> သင်္ကြန်မုန့်ဖိုး Bot မှ ပေးပို့ပါတယ်။"
            })
            success_count += 1
        except Exception as e:
            logger.error(f"Queue error: {e}")
        await asyncio.sleep(0.01)

    await update.message.reply_text(f"✅ Broadcast queued! Queued: {success_count} users")

async def baladd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("⛔ ဒီ command ကို Owner သာ သုံးခွင့်ရှိပါတယ်။")
        return

    if len(context.args) != 2:
        await update.message.reply_text("❌ Usage: /baladd <user_id> <amount>")
        return

    try:
        target_user_id = int(context.args[0])
        amount = int(context.args[1])

        user = await db.get_user(target_user_id)
        if not user:
            await update.message.reply_text(f"❌ User ID {target_user_id} ကို ရှာမတွေ့ပါ။")
            return

        new_balance = await db.add_balance(target_user_id, amount)

        try:
            await context.bot.send_message(
                chat_id=target_user_id,
                text=f"🎉 သင်္ကြန်မုန့်ဖိုး {amount} ကျပ် ထပ်တိုးသွားပါတယ်။\n💰 လက်ရှိလက်ကျန်: {new_balance} ကျပ်"
            )
        except Exception as e:
            logger.error(f"Failed to notify user {target_user_id}: {e}")

        await update.message.reply_text(
            f"✅ User {target_user_id} ကို {amount} ကျပ် ထပ်တိုးပြီးပါပြီ။\n"
            f"💰 လက်ရှိလက်ကျန်: {new_balance} ကျပ်"
        )
    except ValueError:
        await update.message.reply_text("❌ Amount ကို ဂဏန်းဖြင့် ထည့်ပါ။")
    except Exception as e:
        logger.error(f"Error in baladd: {e}")
        await update.message.reply_text("❌ အမှားတစ်ခုဖြစ်သွားပါသည်။")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("⛔ ဒီ command ကို Owner သာ သုံးခွင့်ရှိပါတယ်။")
        return

    try:
        total_users = await db.get_total_users()
        total_referrals = await db.get_total_referrals()
        total_balance = await db.get_total_balance()
        total_withdrawals = await db.get_total_withdrawals_amount()
        today_stats = await db.get_today_stats()

        text = f"""📊 <b>Bot Statistics</b>
━━━━━━━━━━━━━━━━━━
👥 စုစုပေါင်းအသုံးပြုသူ: {total_users}
🔗 စုစုပေါင်း Referral များ: {total_referrals}
💰 စုစုပေါင်း Balance: {total_balance} ကျပ်
💸 စုစုပေါင်းထုတ်ယူမှု: {total_withdrawals} ကျပ်
━━━━━━━━━━━━━━━━━━
📊 <b>ယနေ့စာရင်း</b>
🆕 User အသစ်: {today_stats['new_users']}
👥 Referral အသစ်: {today_stats['total_referrals']}
💸 ငွေထုတ်ယူမှု: {today_stats['total_withdrawals']} ကြိမ်
💰 ထုတ်ယူငွေ: {today_stats['withdrawal_amount']} ကျပ်
━━━━━━━━━━━━━━━━━━
🎁 Referral Bonus: {REFERRAL_BONUS} ကျပ်
💎 ငွေထုတ်နိုင်ဖို့ အနည်းဆုံး: {WITHDRAWAL_THRESHOLD} ကျပ်"""

        await update.message.reply_text(text, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Error in stats: {e}")
        await update.message.reply_text("❌ စာရင်းများ ရယူရာတွင် အမှားဖြစ်သွားပါသည်။")

async def users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        await update.message.reply_text("⛔ ဒီ command ကို Owner သာ သုံးခွင့်ရှိပါတယ်။")
        return

    try:
        user_ids = await db.get_all_users()
        if not user_ids:
            await update.message.reply_text("No users yet.")
            return

        text = "📋 <b>User List (First 50)</b>\n━━━━━━━━━━━━━━━━━━\n"
        for i, user_id in enumerate(user_ids[:50]):
            user = await db.get_user(user_id)
            if user:
                ref_count = await db.get_referrals_count(user_id)
                text += f"🆔 <code>{user_id}</code> | {user.get('username', 'No name')}\n"
                text += f"   💰 {user['balance']} Ks | 👥 {ref_count} refs\n"

        if len(user_ids) > 50:
            text += f"\n... and {len(user_ids) - 50} more users"

        await update.message.reply_text(text, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Error in users_list: {e}")
        await update.message.reply_text("❌ User စာရင်း ရယူရာတွင် အမှားဖြစ်သွားပါသည်။")

# ==================== MAIN ====================

async def main():
    await db.connect()
    logger.info("Database connected")

    app = Application.builder().token(BOT_TOKEN).build()

    await start_broadcast_workers(app)

    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("baladd", baladd))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("users", users_list))

    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(button_handler, pattern="^withdraw$")],
        states={
            METHOD: [CallbackQueryHandler(button_handler, pattern="^method_")],
            PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_phone)],
            ACCOUNT_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_account_name)],
            AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_amount)],
            CONFIRM: [CallbackQueryHandler(confirm_withdraw, pattern="^confirm_withdraw$")],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_withdraw),
            CallbackQueryHandler(button_handler, pattern="^main_menu$"),
            CallbackQueryHandler(button_handler, pattern="^withdraw$"),
        ],
        allow_reentry=True
    )
    app.add_handler(conv_handler)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))

    logger.info(f"Bot is running...")
    logger.info(f"Owner ID: {OWNER_ID}")
    logger.info(f"Referral Bonus: {REFERRAL_BONUS} Ks")
    logger.info(f"Withdrawal Threshold: {WITHDRAWAL_THRESHOLD} Ks")
    logger.info(f"Required Channels: {len(REQUIRED_CHANNELS)} channels")
    for ch in REQUIRED_CHANNELS:
        logger.info(f"  - {ch['name']}: {ch['id']}")

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
