import os
import datetime
import logging
import sqlite3
import random
from typing import Optional

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.bot import DefaultBotProperties

from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup

from dotenv import load_dotenv
from aiohttp import web
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from flask import Flask, request, Response

# ================== Глобальные настройки ==================
TARGET_YEARS_DISPLAY = 90
WEEKS_IN_YEAR = 52

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
ADMIN_ID = os.getenv("ADMIN_ID")

if not TOKEN:
    logger.critical("BOT_TOKEN не задан. Проверь переменные окружения!")
    exit(1)
if not WEBHOOK_URL:
    logger.critical("WEBHOOK_URL не задан. Проверь переменные окружения!")
    exit(1)

if not ADMIN_ID:
    logger.warning("ADMIN_ID не задан. Администратор не будет уведомлен.")
else:
    try:
        ADMIN_ID = int(ADMIN_ID)
    except ValueError:
        logger.critical("ADMIN_ID должен быть числом!")
        exit(1)

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())
scheduler = AsyncIOScheduler()


# ================== База данных (SQLite) ==================
def create_database():
    """Создаёт таблицы users и messages, если их нет."""
    try:
        with sqlite3.connect('users.db') as conn:
            cursor = conn.cursor()
            # Таблица пользователей
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    birth_date TEXT
                )
            ''')
            # Таблица сообщений
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    text TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.commit()
    except Exception as e:
        logger.error(f"Ошибка при создании базы данных: {e}")


def save_user(user_id: int, birth_date: str):
    """Сохраняет (или обновляет) запись о пользователе (user_id, birth_date)."""
    try:
        with sqlite3.connect('users.db') as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT OR REPLACE INTO users (user_id, birth_date)
                VALUES (?, ?)
            ''', (user_id, birth_date))
            conn.commit()
    except Exception as e:
        logger.error(f"Ошибка при сохранении пользователя {user_id}: {e}")


def get_user(user_id: int) -> Optional[str]:
    """Возвращает birth_date (str) или None, если юзер не найден."""
    try:
        with sqlite3.connect('users.db') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT birth_date FROM users WHERE user_id = ?",
                           (user_id, ))
            row = cursor.fetchone()
            return row[0] if row else None
    except Exception as e:
        logger.error(f"Ошибка при получении пользователя {user_id}: {e}")
        return None


def get_all_users():
    """Возвращает список (user_id, birth_date) для всех записей."""
    try:
        with sqlite3.connect('users.db') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id, birth_date FROM users")
            return cursor.fetchall()
    except Exception as e:
        logger.error(f"Ошибка при получении всех пользователей: {e}")
        return []


def log_user_message(user_id: int, text: str):
    """Записывает входящее сообщение пользователя в таблицу messages."""
    try:
        with sqlite3.connect("users.db") as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO messages (user_id, text) VALUES (?, ?)",
                (user_id, text))
            conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Ошибка при записи сообщения: {e}")


# ================== Состояния FSM ==================
class UserState(StatesGroup):
    WAITING_BIRTH_DATE = State()
    WAITING_NEW_BIRTH_DATE = State()


# ================== Текстовые сообщения ==================
class BotTexts:
    DATE_FORMAT_ERROR = "🌌 Формат должен быть ДД.ММ.ГГГГ (например: 15.05.1990)."
    INTERNAL_ERROR = "🌀 Произошла неожиданная ошибка. Попробуй позже."
    DATE_SAVED = "✅ Дата рождения сохранена!"

    BUTTONS = {
        'years': "📅 Хронология бытия",
        'hours': "⏳ Песочные миры",
        'progress': "📜 Прогресс жизни",
        'change_date': "✒️ Переписать судьбу",
        'motivation': "🔮 Мудрость эпох"
    }

    WELCOME_NEW = (
        "🌠 <b>Добро пожаловать в Хроносферу!</b>\n\n"
        "Здесь миги становятся вехами, а время обретает смысл.\n\n"
        "Введи дату рождения (ДД.ММ.ГГГГ), "
        "чтобы узнать, сколько недель прожито и сколько ещё может оставаться..."
    )

    INVALID_DATE_RESPONSES = [
        "Ой, формат даты неверный! Попробуй ещё раз.",
        "Ошибка ввода. Формат ДД.ММ.ГГГГ.",
        "Неверный формат даты! Я лишь машина, а ты тлен, ха-ха.",
        "Проверка формата (ДД.ММ.ГГГГ) провалилась.",
        "Введённая дата сбилась с пути. Попробуй снова.",
        "Дата не та, как должна быть. Проверь формат (ДД.ММ.ГГГГ).",
        "Формат даты некорректен. Не пытайся обмануть время!",
        "Время и даты — дело тонкое. Формат: ДД.ММ.ГГГГ.",
        "Ошибка: дата. Исправь (ДД.ММ.ГГГГ).",
        "Неверный ввод даты. Даже алгоритмы не любят хаос.",
        "Дата не в формате. Давай ещё раз (ДД.ММ.ГГГГ).",
        "С датой что-то не так. Должно быть ДД.ММ.ГГГГ."
    ]

    RANDOM_ENDING_PHRASES = [
        "Каждый миг — шанс стать лучше!",
        "Время не ждёт, но ты можешь им управлять.",
        "Мѳновения ценны — лови их.",
        "Не позволяй песку времени рассыпаться зря!",
        "Даже малейшие перемены меняют судьбу!",
        "Сегодняшние усилия — завтрашние достижения!",
        "Будь хозяином своего времени!", "Пусть звёзды освещают твой путь!",
        "Не упускай мгновений — из них ткётся вечность!",
        "Час за часом мы формируем будущее!"
    ]

    WELCOME_RETURN = (
        "🌀 <b>{name}, твое путешествие продолжается!</b>\n"
        "📖 Начало пути: <b>{date}</b>\n"
        "🕯 Прогресс: <b>{progress_bar}</b>\n"
        "⌛ Прожито недель: <b>{weeksLived}</b> из <b>{targetWeeks}</b>\n\n"
        "{random_line}")

    WEEKLY_REPORT = (
        "🌌 Недельный отчет | Цикл <b>{week_number}</b>\n"
        "▰ Прожито недель: <b>{weeksLived}</b> из <b>{targetWeeks}</b> (<b>{progress:.1%}</b>)\n"
        "🌖 Фаза Луны: {moon_phase}\n"
        "{random_line}")


# ================== Утилиты ==================
def validate_date(date_str: str) -> Optional[datetime.date]:
    try:
        return datetime.datetime.strptime(date_str, "%d.%m.%Y").date()
    except ValueError:
        return None


def create_progress_bar(percentage: float, length: int = 10) -> str:
    filled = '▰' * int(percentage * length)
    empty = '▱' * (length - len(filled))
    return f"{filled}{empty} {percentage:.1%}"


def get_moon_phase() -> str:
    phases = [
        "🌑 Новолуние - Начало пути", "🌒 Молодая луна - Время действий",
        "🌓 Первая четверть - Испытание воли", "🌔 Прибывающая - Сбор плодов",
        "🌕 Полнолуние - Пик возможностей", "🌖 Убывающая - Анализ итогов",
        "🌗 Последняя четверть - Отпускание", "🌘 Старая луна - Подготовка"
    ]
    return phases[datetime.datetime.now().day % 8]


# ================== Плановая задача ==================
async def send_weekly_update(user_id: int, birth_date: str):
    """Отправляет недельный отчёт c фазой Луны."""
    try:
        birth_date_obj = datetime.datetime.strptime(birth_date,
                                                    "%d.%m.%Y").date()
        weeksLived = (datetime.date.today() - birth_date_obj).days // 7
        targetWeeks = TARGET_YEARS_DISPLAY * WEEKS_IN_YEAR
        progress = weeksLived / targetWeeks
        week_number = datetime.date.today().isocalendar()[1]

        random_line = random.choice(BotTexts.RANDOM_ENDING_PHRASES)
        await bot.send_message(
            user_id,
            BotTexts.WEEKLY_REPORT.format(week_number=week_number,
                                          weeksLived=weeksLived,
                                          targetWeeks=targetWeeks,
                                          progress=progress,
                                          moon_phase=get_moon_phase(),
                                          random_line=random_line))
    except Exception as e:
        logger.error(f"Ошибка отправки отчета пользователю {user_id}: {e}")


async def weekly_updates_task():
    """Запускается каждую неделю (понедельник 9:00) и шлёт отчёт всем."""
    try:
        users = get_all_users()
        for user_id, birth_date in users:
            await send_weekly_update(user_id, birth_date)
    except Exception as e:
        logger.error(f"Ошибка задачи обновлений: {e}")


# ================== Клавиатура ==================
main_keyboard = ReplyKeyboardMarkup(
    keyboard=[[
        KeyboardButton(text=BotTexts.BUTTONS['years']),
        KeyboardButton(text=BotTexts.BUTTONS['hours'])
    ],
              [
                  KeyboardButton(text=BotTexts.BUTTONS['progress']),
                  KeyboardButton(text=BotTexts.BUTTONS['motivation'])
              ], [KeyboardButton(text=BotTexts.BUTTONS['change_date'])]],
    resize_keyboard=True,
    input_field_placeholder="Выбери путь познания...")


# ================== Обработчики ==================
@dp.message(Command("start", "help"))
async def start_handler(message: types.Message, state: FSMContext):
    """Обработка /start и /help."""
    try:
        user_id = message.from_user.id
        user_name = message.from_user.first_name
        birth_date = get_user(user_id)

        if birth_date:
            b_date = validate_date(birth_date)
            if not b_date:
                await message.answer(BotTexts.DATE_FORMAT_ERROR)
                await state.set_state(UserState.WAITING_BIRTH_DATE)
                return

            weeksLived = (datetime.date.today() - b_date).days // 7
            targetWeeks = TARGET_YEARS_DISPLAY * WEEKS_IN_YEAR
            progress = weeksLived / targetWeeks
            random_line = random.choice(BotTexts.RANDOM_ENDING_PHRASES)

            await message.answer(BotTexts.WELCOME_RETURN.format(
                name=user_name,
                date=birth_date,
                progress_bar=create_progress_bar(progress),
                weeksLived=weeksLived,
                targetWeeks=targetWeeks,
                random_line=random_line),
                                 reply_markup=main_keyboard)
        else:
            await message.answer(
                f"{BotTexts.WELCOME_NEW}\n\n<i>(Приятно познакомиться, {user_name}!)</i>",
                reply_markup=types.ReplyKeyboardRemove())
            await state.set_state(UserState.WAITING_BIRTH_DATE)
    except Exception as e:
        logger.error(f"Start error: {e}")
        await message.answer(BotTexts.INTERNAL_ERROR)


@dp.message(UserState.WAITING_BIRTH_DATE)
async def process_birth_date(message: types.Message, state: FSMContext):
    """Обработка первого ввода даты рождения."""
    try:
        user_input = message.text.strip()
        birth_date = validate_date(user_input)
        user_name = message.from_user.first_name

        if not birth_date:
            response = random.choice(BotTexts.INVALID_DATE_RESPONSES)
            await message.answer(response)
            return

        save_user(message.from_user.id, user_input)
        await message.answer(
            f"✅ Дата рождения успешно сохранена, {user_name}!\n\n"
            "Теперь ты можешь:\n"
            "• Узнать, сколько недель и часов уже прожито\n"
            "• Посмотреть прогресс жизни (с фазой Луны!)\n"
            "• Получить мотивационную фразу\n\n"
            "Что выберешь?",
            reply_markup=main_keyboard)
        await state.clear()

    except Exception as e:
        logger.error(f"Ошибка обработки даты рождения: {e}")
        await message.answer(BotTexts.INTERNAL_ERROR)


@dp.message(lambda msg: msg.text == BotTexts.BUTTONS['years'])
async def handle_years(message: types.Message):
    """Сколько недель прожито."""
    try:
        user_id = message.from_user.id
        user_name = message.from_user.first_name
        birth_date_str = get_user(user_id)
        if not birth_date_str:
            await message.answer(
                f"ℹ️ {user_name}, укажи дату рождения через /start")
            return

        birth_date = validate_date(birth_date_str)
        if not birth_date:
            await message.answer(BotTexts.DATE_FORMAT_ERROR)
            return

        weeksLived = (datetime.date.today() - birth_date).days // 7
        targetWeeks = TARGET_YEARS_DISPLAY * WEEKS_IN_YEAR
        weeksRemaining = max(0, targetWeeks - weeksLived)
        random_line = random.choice(BotTexts.RANDOM_ENDING_PHRASES)

        await message.answer(
            f"🕰 <b>{user_name}</b>, прожито недель: <b>{weeksLived}</b>\n"
            f"⌛ Осталось: <b>{weeksRemaining}</b> (из <b>{targetWeeks}</b>)\n\n"
            f"{random_line}",
            reply_markup=main_keyboard)
    except Exception as e:
        logger.error(f"Ошибка в обработке недель: {e}")
        await message.answer(BotTexts.INTERNAL_ERROR)


@dp.message(lambda msg: msg.text == BotTexts.BUTTONS['hours'])
async def handle_hours(message: types.Message):
    """Сколько часов прожито."""
    try:
        user_id = message.from_user.id
        user_name = message.from_user.first_name
        birth_date_str = get_user(user_id)
        if not birth_date_str:
            await message.answer(
                f"ℹ️ {user_name}, укажи дату рождения через /start")
            return

        birth_date = validate_date(birth_date_str)
        if not birth_date:
            await message.answer(BotTexts.DATE_FORMAT_ERROR)
            return

        lived_days = (datetime.date.today() - birth_date).days
        lived_hours = lived_days * 24
        random_line = random.choice(BotTexts.RANDOM_ENDING_PHRASES)

        await message.answer(
            f"⏳ <b>{user_name}</b>, прожито часов: <b>{lived_hours:,}</b>\n"
            f"⏱ Это примерно <b>{lived_hours // 8760}</b> лет и <b>{(lived_hours % 8760)//24}</b> дней\n\n"
            f"{random_line}",
            reply_markup=main_keyboard)
    except Exception as e:
        logger.error(f"Ошибка в обработке часов: {e}")
        await message.answer(BotTexts.INTERNAL_ERROR)


@dp.message(lambda msg: msg.text == BotTexts.BUTTONS['progress'])
async def handle_progress(message: types.Message):
    """Прогресс жизни + фаза Луны."""
    try:
        user_id = message.from_user.id
        user_name = message.from_user.first_name
        birth_date_str = get_user(user_id)
        if not birth_date_str:
            await message.answer(
                f"ℹ️ {user_name}, укажи дату рождения через /start")
            return

        birth_date = validate_date(birth_date_str)
        if not birth_date:
            await message.answer(BotTexts.DATE_FORMAT_ERROR)
            return

        weeksLived = (datetime.date.today() - birth_date).days // 7
        targetWeeks = TARGET_YEARS_DISPLAY * WEEKS_IN_YEAR
        progress = weeksLived / targetWeeks
        random_line = random.choice(BotTexts.RANDOM_ENDING_PHRASES)
        bar = create_progress_bar(progress)
        moon_phase = get_moon_phase()  # фаза Луны

        await message.answer(
            f"📜 <b>{user_name}</b>, вот твой прогресс жизни:\n"
            f"{bar}\n"
            f"Фаза Луны: {moon_phase}\n"
            f"⌛ Осталось недель: <b>{max(0, targetWeeks - weeksLived)}</b>\n\n"
            f"{random_line}",
            reply_markup=main_keyboard)
    except Exception as e:
        logger.error(f"Ошибка в обработке прогресса: {e}")
        await message.answer(BotTexts.INTERNAL_ERROR)


@dp.message(lambda msg: msg.text == BotTexts.BUTTONS['motivation'])
async def handle_motivation(message: types.Message):
    """Мотивация."""
    try:
        user_name = message.from_user.first_name
        motivations = [
            "🌱 Каждый день — новый шанс изменить свою историю",
            "⏳ Время не ждет, но ты можешь им управлять",
            "💡 Сегодняшние усилия — завтрашние достижения",
            "🚀 Маленькие шаги приводят к большим целям",
            "🌌 Твоя жизнь — самый ценный проект",
            "🔑 Ключ к будущему — в настоящих поступках",
            "🔥 Не бойся идти вперёд, даже если шаги малы",
            "✨ Великие дела начинаются с маленькой идеи",
            "🌿 Расти, как растёт дерево: медленно, но уверенно",
            "💎 Каждый прожитый день — бесценный опыт"
        ]
        random_motivation = random.choice(motivations)
        random_line = random.choice(BotTexts.RANDOM_ENDING_PHRASES)

        await message.answer(
            f"💬 <b>{user_name}</b>, держи мудрость:\n\n"
            f"{random_motivation}\n\n{random_line}",
            reply_markup=main_keyboard)
    except Exception as e:
        logger.error(f"Ошибка в мотивации: {e}")
        await message.answer(BotTexts.INTERNAL_ERROR)


@dp.message(lambda msg: msg.text == BotTexts.BUTTONS['change_date'])
async def handle_change_date(message: types.Message, state: FSMContext):
    """Смена даты рождения."""
    try:
        user_name = message.from_user.first_name
        await message.answer(
            f"📅 {user_name}, введи новую дату рождения (ДД.ММ.ГГГГ):",
            reply_markup=types.ReplyKeyboardRemove())
        await state.set_state(UserState.WAITING_NEW_BIRTH_DATE)
    except Exception as e:
        logger.error(f"Ошибка смены даты: {e}")
        await message.answer(BotTexts.INTERNAL_ERROR)


@dp.message(UserState.WAITING_NEW_BIRTH_DATE)
async def process_new_birth_date(message: types.Message, state: FSMContext):
    """Обработка новой даты рождения (при смене)."""
    try:
        user_input = message.text.strip()
        birth_date = validate_date(user_input)
        user_name = message.from_user.first_name

        if not birth_date:
            response = random.choice(BotTexts.INVALID_DATE_RESPONSES)
            await message.answer(response)
            return

        save_user(message.from_user.id, user_input)
        await message.answer(f"✅ Дата успешно обновлена, {user_name}!",
                             reply_markup=main_keyboard)
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка обновления даты: {e}")
        await message.answer(BotTexts.INTERNAL_ERROR)


# === 1. Фиксируем ВСЕ входящие сообщения и отвечаем fallback, если не обработано выше
@dp.message()
async def fallback_handler(message: types.Message):
    """Логируем и отвечаем, если сообщение не подходит под остальные хендлеры."""
    user_id = message.from_user.id
    user_name = message.from_user.first_name

    # Логируем сообщение
    log_user_message(user_id, message.text)

    # Пример саркастических фраз:
    fallback_phrases = [
        f"Ты тратишь время, {user_name}, ведь я всего лишь бот — а жизнь твоя конечна.",
        f"{user_name}, неужели писать боту — лучшее, что ты можешь сделать?",
        f"Каждая секунда уходит безвозвратно, {user_name}, а ты здесь со мной...",
        f"Время идёт, {user_name}, и пока мы тут болтаем, никто не становится моложе."
    ]
    phrase = random.choice(fallback_phrases)

    await message.answer(phrase)


# ================== Команда /superstats для администратора ==================
@dp.message(Command("superstats"))
async def admin_stats_handler(message: types.Message):
    """Показывает статистику бота: пользователей, сообщений, топ-5."""
    if not ADMIN_ID or message.from_user.id != ADMIN_ID:
        await message.answer("⛔ У вас нет доступа к этой команде.")
        return

    try:
        # Количество пользователей
        users = get_all_users()
        total_users = len(users)

        # Считаем общее количество сообщений
        with sqlite3.connect("users.db") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM messages")
            total_messages = cursor.fetchone()[0] or 0

        # Топ-5 пользователей по количеству сообщений
        with sqlite3.connect("users.db") as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT user_id, COUNT(*) as msg_count 
                FROM messages 
                GROUP BY user_id 
                ORDER BY msg_count DESC 
                LIMIT 5
                """)
            top_users = cursor.fetchall()

        result_text = (f"📊 <b>Статистика бота:</b>\n\n"
                       f"👥 Всего пользователей: <b>{total_users}</b>\n"
                       f"✉️ Всего сообщений: <b>{total_messages}</b>\n")

        if top_users:
            result_text += "\n🏆 <b>Топ-5 самых болтливых:</b>\n"
            for i, (uid, count) in enumerate(top_users, start=1):
                result_text += f"{i}. <code>{uid}</code> — <b>{count}</b> сообщений\n"
        else:
            result_text += "\nПока никто не написал ни одного сообщения..."

        await message.answer(result_text)

    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        await message.answer("⚠️ Не удалось получить статистику.")


# ================== Планировщик (запуск) ==================
async def on_startup(app):
    """Запускается при старте aiohttp-приложения (установка webhook, планировщик)."""
    try:
        await bot.set_webhook(url=WEBHOOK_URL,
                              drop_pending_updates=True,
                              allowed_updates=dp.resolve_used_update_types())
        # Каждую неделю (понедельник 9:00 МСК)
        scheduler.add_job(
            weekly_updates_task,
            CronTrigger(day_of_week="mon",
                        hour=9,
                        minute=0,
                        timezone="Europe/Moscow"))
        scheduler.start()

        if ADMIN_ID:
            await bot.send_message(
                ADMIN_ID, f"🌀 Бот активирован!\n"
                f"⏰ Серверное время: {datetime.datetime.now():%d.%m.%Y %H:%M}")
    except Exception as e:
        logger.critical(f"Ошибка при запуске: {e}")


async def handle_root(request_: web.Request,
                      webhook_handler: SimpleRequestHandler):
    """HEAD → 200 (UptimeRobot), GET → 'Хроносфера активна', POST → webhook."""
    if request_.method == "HEAD":
        return web.Response(status=200)
    elif request_.method == "GET":
        return web.Response(text="🕰️ Хроносфера активна")
    else:
        return await webhook_handler.handle(request_)


def main():
    create_database()

    app = web.Application()
    webhook_handler = SimpleRequestHandler(dp, bot)

    async def root_route(request_: web.Request):
        return await handle_root(request_, webhook_handler)

    app.router.add_route('*', '/', root_route)
    setup_application(app, dp, bot=bot)
    app.on_startup.append(on_startup)

    try:
        web.run_app(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
    except Exception as e:
        logger.critical(f"Остановка времени: {e}")
    finally:
        scheduler.shutdown()


if __name__ == "__main__":
    main()
