import json
import os
import logging
import random
import datetime
import sqlite3

from typing import Optional
from aiogram import Bot, Dispatcher, types
from aiogram.types import Update
from aiogram.client.bot import DefaultBotProperties
from aiogram.fsm.storage.memory import MemoryStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("BOT_TOKEN не задан!")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(bot, storage=MemoryStorage())

# --- Пример ваших функций и обработчиков ---
# Вместо "..." подставьте свою логику, обработчики, работу с БД и т.п.

@dp.message_handler(commands=["start", "help"])
async def cmd_start(message: types.Message):
    await message.answer("Привет! Я бот, запущенный на Vercel.")

async def handler(request):
    """
    Главная функция, вызываемая при запросе к /api/webhook
    (Vercel серверлес-функция).
    """
    try:
        data = await request.json()
        update = Update(**data)
        await dp.feed_update(update)
        return {
            "statusCode": 200,
            "body": json.dumps({"ok": True})
        }
    except Exception as e:
        logger.error(f"Ошибка в webhook: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
