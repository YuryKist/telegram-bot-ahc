import os
import shutil
import tempfile
from pathlib import Path

from dotenv import load_dotenv
from telegram import ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    ConversationHandler,
    filters
)
from loguru import logger

from import_1C import run_pipeline as run_1c_pipeline
from import_Bitrix import run_pipeline as run_bitrix_pipeline
from import_invoice import run_pipeline as run_invoice_pipeline

# Загрузка переменных окружения
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")

# Логирование
logger.remove()
logger.add("bot_logs.log", rotation="10 MB", level="DEBUG")

# Состояния
UPLOAD_REGISTRY, MENU, UPLOAD_FILE = range(3)

# Меню
reply_keyboard = [
    ['Загрузить выписку из 1С'],
    ['Загрузить отчёт из Bitrix'],
    ['Подгрузить счёт (PDF)'],
    ['Выгрузить реестр АХЧ'],
    ['Отмена']
]

prompts = {
    'Загрузить выписку из 1С': "Отправь Excel-файл с выпиской из 1С.",
    'Загрузить отчёт из Bitrix': "Отправь Excel-файл с отчётом из Bitrix.",
    'Подгрузить счёт (PDF)': "Отправь PDF-файл со счётом."
}

# Хранилище данных пользователей
user_data = {}

# =================== ОСНОВНЫЕ ФУНКЦИИ ===================

async def start(update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    logger.info(f"Пользователь {user.id} ({user.username}) начал работу.")
    await update.message.reply_text(
        f"Привет, {user.first_name}! Пожалуйста, загрузи файл *Реестр АХЧ* (Excel).",
        parse_mode="Markdown"
    )
    return UPLOAD_REGISTRY


async def registry_received(update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    file = update.message.document

    if not file or not file.file_name.endswith('.xlsx'):
        await update.message.reply_text("Пожалуйста, загрузи корректный Excel-файл.")
        return UPLOAD_REGISTRY

    temp_dir = Path(tempfile.mkdtemp())
    user_data[user_id] = {'temp_dir': temp_dir}

    # Скачивание файла
    new_file = await file.get_file()
    file_path = temp_dir / file.file_name
    await new_file.download_to_drive(file_path)
    logger.info(f"Файл реестра загружен: {file_path}")

    # Открытие меню
    markup = ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=False, resize_keyboard=True)
    await update.message.reply_text("Выберите действие:", reply_markup=markup)
    return MENU


async def menu_choice(update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    choice = update.message.text
    logger.info(f"Пользователь {user_id} выбрал: {choice}")

    if choice in prompts:
        user_data[user_id]['current_action'] = choice
        await update.message.reply_text(prompts[choice])
        return UPLOAD_FILE

    elif choice == 'Выгрузить реестр АХЧ':
        temp_dir = user_data[user_id]['temp_dir']
        # Ищем файл с названием, содержащим "реестр" и "ахч" (регистронезависимо)
        registry_files = list(temp_dir.glob("*реестр*ахч*.xlsx")) or \
                        list(temp_dir.glob("*АХЧ*.xlsx")) or \
                        list(temp_dir.glob("*Реестр*.xlsx"))
        if not registry_files:
            # Если не нашли по шаблону, ищем любой xlsx файл (резервный вариант)
            registry_files = list(temp_dir.glob("*.xlsx"))
        
        if registry_files:
            registry_path = registry_files[0]  # Берем первый найденный
            await update.message.reply_document(document=open(registry_path, 'rb'))
        else:
            await update.message.reply_text("Реестр не найден.")

        await cleanup(user_id)
        await start(update, context)
        return UPLOAD_REGISTRY

    elif choice == 'Отмена':
        await send_logs_and_cleanup(update, context)
        await start(update, context)
        return UPLOAD_REGISTRY

    else:
        await update.message.reply_text("Неизвестная команда.")
        return MENU


async def file_received(update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    action = user_data[user_id]['current_action']
    temp_dir = user_data[user_id]['temp_dir']

    file = update.message.document or update.message.photo and update.message.photo[-1]
    if not file:
        await update.message.reply_text("Пожалуйста, отправьте файл.")
        return UPLOAD_FILE

    file_name = file.file_name or "file"
    file_path = temp_dir / file_name
    new_file = await file.get_file()
    await new_file.download_to_drive(file_path)
    logger.info(f"Файл загружен: {file_path}")

    # Запуск соответствующего pipeline
    try:
        if action == 'Загрузить выписку из 1С':
            run_1c_pipeline(str(temp_dir))
        elif action == 'Загрузить отчёт из Bitrix':
            run_bitrix_pipeline(str(temp_dir))
        elif action == 'Подгрузить счёт (PDF)':
            run_invoice_pipeline(str(temp_dir))
    except Exception as e:
        logger.error(f"Ошибка при выполнении pipeline: {e}")
        await update.message.reply_text("Произошла ошибка при обработке файла.")

    await update.message.reply_text("Файл успешно обработан. Выберите следующее действие.")
    return MENU


async def send_logs_and_cleanup(update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    log_file = "bot_logs.log"
    temp_dir = user_data[user_id]['temp_dir']

    if os.path.exists(log_file):
        await update.message.reply_document(document=open(log_file, 'rb'))

    shutil.rmtree(temp_dir, ignore_errors=True)
    user_data.pop(user_id, None)
    logger.info(f"Пользователь {user_id} завершил сессию и данные удалены.")


async def cleanup(user_id):
    temp_dir = user_data[user_id]['temp_dir']
    shutil.rmtree(temp_dir, ignore_errors=True)
    user_data.pop(user_id, None)
    logger.info(f"Данные пользователя {user_id} очищены.")


# =================== ГЛАВНАЯ ФУНКЦИЯ ===================

def main():
    app = ApplicationBuilder().token(TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            UPLOAD_REGISTRY: [MessageHandler(filters.Document.ALL, registry_received)],
            MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, menu_choice)],
            UPLOAD_FILE: [MessageHandler(filters.Document.ALL | filters.PHOTO, file_received)],
        },
        fallbacks=[CommandHandler('start', start)]
    )

    app.add_handler(conv_handler)
    logger.info("Бот запущен.")
    app.run_polling()


if __name__ == '__main__':
    main()