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
    filters,
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

# Клавиатура для завершения загрузки
done_keyboard = [['/done']]
markup_done = ReplyKeyboardMarkup(done_keyboard, resize_keyboard=True)


# Добавим список действий, где нужен только один файл
SINGLE_FILE_ACTIONS = {
    'Загрузить выписку из 1С',
    'Загрузить отчёт из Bitrix'
}

# Меняем prompts — не нужно упоминать /done для Excel
prompts = {
    'Загрузить выписку из 1С': "Отправь Excel-файл с выпиской из 1С.",
    'Загрузить отчёт из Bitrix': "Отправь Excel-файл с отчётом из Bitrix.",
    'Подгрузить счёт (PDF)': "Отправь PDF-файл(ы) со счётом. Когда закончишь — нажми «Готово».",
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

    if not file or not file.file_name.lower().endswith('.xlsx'):
        await update.message.reply_text("Пожалуйста, загрузи корректный Excel-файл (.xlsx).")
        return UPLOAD_REGISTRY

    temp_dir = Path(tempfile.mkdtemp())
    user_data[user_id] = {'temp_dir': temp_dir}


    new_file = await file.get_file()
    file_path = temp_dir / file.file_name
    await new_file.download_to_drive(file_path)
    logger.info(f"Файл реестра загружен: {file_path}")


    markup = ReplyKeyboardMarkup(reply_keyboard, one_time_keyboard=False, resize_keyboard=True)
    await update.message.reply_text("Выберите действие:", reply_markup=markup)
    return MENU


async def menu_choice(update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    choice = update.message.text
    logger.info(f"Пользователь {user_id} выбрал: {choice}")

    if choice in prompts:
        user_data[user_id]['current_action'] = choice

        # Если это одиночный файл (Excel), не показываем "Готово"
        if choice in SINGLE_FILE_ACTIONS:
            await update.message.reply_text(prompts[choice])
            return UPLOAD_FILE
        else:
            # Для PDF — показываем кнопку "Готово"
            markup = ReplyKeyboardMarkup([['Готово']], resize_keyboard=True)
            await update.message.reply_text(prompts[choice], reply_markup=markup)
            return UPLOAD_FILE

    elif choice == 'Выгрузить реестр АХЧ':
        temp_dir = user_data[user_id]['temp_dir']
        registry_files = list(temp_dir.glob("*реестр*ахч*.xlsx")) or \
                         list(temp_dir.glob("*АХЧ*.xlsx")) or \
                         list(temp_dir.glob("*Реестр*.xlsx")) or \
                         list(temp_dir.glob("*.xlsx"))

        if registry_files:
            registry_path = registry_files[0]
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


# =============== НОВЫЙ: ОБРАБОТЧИК ФАЙЛОВ (ТОЛЬКО СБОР) ===============

async def file_received(update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    action = user_data[user_id]['current_action']
    temp_dir = user_data[user_id]['temp_dir']

    if not update.message.document:
        await update.message.reply_text("Пожалуйста, отправьте файл как документ.")
        return UPLOAD_FILE

    file = update.message.document

    # Проверка типа файла
    if action == 'Подгрузить счёт (PDF)':
        if not file.file_name.lower().endswith('.pdf'):
            await update.message.reply_text("Поддерживаются только PDF-файлы.")
            return UPLOAD_FILE
    else:
        allowed_extensions = ('.xls', '.xlsx', '.xlsm', '.xlsb', '.xltx', '.xltm')
        if not any(file.file_name.lower().endswith(ext) for ext in allowed_extensions):
            await update.message.reply_text("Поддерживаются только Excel-файлы (.xls*).")
            return UPLOAD_FILE

    # Избегаем дубликатов
    file_path = temp_dir / file.file_name
    counter = 1
    stem = file_path.stem
    suffix = file_path.suffix
    while file_path.exists():
        file_path = temp_dir / f"{stem}_{counter}{suffix}"
        counter += 1

    # Скачиваем
    try:
        new_file = await file.get_file()
        await new_file.download_to_drive(file_path)
        logger.info(f"Файл загружен: {file_path}")
        await update.message.reply_text(f"Файл '{file_path.name}' успешно загружён.")
    except Exception as e:
        logger.error(f"Ошибка загрузки файла: {e}")
        await update.message.reply_text(f"Ошибка при загрузке файла: {e}")
        return UPLOAD_FILE

    # 🔹 Автозапуск для Excel (1С и Bitrix)
    if action in SINGLE_FILE_ACTIONS:
        try:
            if action == 'Загрузить выписку из 1С':
                run_1c_pipeline(str(temp_dir))
            elif action == 'Загрузить отчёт из Bitrix':
                run_bitrix_pipeline(str(temp_dir))

            await update.message.reply_text(
                "Файл успешно обработан.\nВыберите следующее действие:",
                reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)
            )
        except Exception as e:
            logger.error(f"Ошибка при выполнении pipeline: {e}", exc_info=True)
            await update.message.reply_text(
                "Произошла ошибка при обработке файла.",
                reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)
            )

        # Очищаем только current_action, temp_dir остаётся
        del user_data[user_id]['current_action']
        return MENU

    # 🔹 Для PDF — остаёмся в UPLOAD_FILE, ждём "Готово"
    return UPLOAD_FILE


# =============== НОВАЯ КОМАНДА: /done ===============

async def done_command(update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in user_data or 'current_action' not in user_data[user_id]:
        await update.message.reply_text(
            "Нет активной загрузки.",
            reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)
        )
        return MENU

    action = user_data[user_id]['current_action']
    temp_dir = user_data[user_id]['temp_dir']

    files = list(temp_dir.glob("*.pdf"))
    if not files:
        await update.message.reply_text("Не найдено ни одного PDF-файла.")
        return MENU

    try:
        run_invoice_pipeline(str(temp_dir))
        await update.message.reply_text(
            f"✅ Успешно обработано {len(files)} PDF-файлов.\nВыберите следующее действие:",
            reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)
        )
    except Exception as e:
        logger.error(f"Ошибка при выполнении pipeline: {e}", exc_info=True)
        await update.message.reply_text(
            "❌ Ошибка при обработке файлов.",
            reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)
        )

    del user_data[user_id]['current_action']
    return MENU


# =============== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===============

async def send_logs_and_cleanup(update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    log_file = "bot_logs.log"
    temp_dir = user_data[user_id].get('temp_dir')

    log_files = []

    if os.path.exists(log_file):
        log_files.append(log_file)

    if temp_dir and os.path.exists(temp_dir):
        log_files.extend([
            os.path.join(temp_dir, f) for f in os.listdir(temp_dir)
            if f.endswith('.log') and os.path.isfile(os.path.join(temp_dir, f))
        ])

    for file_path in log_files:
        try:
            with open(file_path, 'rb') as f:
                await update.message.reply_document(document=f)
        except Exception as e:
            logger.error(f"Не удалось отправить {file_path}: {e}")

    if temp_dir and os.path.exists(temp_dir):
        shutil.rmtree(temp_dir, ignore_errors=True)

    user_data.pop(user_id, None)
    logger.info(f"Пользователь {user_id} завершил сессию.")


async def cleanup(user_id):
    if user_id in user_data:
        temp_dir = user_data[user_id]['temp_dir']
        if temp_dir and os.path.exists(temp_dir):
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
        UPLOAD_FILE: [
            MessageHandler(filters.Document.ALL, file_received),
            MessageHandler(filters.TEXT & filters.Regex("^Готово$"), done_command),  # Только для PDF
        ],
    },
    fallbacks=[
        CommandHandler('start', start),
        CommandHandler('cancel', send_logs_and_cleanup),
    ],
    per_user=True,
)



    app.add_handler(conv_handler)
    logger.info("Бот запущен.")
    app.run_polling()


if __name__ == '__main__':
    main()