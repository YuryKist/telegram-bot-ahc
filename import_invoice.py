# import_invoice.py
"""
Модуль для обработки PDF-счетов и обновления Реестра АХЧ.
Ожидает:
- PDF-файлы в директории (с фразой "счет")
- Файл *Реестр АХЧ*.xlsx — основной реестр
Добавляет новые счета и возвращает сообщение.
"""

import re
import os
import PyPDF2
import pandas as pd
from pathlib import Path
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from dateutil.parser import parse
from loguru import logger
from import_1C import update_excel_file as update_excel_file
from import_1C import load_ahx_data as load_ahx_data
from import_1C import prepare_register as prepare_register

# Настройка логирования (в файл и в stdout)
logger.remove()  # Убираем стандартный handler
logger.add("invoice_log.log", rotation="10 MB", level="INFO", encoding="utf-8", backtrace=True, diagnose=True)
logger.add(lambda msg: print(msg, end=''), level="INFO", colorize=True)  # Логи в консоль


def get_pdf_files(directory):
    """Возвращает список PDF-файлов в директории."""
    try:
        dir_path = Path(directory)
        if not dir_path.is_dir():
            raise NotADirectoryError(f"Путь '{directory}' не является директорией.")
        pdf_files = [f.name for f in dir_path.iterdir() if f.is_file() and f.suffix.lower() == '.pdf']
        logger.info(f"📁 Найдено {len(pdf_files)} PDF-файлов.")
        return pdf_files
    except Exception as e:
        logger.error(f"[Ошибка поиска PDF] {e}")
        return []


def process_pdf_files(directory, filename):
    """Извлекает текст из одного PDF-файла."""
    file_path = Path(directory) / filename
    try:
        with open(file_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            text = ''
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
            logger.info(f"📄 Успешно извлечён текст из '{filename}' ({len(reader.pages)} стр.)")
            logger.info(f"{text[100:500]}")
            return text
    except Exception as e:
        logger.error(f"❌ Ошибка при чтении PDF '{filename}': {e}")
        return None


def extract_amount(text):
    """Извлекает сумму после фразы 'на сумму'."""
    match = re.search(r'(?:на сумму)\D*([0-9\s.,]+)', text, re.IGNORECASE | re.DOTALL)
    if match:
        amount_str = match.group(1).strip()
        cleaned = re.sub(r'[^\d.,]', '', amount_str).replace(',', '.')
        amount = pd.to_numeric(cleaned, errors='coerce')
        if pd.notna(amount):
            logger.info(f"💰 Извлечена сумма: {amount}")
            return amount
    logger.debug("Сумма не найдена.")
    return None


def extract_supplier(text):
    """Извлекает поставщика (ООО или ИП)."""
    match_ooo = re.search(r'Поставщик:.*?"([А-ЯЁ][А-ЯЁ\s\d\-]+)', text, re.IGNORECASE | re.DOTALL)
    if match_ooo:
        supplier = match_ooo.group(1).strip().title()
        logger.info(f"🏭 Поставщик (ООО): {supplier}")
        return supplier

    match_ip = re.search(r'(?:Индивидуальный предприниматель|ИП)\s+([А-ЯЁ][А-ЯЁа-яё\s\-]+)', text, re.IGNORECASE | re.DOTALL)
    if match_ip:
        full_name = match_ip.group(1).strip()
        surname = full_name.split()[0].title()
        logger.info(f"👤 Поставщик (ИП): {surname}")
        return surname

    logger.debug("Поставщик не найден.")
    return None


def extract_date_from_line(line):
    """Извлекает дату после слова 'от'."""
    if "от" not in line.lower():
        return None
    index = line.lower().find("от") + 2
    date_part = line[index:].strip()

    if "г." in date_part:
        date_part = date_part.split("г.")[0].strip()
    elif "года" in date_part:
        date_part = date_part.split("года")[0].strip()

    month_list = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
                  'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
    for i, month in enumerate(month_list, 1):
        if month in date_part:
            date_part = date_part.replace(month, str(i)).replace(' ', '.')
            break
    return date_part


def get_num_invoce(text, target_phrase):
    """Извлекает номер и дату счета по ключевой фразе."""
    invoice_number = None
    invoice_date = None
    for line in text.splitlines():
        if target_phrase in line.lower():
            parts = line.split("оплату №")
            if len(parts) > 1:
                invoice_number = parts[1].strip().split()[0]
                invoice_date = extract_date_from_line(line)
                break
    logger.info(f"💰 Извлечена дата: {invoice_date}")
    logger.info(f"💰 Извлечен номер: {invoice_number}")
    return invoice_number, invoice_date


def extract_invoice_data(pdf_files, directory, df_register, target_phrase="счет"):
    """Извлекает данные из всех PDF-файлов."""
    invoice_data_list = []
    for f in pdf_files:
        try:
            text = process_pdf_files(directory, f)
            if not text:
                logger.warning(f"⚠️ Не удалось извлечь текст из '{f}'. Пропущен.")
                continue

            invoice_number, invoice_date = get_num_invoce(text, target_phrase)
            supplier = extract_supplier(text)
            amount = extract_amount(text)

            if invoice_number and invoice_number not in df_register['№ счета'].values:
                invoice_data_list.append({
                    '№ счета': invoice_number,
                    'Дата счета': invoice_date,
                    'Поставщик': supplier,
                    'Сумма': amount
                })
                logger.info(f"✅ Счёт №{invoice_number} добавлен из '{f}'")
            else:
                logger.info(f"ℹ️ Счёт №{invoice_number} уже есть в реестре или не найден.")
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке '{f}': {e}")

    return pd.DataFrame(invoice_data_list, columns=['№ счета', 'Дата счета', 'Поставщик', 'Сумма'])


def update_register_with_new_invoices(df_register, df_invoice_reg):
    """
    Обновляет реестр счетов, добавляя новые счета из df_invoice_reg.
    Генерирует уникальные номера для новых записей (вида 'Ю-1', 'Ю-2' и т.д.).
    """
    if df_invoice_reg.empty:
        logger.info("ℹ️ Нет новых счетов для добавления в реестр.")
        return "ℹ️ Нет новых счетов для добавления в реестр."
    try:
        # 1. Объединяем основной датафрейм с новыми строками
        df_register = pd.concat([df_register, df_invoice_reg], ignore_index=True)
        logger.info(f"✅ Добавлено {len(df_invoice_reg)} новых записей в реестр.")
        # 2. Приводим даты к формату datetime
#        df_register['Дата счета'] = pd.to_datetime(df_register['Дата счета'], errors='coerce')
        # 3. Извлекаем числа из существующих номеров "Ю-..."
        df_register['number_part'] = df_register['№ синей накладной'].str.extract(r'(\d+)')[0]
        df_register['number_part'] = pd.to_numeric(df_register['number_part'], errors='coerce')
        # 4. Находим максимальный номер
        last_number = df_register['number_part'].max()
        start_num = int(last_number) + 1 if pd.notna(last_number) else 1
        # 5. Генерируем новые номера для NaN
        nan_count = df_register['№ синей накладной'].isna().sum()
        last_prefix = df_register['№ синей накладной'].str.findall(r'[А-ЯЁ]+').str[-1].loc[0]
        new_numbers = [f'{last_prefix}-{i}' for i in range(start_num, start_num + nan_count)]
        # 6. Заполняем пропущенные значения
        df_register.loc[df_register['№ синей накладной'].isna(), '№ синей накладной'] = new_numbers
        # 7. Убираем временный столбец
        df_register.drop(columns=['number_part'], inplace=True)
        return df_register
    except Exception as e:
        logger.exception(f"❌ Ошибка при обновлении реестра: {e}")
        return "❌ Ошибка при обновлении реестра"

def run_pipeline(directory_path: str) -> str:
    """
    Основная функция — запускает пайплайн.
    Возвращает строку-результат для Telegram-бота.
    """
    try:
        logger.info("🔹 Запуск пайплайна: обработка PDF-счетов")

        # Шаг 1: Поиск PDF
        pdf_files = get_pdf_files(directory_path)
        if not pdf_files:
            return "⚠️ В папке нет PDF-файлов."

        # Шаг 2: Загрузка реестра
        df_register, output_file_path = load_ahx_data(directory_path)
        df_register_clean = prepare_register(df_register)

        logger.info(f"Обновление реестра: {output_file_path}")

        # Шаг 3: Извлечение данных из PDF
        df_invoice_reg = extract_invoice_data(pdf_files, directory_path, df_register_clean, "счет")
        if df_invoice_reg.empty:
            return "ℹ️ Новых счетов для добавления не найдено."

        # Шаг 4: Обновление реестра
        df_updated = update_register_with_new_invoices(df_register_clean, df_invoice_reg)

        # Шаг 5: Сохранение
        success = update_excel_file(output_file_path, df_updated)
        if success:
            logger.success("Данные из Bitrix успешно обновлены в Реестре АХЧ")
            return "✅ Счета из PDF добавлены в Реестр АХЧ."
        else:
            logger.error(f"Ошибка при сохранении: {success}")
            return "❌ Ошибка при сохранении файла."

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        return f"❌ Ошибка выполнения: {str(e)}"


# --- Для теста (не обязательно) ---
if __name__ == "__main__":
    test_path = r"C:\Users\Юрий Кистенев\Desktop\ACH_manager"
    result = run_pipeline(test_path)
    print(result)