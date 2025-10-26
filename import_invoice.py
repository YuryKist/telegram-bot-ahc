# import_invoice.py
"""
Модуль для обработки PDF-счетов и обновления Реестра АХЧ.
Ожидает:
- PDF-файлы в директории (с фразой "счет")
- Файл *Реестр АХЧ*.xlsx — основной реестр
Добавляет новые счета и возвращает сообщение.
"""

import re
import pdfplumber
import pandas as pd
from datetime import datetime
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
        with pdfplumber.open(file_path) as pdf:
            text = ''
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
            logger.info(f"📄 Успешно извлечён текст из '{filename}' ({len(text)} стр.)")
            logger.info(f"{text[100:300]}")
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


def extract_supplier(text: str) -> str:
    """Извлекает поставщика (ООО или ИП)."""
    lower_text = text.lower()
    match_start = re.search(r'получател', lower_text)
    if not match_start:
        logger.debug("Поставщик не найден.")
        return None 
    
    # Начинаем поиск в тексте
    search_area = text[match_start.start():]
    logger.debug(f"🔍 Search area: {repr(search_area[:100])}")
    patterns = [
        # 3. ИП полное имя (берём только первое слово после ИП)
        r'ИП\s+([А-ЯЁ][А-ЯЁа-яё\-]+)',
        # 4. ИП с инициалами
        r'ИП\s+([А-ЯЁ][а-яё]+?)\s+[А-ЯЁ]\.\s*[А-ЯЁ]\.',
        # 5. Полная форма ИП
        r'Индивидуальный\s+предприниматель\s+([А-ЯЁ][а-яё]+)',
        # 1. ООО в кавычках (любые кавычки: «», "", '')
        r'ООО\s*[«"]([^»"]+?)[»"]',
        # 2. ООО без кавычек — захватываем всё до первого "стоп-слова" или конца строки
        r'ООО\s+([А-ЯЁ][А-ЯЁа-яё\s\-]+?)(?=\s+(?:ИНН|КПП|Сч\.?|Вид|Наз\.|Очер|Код|Рез|Оплата|Банк|$))'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, search_area)
        if match:
            name = match.group(1).strip()
            # Убираем возможные кавычки
            name = re.sub(r'^["«"]+|["»"]+$', '', name)
            supplier = name.strip()
            logger.info(f"🏭 Поставщик: {supplier}")
            return supplier
    
    logger.debug("Поставщик не найден.")
    return None  # если ничего не найдено


def get_date_from_line(text: str):
    # Словарь для перевода названий месяцев в числа
    month_names = {
        'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
        'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
        'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
    }

    # 1. Поиск даты в формате дд.мм.гггг
    dot_date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', text)
    if dot_date_match:
        day, month, year = map(int, dot_date_match.groups())
        try:
            date_part = datetime(year, month, day).date()
            logger.info(f"📆 Извлечена дата: {date_part}")
            return date_part
        except ValueError:
            pass  # Некорректная дата

    # 2. Поиск даты в формате "д (или дд) месяц гггг"
    word_date_pattern = r'(\d{1,2})\s*([а-яё]+)\s+(\d{4})\s*(?:г\.?)?'
    matches = re.finditer(word_date_pattern, text, re.IGNORECASE)
    for match in matches:
        day_str, month_word, year_str = match.groups()
        day = int(day_str)
        year = int(year_str)
        month_word = month_word.lower()

        if month_word in month_names:
            month = month_names[month_word]
            try:
                date_part = datetime(year, month, day).date()
                logger.info(f"📆 Извлечена дата: {date_part}")
                return date_part
            except ValueError:
                continue  # Некорректная дата

    # Если ничего не найдено
    return None


def get_num_invoce(text, target_phrase):
    """Извлекает номер по ключевой фразе."""
    invoice_number = None
    for line in text.splitlines():
        if target_phrase in line.lower():
            parts = line.split("оплату №")
            if len(parts) > 1:
                invoice_number = parts[1].strip().split()[0]
                break
    logger.info(f"💰 Извлечен номер: {invoice_number}")
    return invoice_number


def extract_invoice_data(pdf_files, directory, target_phrase="счет"):
    """Извлекает данные из всех PDF-файлов."""
    invoice_data_list = []
    for f in pdf_files:
        try:
            text = process_pdf_files(directory, f)
            if not text:
                logger.warning(f"⚠️ Не удалось извлечь текст из '{f}'. Пропущен.")
                continue

            invoice_number = get_num_invoce(text, target_phrase)
            invoice_date = get_date_from_line(text)
            supplier = extract_supplier(text)
            amount = extract_amount(text)
            invoice_data_list.append({
                    '№ счета': invoice_number,
                    'Дата счета': invoice_date,
                    'Поставщик': supplier,
                    'Сумма': amount
                })

            logger.info(f"✅ Счёт №{invoice_number} добавлен из '{f}'")

        except Exception as e:
            logger.error(f"❌ Ошибка при обработке '{f}': {e}")

    
    invoice_df = pd.DataFrame(invoice_data_list, columns=['№ счета', 'Дата счета', 'Поставщик', 'Сумма'])
    invoice_df['Дата счета'] = pd.to_datetime(invoice_df['Дата счета'], errors='coerce')
    invoice_df['Контроль оплаты'] = invoice_df['Дата счета'] + pd.Timedelta(days=21)
    invoice_df['№ счета'] = invoice_df['№ счета'].pipe(
        lambda series: series.fillna('')
        .astype("string")
        .str.lower()
        .str.lstrip('0')
    )

    invoice_df['Сумма'] = pd.to_numeric(invoice_df['Сумма'], errors='coerce').round(2)

    return invoice_df


def update_register_with_new_invoices(df_register, df_invoice_reg):
    """
    Обновляет реестр счетов, добавляя новые счета из df_invoice_reg.
    Генерирует уникальные номера для новых записей.
    """
    if df_invoice_reg.empty:
        logger.info("ℹ️ Нет новых счетов для добавления в реестр.")
        return "ℹ️ Нет новых счетов для добавления в реестр."
    try:
        # 1. Объединяем основной датафрейм с новыми строками
        df_export_unique = df_invoice_reg.drop_duplicates(subset=['№ счета', 'Сумма'])
        df_register_keep = df_register[['№ счета', 'Сумма']]
        
        df_merged = df_export_unique.merge(
            df_register_keep,
            on=['№ счета', 'Сумма'],
            how='left',
#            suffixes=('', '_merge'),
            indicator=True
            )
        new_rows = df_merged[df_merged['_merge'] == 'left_only'].drop('_merge', axis=1)
        logger.info(f"✅ Добавлено {new_rows} новых записей в реестр.")
        df_register = pd.concat([df_register, new_rows], ignore_index=True)

        logger.info(f"✅ Добавлено {len(new_rows)} новых записей в реестр.")
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
        df_invoice_reg = extract_invoice_data(pdf_files, directory_path, "счет")
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
    test_path = r"C:\Users\Юрий Кистенев\Desktop\ACH_manager\record"
    result = run_pipeline(test_path)
    print(result)