# import_Bitrix.py
"""
Модуль для обработки выгрузки из Bitrix и обновления Реестра АХЧ.
Ожидает два файла в директории:
- *Реестр АХЧ*.xlsx — основной реестр
- *Bitrix*.xlsx — выгрузка из Битрикс
Обновляет реестр и возвращает сообщение об успехе или ошибке.
"""

import re
import pandas as pd
from pathlib import Path
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.cell import MergedCell
from loguru import logger
from import_1C import update_excel_file as update_excel_file
from import_1C import load_ahx_data as load_ahx_data
from import_1C import prepare_register as prepare_register
from import_1C import update_date_payment as update_date_payment



def load_bitrix(directory_path: str):
    """Загружает df_bitrix из указанной директории."""
    logger.info(f"Загрузка данных из директории: {directory_path}")
    directory = Path(directory_path)
    excel_files_bitrix = list(directory.glob('*Bitrix*.xls*'))

    if not excel_files_bitrix:
        logger.error("Файл с 'Bitrix' не найден в указанной директории")
        raise FileNotFoundError("❌ Файл с 'Bitrix' не найден в указанной директории.")
    
    logger.info(f"Найден файл Bitrix: {excel_files_bitrix[0].name}")

    dtypes = {
#        'ID': str,
        'Номер счета': str,
        'Сумма': str,
        'Дата счета': 'datetime64[ns]',
        'Статус Счета': str,
    }
        # Чтение файла Bitrix
    df_bitrix = pd.read_excel(
        excel_files_bitrix[0],  # обычно первый файл — нужный
#        names = columns,
        dtype=dtypes,
        engine='openpyxl'
    )
    
    logger.info(f"📊 Загружено {len(df_bitrix)} записей из реестра Bitrix.")

    return df_bitrix

def extract_task_id(url: str) -> str:
    """Извлекает ID задачи из ссылки"""
    match = re.search(r'/view/(\d+)/?$', url)
    if match:
        return match.group(1)
    return None


def prepare_bitrix(df_bitrix):
    """Подготавливает данные: извлечение ID, приведение типов, замена статусов."""
    logger.info("Подготовка данных Bitrix")
    df_bitrix = df_bitrix.copy()

    df_bitrix = df_bitrix.rename(columns={
        df_bitrix.columns[0]: 'ID',
        df_bitrix.columns[1]: 'Things',
        df_bitrix.columns[2]: 'Recierver',
        'Объект': 'Object'
        })

    
    # Замена статусов
    status_changes = {
        'Оплачен': 'Отправлен в 1С',
        'Передан в оплату': 'Утверждена'
    }
    df_bitrix['Статус Счета'] = df_bitrix['Статус Счета'].replace(status_changes)

     # Извлечение ID задачи из ссылки
    logger.info("Извлечение ID задач из ссылок")
    df_bitrix['ID task'] = (
        df_bitrix.loc[df_bitrix['Ссылка на задачу'].notna(), 'Ссылка на задачу']
        .astype("string")
        .apply(extract_task_id)
    )
    df_bitrix['ID task'] = pd.to_numeric(df_bitrix['ID task'], errors='coerce').astype(dtype = int, errors = 'ignore')
    df_bitrix['Номер счета'] = df_bitrix['Номер счета'].astype("string").str.lstrip('0')
    df_bitrix['Сумма'] = pd.to_numeric(df_bitrix['Сумма'], errors='coerce').round(2)
    df_bitrix.columns = ['ID'] + list(df_bitrix.columns[1:])
    df_bitrix['ID'] = pd.to_numeric(df_bitrix['ID'], errors='coerce').astype('Int32')

    task_ids_count = df_bitrix['ID task'].notna().sum()
    logger.info(f"Извлечено {task_ids_count} ID задач из ссылок")
    
    logger.success("✅ Данные подготовлены.")
    return df_bitrix


def preprocess_bitrix_data(df_bitrix):
    """Переименование столбцов и удаление дубликатов."""
    logger.info("Предобработка данных Bitrix")
    df_bitrix = df_bitrix.rename(columns={
        'Номер счета': '№ счета',
        'Статус Счета': 'Новый статус',
        'ID task': '№ задачи Битрикс'
    })
    
    before_dedup = len(df_bitrix)
    df_bitrix = df_bitrix.drop_duplicates(subset=['№ задачи Битрикс', 'Сумма'])
    after_dedup = len(df_bitrix)
    
    removed_duplicates = before_dedup - after_dedup
    if removed_duplicates > 0:
        logger.info(f"Удалено {removed_duplicates} дубликатов")
    
    logger.success(f"Предобработка завершена. Итоговое количество строк: {len(df_bitrix)}")

    return df_bitrix


def fill_invoice_numbers_from_bitrix(df_register, df_bitrix):
    """Заполняет № счета в реестре на основе совпадения по № задачи и сумме."""
    logger.info("Заполнение номеров счетов из Bitrix")
    
    if '№ задачи Битрикс' not in df_register.columns:
        logger.error("В реестре отсутствует столбец '№ задачи Битрикс'")
        raise KeyError("❌ В реестре отсутствует столбец '№ задачи Битрикс'.")

    df_bitrix_rize = df_bitrix[['№ задачи Битрикс', 'Сумма', '№ счета']].copy()
    df_bitrix_deduplicated = df_bitrix_rize.drop_duplicates(subset=['№ задачи Битрикс', 'Сумма'])

    df_register = df_register.merge(
        df_bitrix_deduplicated,
        on=['№ задачи Битрикс', 'Сумма'],
        how='left',
        suffixes=('', '_bitrix')
    )

    df_register['№ счета'] = df_register['№ счета'].replace('', pd.NA)
   
    fill_condition = df_register['№ счета'].isna() & df_register['№ счета_bitrix'].notna()
    df_register.loc[fill_condition, '№ счета'] = df_register.loc[fill_condition, '№ счета_bitrix']

    # Удаляем вспомогательную колонку
    df_register = df_register.drop('№ счета_bitrix', axis=1)
    df_register['№ счета'] = df_register['№ счета'].fillna('')
 
    updated_count = fill_condition.sum()
    if updated_count > 0:
        logger.success(f"Заполнено {updated_count} номеров счётов из Bitrix")

    return df_register


def update_register_payment_status_bitrix(df_register, df_bitrix):
    """Обновляет статус оплаты на основе данных из Bitrix."""
    logger.info("Обновление статусов оплаты из Bitrix")
    
    df_bitrix_mapped = df_bitrix[['№ счета', 'Сумма', 'Новый статус']].copy()
    df_bitrix_deduplicated = df_bitrix_mapped.drop_duplicates(subset=['№ счета', 'Сумма'])

    df_merged = df_register.merge(
        df_bitrix_deduplicated,
        on=['№ счета', 'Сумма'],
        how='left',
        suffixes=('', '_bitrix')
    )

    df_register = df_register.copy()
    match_condition = df_merged['Новый статус'].notna() & (~df_register['Статус оплаты'].isin(["Оплачено", "Подготовлено"]))
    df_register.loc[match_condition, 'Статус оплаты'] = df_merged.loc[match_condition, 'Новый статус']

    updated_count = match_condition.sum()
    if updated_count > 0:
        logger.success(f"Обновлено {updated_count} статусов оплаты")

    return df_register

def clean_text_from_stop_words_precise(text, stop_words):
    if pd.isna(text):
        return text
    
    # Приводим текст к нижнему регистру
    cleaned_text = str(text).lower()
    
    # Создаем паттерн для поиска стоп-слов как отдельных слов
    pattern = r'\b(?:' + '|'.join(re.escape(word.lower()) for word in stop_words) + r')\b'
    # Удаляем стоп-слова
    cleaned_text = re.sub(pattern, '', cleaned_text)
    
    # Очищаем лишние пробелы
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
    
    return cleaned_text


def update_register_id_payment_object_bitrix(df_register, df_bitrix):
    """Обновляет ID оплаты, если он пуст."""
    logger.info("Обновление ID оплаты ТМЦ Объект из Bitrix")
    
    df_bitrix_mapped = df_bitrix[['№ счета', 'Сумма', 'ID', 'Object', 'Things']].copy()
    df_bitrix_deduplicated = df_bitrix_mapped.drop_duplicates(subset=['№ счета', 'Сумма'])

    df_merged = df_register.merge(
        df_bitrix_deduplicated,
        on=['№ счета', 'Сумма'],
        how='left',
        suffixes=('', '_bitrix')
    )
       
        # Обновляем Объект
    match_condition = df_merged['Object'].notna() & df_register['Объект'].isna()
    df_register = df_register.copy()
    df_register.loc[match_condition, 'Объект'] = df_merged.loc[match_condition, 'Object']

    updated_count = match_condition.sum()
    if updated_count > 0:
        logger.success(f"Обновлено {updated_count} Объектов")

    # Обновляем ID
    match_condition = df_merged['ID'].notna() & df_register['ID_Счет_Bitrix'].isna()
    df_register = df_register.copy()
    df_register.loc[match_condition, 'ID_Счет_Bitrix'] = df_merged.loc[match_condition, 'ID']

    updated_count = match_condition.sum()
    if updated_count > 0:
        logger.success(f"Обновлено {updated_count} ID оплаты")

    # Стоп-слова для удаления
    stop_words = ['cчет', 'расходы', 'cчета', 'расходов', 'ахч']

    # Применяем очистку к столбцу Object перед обновлением
    df_merged['Things'] = df_merged['Things'].apply(lambda x: clean_text_from_stop_words_precise(x, stop_words))

    # Обновляем TMC
    match_condition = df_merged['Things'].notna() & df_register['ТМЦ'].isna()
    df_register = df_register.copy()
    df_register.loc[match_condition, 'ТМЦ'] = df_merged.loc[match_condition, 'Things']

    updated_count = match_condition.sum()
    if updated_count > 0:
        logger.success(f"Обновлено {updated_count} статусов оплаты")

    return df_register


def update_register_id_task_bitrix(df_register, df_bitrix):
    """Обновляет № задачи Битрикс, если он пуст."""
    logger.info("Обновление ID задач из Bitrix")
    
    df_bitrix_mapped = df_bitrix[['№ счета', 'Сумма', '№ задачи Битрикс']].copy()
    df_bitrix_deduplicated = df_bitrix_mapped.drop_duplicates(subset=['№ счета', 'Сумма'])

    df_merged = df_register.merge(
        df_bitrix_deduplicated,
        on=['№ счета', 'Сумма'],
        how='left',
        suffixes=('', '_bitrix')
    )

    df_register['№ задачи Битрикс'] = df_register['№ задачи Битрикс'].replace('', pd.NA)
    df_register = df_register.copy()
    mask = df_register['№ задачи Битрикс'].isna() & df_merged['№ задачи Битрикс_bitrix'].notna()
    df_register.loc[mask, '№ задачи Битрикс'] = df_merged.loc[mask, '№ задачи Битрикс_bitrix']
    df_register['№ задачи Битрикс'] = df_register['№ задачи Битрикс'].fillna('')

    updated_count = mask.sum()
    if updated_count > 0:
        logger.success(f"Добавлено {updated_count} номеров задач из Bitrix")

    return df_register


def replace_nan_in_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Очистка NaN значений в DataFrame")
    df_clean = df.copy()
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            df_clean[col] = df_clean[col].fillna("")
        else:
            # df_clean[col] = df_clean[col].fillna(0)
            pass

    logger.success("Очистка NaN завершена")
    
    return df_clean

def run_pipeline(directory_path: str) -> str:
    """
    Обновляет данные из выписки из Bitrix в реестре АХЧ.
    Возвращает строку-результат для Telegram-бота.
    """
    logger.info("Запуск пайплайна обработки данных")
    
    try:
        logger.info(f"Загрузка данных из: {directory_path}")
        df_bitrix = load_bitrix(directory_path)
        df_register, output_file_path = load_ahx_data(directory_path)      

        df_bitrix_clean = prepare_bitrix(df_bitrix)
        df_register_clean = prepare_register(df_register)
        df_bitrix_prepared = preprocess_bitrix_data(df_bitrix_clean)
        
        logger.info(f"Обновление реестра: {output_file_path}")

        df_register_filled = fill_invoice_numbers_from_bitrix(df_register_clean, df_bitrix_prepared)
        df_register_updated = update_register_payment_status_bitrix(df_register_filled, df_bitrix_prepared)
        df_register_updated = update_register_id_payment_object_bitrix(df_register_updated, df_bitrix_prepared)
        df_register_updated  = update_date_payment(df_register_updated)
        df_register_final = update_register_id_task_bitrix(df_register_updated, df_bitrix_prepared)

        df_register_updated = replace_nan_in_dataframe(df_register_final)

        success = update_excel_file(output_file_path, df_register_updated)

        if success:
            logger.success("Данные из Bitrix успешно обновлены в Реестре АХЧ")
            return "✅ Данные из Bitrix успешно обновлены в Реестре АХЧ."
        else:
            logger.error(f"Ошибка при сохранении: {success}")
            return "❌ Ошибка при сохранении файла."

    except FileNotFoundError as e:
        logger.error(f"Файл не найден: {e}")
        return str(e)
    except Exception as e:
        logger.error(f"Ошибка при обработке: {str(e)}")
        return f"❌ Ошибка при обработке: {str(e)}"


# Настройка логирования
logger.remove()
logger.add("Bitrix_import.log", rotation="10 MB", level="INFO", encoding="utf-8")
logger.add(lambda msg: print(msg, end=''), level="INFO")

# --- Для теста (не обязательно) ---
if __name__ == "__main__":
    directory_path = r"C:\Users\Юрий Кистенев\Desktop\ACH_manager\record"
#    directory_path = r"C:\Users\Юрий Кистенев\Desktop\ACH_manager\record"
    result = run_pipeline(directory_path)
    print(result)