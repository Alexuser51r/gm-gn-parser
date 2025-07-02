from telethon import TelegramClient
import logging
import os
import time
import pandas as pd

# Настройки для Telegram API
api_id = ""
api_hash = ""
session_file = "anon.session"
downloaded_list_file = "downloaded_files.txt"
seen_reports_file = "reports_seen.txt"
uploaded_reports_file = "reports_uploaded.txt"

SOURCE_CHANNEL_ID = ""  # ID канала, откуда берём Excel
TARGET_CHANNEL = ""       # Канал для отправки отчётов

# Папка для скачивания и выгрузки
DOWNLOAD_FOLDER = 'downloads/'
REPORTS_FOLDER = '/home/chromeuser/reports/'

# Создаём папки при необходимости
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

# Устанавливаем логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_log_set(file_path):
    return set(open(file_path).read().splitlines()) if os.path.exists(file_path) else set()

def append_to_log(file_path, entry):
    with open(file_path, 'a') as f:
        f.write(entry + '\n')

# Функция для загрузки Excel-файлов из Telegram
async def download_excel_files():
    chat = await client.get_entity(SOURCE_CHANNEL_ID)
    logger.info(f"Получаем сообщения из канала: {chat.title}")

    messages = await client.get_messages(chat, limit=100)
    downloaded_files = read_log_set(downloaded_list_file)

    # Отфильтровать только Excel-файлы (и не содержащие "migrated")
    excel_messages = []
    for message in messages:
        if message.document and message.document.mime_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            file_name = message.document.attributes[0].file_name
            if "migrated" not in file_name.lower():
                excel_messages.append((message, file_name))

    # Оставляем только 10 последних Excel-файлов
    last_10_excel = excel_messages[:10]

    for message, file_name in last_10_excel:
        if file_name in downloaded_files:
            logger.info(f"Файл уже загружен ранее: {file_name}")
            continue

        file_path = os.path.join(DOWNLOAD_FOLDER, file_name)
        await message.download_media(file_path)
        logger.info(f"Скачан новый файл: {file_name}")
        append_to_log(downloaded_list_file, file_name)

# Функция для выгрузки Excel-файлов из reports
async def process_reports_folder():
    current_files = sorted([
        f for f in os.listdir(REPORTS_FOLDER)
        if f.endswith('.xlsx')
    ])

    seen_files = read_log_set(seen_reports_file)
    uploaded_files = read_log_set(uploaded_reports_file)

    new_files = set(current_files) - seen_files
    pending_files = [f for f in current_files if f in seen_files and f not in uploaded_files]

    for f in new_files:
        append_to_log(seen_reports_file, f)

    if new_files and pending_files:
        file_to_send = pending_files[0]
        file_path = os.path.join(REPORTS_FOLDER, file_to_send)

        try:
            # Unicode-эмодзи (безопасны для UTF-8)
            package = "\U0001F4E6"  # ������
            rocket = "\U0001F680"   # ������
            fire = "\U0001F525"     # ������
            boom = "\U0001F4A5"     # ������

            # Загружаем Excel
            df = pd.read_excel(file_path, header=None)
            total_tokens = df.iloc[1, 2]  # C2
            max_rocket = df.iloc[1, 3]    # D2
            more_than_5x = df.iloc[1, 5]  # F2
            more_than_10x = df.iloc[1, 6] # G2

            # Обработка 0
            value_5x = 0 if str(more_than_5x).strip() == "0" else more_than_5x
            value_10x = 0 if str(more_than_10x).strip() == "0" else more_than_10x

            # Подпись
            caption = (
                f"{package} Total Tokens: {total_tokens}\n"
                f"{rocket} Max Rocket: {max_rocket}\n"
                f"{fire} >5x: {value_5x}\n"
                f"{boom} >10x: {value_10x}"
            )

            await client.send_file(TARGET_CHANNEL, file_path, caption=caption, force_document=True)
            append_to_log(uploaded_reports_file, file_to_send)
            os.remove(file_path)
            logger.info(f"Файл {file_to_send} отправлен и удалён.")
        except Exception as e:
            logger.error(f"Ошибка при отправке {file_to_send}: {e}")

# Основной запуск
with TelegramClient(session_file, api_id, api_hash,
                    device_model="PC",
                    system_version="Windows 10",
                    app_version="4.5.1",
                    lang_code="en") as client:
    logger.info("Подключено к Telegram")

    # Скачивание новых файлов при старте
    client.loop.run_until_complete(download_excel_files())

    # Цикл сканирования папки reports
    while True:
        try:
            # Пробуждение соединения, чтобы не терялось
            client.loop.run_until_complete(client.get_me())
            logger.debug("Пинг Telegram: соединение активно.")

            # Основная задача: проверка и выгрузка
            client.loop.run_until_complete(process_reports_folder())

        except Exception as e:
            logger.error(f"Ошибка в основном цикле: {e}")

        time.sleep(300)  # 5 минут пауза
