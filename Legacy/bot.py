import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters, ConversationHandler
from docxtpl import DocxTemplate
import os
from datetime import datetime


# --- КОНФІГУРАЦІЯ ---
TOKEN = "8275397295:AAHskfUiw8oKQUbWqNRa9JK0eMIRtUW2jNQ"  # <--- Вставте свій токен сюди
TEMPLATE_FILE = "Заява про дозвіл на виїзд закордон в навчальний період.docx"
SHEET_NAME = "StudentsDB" # Назва вашої таблиці
CREDENTIALS_FILE = "service_account.json"

# СТАНИ ДІАЛОГУ
CHOOSING_DOC, ASK_DATE_FROM, ASK_DATE_TO = range(3)

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# --- ДОПОМІЖНІ ФУНКЦІЇ ---

def calculate_age(birth_date_str):
    try:
        if not birth_date_str: return 0
        birth_date = datetime.strptime(birth_date_str, "%d.%m.%Y")
        today = datetime.now()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        return age
    except ValueError:
        return 0

def validate_date_format(date_text):
    try:
        valid_date = datetime.strptime(date_text, "%d.%m.%Y")
        return valid_date
    except ValueError:
        return None

# --- РОБОТА З ТАБЛИЦЕЮ (ОНОВЛЕНО) ---
def get_google_sheet():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME).sheet1

def get_student_record(search_col_name, search_value):
    """
    Універсальна функція пошуку.
    Повертає словник {Назва_Колонки: Значення}, а не просто список.
    """
    sheet = get_google_sheet()
    try:
        # 1. Отримуємо заголовки (перший рядок)
        headers = sheet.row_values(1)
        
        # 2. Знаходимо індекс колонки, по якій шукаємо (наприклад, Telegram_ID)
        try:
            col_index = headers.index(search_col_name) + 1 # gspread рахує з 1
        except ValueError:
            logging.error(f"Колонку {search_col_name} не знайдено в таблиці!")
            return None

        # 3. Шукаємо клітинку зі значенням
        cell = sheet.find(str(search_value), in_column=col_index)
        if cell is None: 
            return None

        # 4. Отримуємо весь рядок
        row_values = sheet.row_values(cell.row)

        # 5. "Зшиваємо" заголовки і значення в словник
        # Якщо рядок коротший за заголовки (пусті клітинки в кінці), додаємо пусті стрічки
        while len(row_values) < len(headers):
            row_values.append("")
            
        return dict(zip(headers, row_values))

    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return None

def update_telegram_id(phone_number, tg_id):
    """
    Знаходить студента за телефоном і записує йому Telegram_ID
    """
    sheet = get_google_sheet()
    clean_phone = phone_number.replace('+', '')
    
    try:
        headers = sheet.row_values(1)
        
        # Динамічно шукаємо номери колонок
        phone_col_idx = headers.index("STUDENTS_NUMBER") + 1
        id_col_idx = headers.index("Telegram_ID") + 1
        
        cell = sheet.find(clean_phone, in_column=phone_col_idx)
        
        if cell is None: 
            return None

        # Записуємо ID
        sheet.update_cell(cell.row, id_col_idx, str(tg_id))
        
        # Повертаємо оновлені дані (викликаємо функцію читання, щоб отримати словник)
        return get_student_record("Telegram_ID", tg_id)
        
    except Exception as e:
        logging.error(f"Error updating ID: {e}")
        return None

# --- ЛОГІКА БОТА ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    # Шукаємо тепер не за індексом, а за назвою колонки
    student = get_student_record("Telegram_ID", user.id)

    if student:
        # Зберігаємо весь словник студента в пам'ять
        context.user_data['student_info'] = student
        await show_doc_menu(update, context)
        return CHOOSING_DOC
    else:
        keyboard = [[KeyboardButton("📱 Надіслати мій номер", request_contact=True)]]
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Привіт! Я не знаю вас. Натисніть кнопку, щоб авторизуватися.",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
        )
        return ConversationHandler.END

async def contact_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    contact = update.effective_message.contact
    # Оновлюємо ID і одразу отримуємо профіль
    student = update_telegram_id(contact.phone_number, update.effective_user.id)
    
    if student:
        context.user_data['student_info'] = student
        await show_doc_menu(update, context)
        return CHOOSING_DOC
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id, 
            text="Ваш номер телефону не знайдено в базі (шукав у колонці STUDENTS_NUMBER)."
        )
        return ConversationHandler.END

async def show_doc_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Звертаємося по ключу!
    name = context.user_data['student_info'].get('STUDENTS_NAME', 'Студент')
    
    keyboard = [["✈️ Заява на виїзд закордон"], ["❌ Скасувати"]]
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"Вітаю, {name}! Оберіть документ:",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=True)
    )

# --- ВАЛІДАЦІЯ ТА ЛОГІКА ДАТ (Без змін) ---

async def ask_date_from(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == "❌ Скасувати": return await cancel(update, context)

    if "Заява на виїзд закордон" in text:
        await update.message.reply_text(
            "Введіть дату ПОЧАТКУ (формат ДД.ММ.РРРР):", 
            reply_markup=ReplyKeyboardRemove()
        )
        return ASK_DATE_FROM
    return CHOOSING_DOC

async def ask_date_to(update: Update, context: ContextTypes.DEFAULT_TYPE):
    date_text = update.message.text
    valid_date = validate_date_format(date_text)
    if not valid_date:
        await update.message.reply_text("⚠️ Невірний формат! Спробуйте ще раз (ДД.ММ.РРРР):")
        return ASK_DATE_FROM 
    
    context.user_data['DATE_FROM_OBJ'] = valid_date
    context.user_data['DATE_FROM'] = date_text
    
    await update.message.reply_text("Введіть дату ЗАВЕРШЕННЯ (ДД.ММ.РРРР):")
    return ASK_DATE_TO

async def generate_zayava(update: Update, context: ContextTypes.DEFAULT_TYPE):
    date_text = update.message.text
    
    valid_date_to = validate_date_format(date_text)
    if not valid_date_to:
        await update.message.reply_text("⚠️ Невірний формат! Введіть ще раз (ДД.ММ.РРРР):")
        return ASK_DATE_TO

    date_from_obj = context.user_data['DATE_FROM_OBJ']
    if valid_date_to < date_from_obj:
        await update.message.reply_text("⛔️ Дата завершення не може бути раніше дати початку! Введіть ще раз:")
        return ASK_DATE_TO

    context.user_data['DATE_TO'] = date_text
    await update.message.reply_text("⏳ Генерую документ...")

    # --- ПІДГОТОВКА ДАНИХ ---
    data = context.user_data['student_info']
    today_date = datetime.now().strftime("%d.%m.%Y")
    
    # Використовуємо .get() для безпеки, якщо клітинка пуста
    dob = data.get('DATE_OF_BIRTH', '')
    age = calculate_age(dob)
    is_under_18 = age < 18

    # Формуємо контекст (можна просто передати весь data, але краще явно задати змінні)
    # Зверніть увагу: ми просто беремо ключі, які збігаються з назвами колонок
    context_vars = {
        **data, # Розпаковуємо всі поля з бази (STUDENTS_NAME, SPECIALITY і т.д.)
        'DATE_FROM': context.user_data['DATE_FROM'],
        'DATE_TO': context.user_data['DATE_TO'],
        'DATE_OF_SIGNING': today_date,
        'under_18': is_under_18
    }

    try:
        doc = DocxTemplate(TEMPLATE_FILE)
        doc.render(context_vars)
        
        # Генеруємо ім'я файлу
        s_name = data.get('STUDENTS_NAME', 'Student').split()[0]
        filename = f"Zayava_{s_name}.docx"
        doc.save(filename)
        
        caption_text = "✅ Заява готова!"
        caption_text += "\n⚠️ Потрібен підпис батьків." if is_under_18 else "\n🔞 Ви повнолітній."

        await context.bot.send_document(
            chat_id=update.effective_chat.id, 
            document=open(filename, 'rb'),
            caption=caption_text
        )
        os.remove(filename)

    except Exception as e:
        await update.message.reply_text(f"❌ Помилка: {e}")
        print(e)

    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Скасовано. /start", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END

if __name__ == '__main__':
    app = ApplicationBuilder().token(TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            CHOOSING_DOC: [MessageHandler(filters.TEXT, ask_date_from)],
            ASK_DATE_FROM: [MessageHandler(filters.TEXT, ask_date_to)],
            ASK_DATE_TO: [MessageHandler(filters.TEXT, generate_zayava)],
        },
        fallbacks=[CommandHandler('cancel', cancel)]
    )

    app.add_handler(MessageHandler(filters.CONTACT, contact_handler))
    app.add_handler(conv_handler)

    print("Бот (версія з назвами колонок) запущено...")
    app.run_polling()