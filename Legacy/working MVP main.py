import os
import io
import json
import logging
import datetime
import asyncio
from dotenv import load_dotenv

# Telegram Imports
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters

# Google Imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google import genai
from google.genai import types
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

import base64
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# Завантаження змінних середовища (якщо використовуєте .env)
load_dotenv()

# --- КОНФІГУРАЦІЯ ---
# Шлях до вашого файлу ключів від Service Account
CLIENT_SECRET_FILE = 'client_secret.json'
TEMPLATES_FOLDER_ID = '1Yv-z0Vbf_QJNFbO2O0k66UH-wij-x_8t'
SPREADSHEET_ID = "1_V9ENBqnuHm3213e7a5CBbE8_xXbFiW_c1uYXaKCboE"

TARGET_PRINT_EMAIL = "sviatoslav.bilous@ukd.edu.ua"

TELEGRAM_TOKEN="8275397295:AAHskfUiw8oKQUbWqNRa9JK0eMIRtUW2jNQ"
GEMINI_API_KEY = "AIzaSyAJzz3N2S4j7vfXECdRkFg7XMktx7HpkcE"

SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/documents',
    'https://www.googleapis.com/auth/spreadsheets.readonly',
    'https://www.googleapis.com/auth/gmail.send'
]

GEMINI_AI_MODEL = "gemini-3-flash-preview"

class DriveManager:
    """Оновлений клас з авторизацією через OAuth (від імені людини)"""

    """Клас для роботи з Google Drive/Docs (Авторизація + PDF)"""

    def __init__(self):
        self.creds = None
        if os.path.exists('token.json'):
            self.creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                # Увага: При першому запуску на сервері це може вимагати ручного втручання
                # Краще згенерувати token.json локально і залити на сервер
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
                self.creds = flow.run_local_server(port=0)
            with open('token.json', 'w') as token:
                token.write(self.creds.to_json())

        self.drive_service = build('drive', 'v3', credentials=self.creds)
        self.docs_service = build('docs', 'v1', credentials=self.creds)


    def get_available_templates(self):
        results = self.drive_service.files().list(
            q=f"'{TEMPLATES_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.document' and trashed=false",
            fields="files(id, name)"
        ).execute()
        return results.get('files', [])


    def create_pdf_from_template(self, template_id, template_name, data):
        # --- БЛОК АВТОМАТИЧНИХ ДАНИХ ---
        # 1. Дата підписання - завжди сьогодні
        data['date_of_signing'] = datetime.date.today().strftime("%d.%m.%Y")

        # 2. Якщо ШІ не вирахував курс, але є група - спробуємо "хак" (для КІПЗ-25 тощо)
        # Це опціонально, але корисно для ваших груп
        if 'group' in data and ('study_year' not in data or not data['study_year']):
            import re
            # Шукаємо дві цифри у назві групи (наприклад 25 з КІПЗ-25)
            match = re.search(r'-(\d{2})', data['group'])
            if match:
                entry_year = 2000 + int(match.group(1))
                current_year = datetime.date.today().year
                current_month = datetime.date.today().month
                # Якщо зараз вересень-грудень, то курс = поточному - вступ + 1
                # Якщо січень-червень, то курс = поточному - вступ
                course = current_year - entry_year + (1 if current_month >= 9 else 0)
                if course > 0:
                    data['study_year'] = str(course)

        # --- СТАНДАРТНА ЛОГІКА ---
        doc_id = None
        try:
            copy_body = {'name': f"TEMP_{template_name}", 'parents': [TEMPLATES_FOLDER_ID]}
            file_copy = self.drive_service.files().copy(fileId=template_id, body=copy_body).execute()
            doc_id = file_copy['id']

            requests = []
            for key, value in data.items():
                safe_value = str(value) if value is not None else ""

                # ТУТ ГОЛОВНА МАГІЯ:
                # Ми перебираємо всі можливі варіанти написання змінної в шаблоні:
                # {{key}}, {{KEY}}, {{Key}}
                # Це гарантує, що DATE_FROM заповниться навіть якщо прийшло date_from

                variants = [key.lower(), key.upper(), key.capitalize()]

                for var in variants:
                    requests.append({
                        'replaceAllText': {
                            'containsText': {'text': f'{{{{{var}}}}}', 'matchCase': True},
                            # matchCase True, бо ми самі перебираємо варіанти
                            'replaceText': safe_value
                        }
                    })

            if requests:
                self.docs_service.documents().batchUpdate(documentId=doc_id, body={'requests': requests}).execute()

            request = self.drive_service.files().export_media(fileId=doc_id, mimeType='application/pdf')
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()

            return fh.getvalue()

        except Exception as e:
            print(f"⚠️ API Error: {e}")
            raise e
        finally:
            if doc_id:
                try:
                    self.drive_service.files().delete(fileId=doc_id).execute()
                except:
                    pass


class GeminiBrain:
    def __init__(self):
        self.client = genai.Client(api_key=GEMINI_API_KEY)
        self.model = GEMINI_AI_MODEL

    def analyze_dialog_turn(self, history, template_list, known_data=None, current_active_template=None):
        if known_data is None: known_data = {}

        templates_str = "\n".join([f"- {t['name']}" for t in template_list])
        current_date_obj = datetime.date.today()
        current_date_str = datetime.date.today().strftime("%d.%m.%Y")
        # Обчислюємо поточний навчальний рік для підказки (наприклад, 2025/2026)
        # Якщо зараз серпень-грудень, то рік початку = поточний. Якщо січень-липень = поточний - 1.
        if current_date_obj.month >= 8:
            academic_year_start = current_date_obj.year
        else:
            academic_year_start = current_date_obj.year - 1

        # Формуємо рядок з відомими даними для промпта
        known_data_str = json.dumps(known_data, ensure_ascii=False, indent=2)
        chat_context = "\n".join([f"{msg['role']}: {msg['content']}" for msg in history])

        active_context_instruction = ""
        if current_active_template:
            active_context_instruction = f"""
            ВАЖЛИВО: Користувач вже вибрав шаблон: "{current_active_template}".
            Твоє єдине завдання зараз - зібрати всі необхідні дані для цього шаблону.
            """

        response_schema = {
            "type": "OBJECT",
            "properties": {
                "status": {
                    "type": "STRING",
                    "enum": ["CLARIFICATION_NEEDED", "TEMPLATE_SELECTED", "READY_TO_GENERATE"]
                },
                "bot_reply": {"type": "STRING"},
                "selected_template_name": {"type": "STRING", "nullable": True},
                "extracted_data": {
                    "type": "OBJECT",
                    "properties": {
                        # --- ОСОБИСТІ ДАНІ ---
                        "STUDENTS_NAME": {"type": "STRING", "description": "ПІБ студента у називному відмінку"},
                        "STUDENTS_EMAIL": {"type": "STRING"},
                        "DATE_OF_BIRTH": {"type": "STRING", "description": "Формат DD.MM.YYYY"},
                        "SPECIALITY": {"type": "STRING"},
                        "GROUP": {"type": "STRING"},
                        "STUDENTS_NUMBER": {"type": "STRING", "description": "Телефон студента"},

                        # --- ДАНІ БАТЬКІВ (для неповнолітніх або сімейних обставин) ---
                        "PARENTS_NAME": {"type": "STRING", "description": "ПІБ батька/матері/опікуна"},
                        "PARENTS_NUMBER": {"type": "STRING", "description": "Телефон батьків"},

                        # --- ДЕТАЛІ ЗАЯВИ ---
                        "DATE_FROM": {"type": "STRING", "description": "Дата початку (DD.MM.YYYY)"},
                        "DATE_TO": {"type": "STRING", "description": "Дата завершення (DD.MM.YYYY)"},
                        "REASON": {"type": "STRING", "description": "Причина (для академвідпустки, пропуску тощо)"},
                        "SPECIALITY_TO": {"type": "STRING",
                                          "description": "На яку спеціальність переводиться (тільки для переведення)"},
                        "SUBJECT": {"type": "STRING", "description": "Назва дисципліни (для перездачі/академрізниці)"},

                        # --- АВТОМАТИЧНІ/ОБЧИСЛЮВАНІ ПОЛЯ ---
                        "DATE_OF_SIGNING": {"type": "STRING", "description": "Поточна дата"},
                        "STUDY_YEAR": {"type": "STRING", "description": "Курс (цифрою: 1, 2, 3...)"},
                        "STUDY_SEMESTER": {"type": "STRING", "description": "Семестр (цифрою: 1 або 2)"},
                    },
                    "nullable": True
                }
            },
            "required": ["status", "bot_reply"]
        }

        prompt = f"""
        Ти адміністративний бот Фахового коледжу Університету Короля Данила. Мета: зібрати дані та згенерувати PDF-файл заяви або довідки.

        СИСТЕМНІ ДАНІ:
        Дата: {current_date_str}
        
        --> БАЗА ДАНИХ СТУДЕНТА (ВЖЕ ВІДОМО, НЕ ПИТАЙ ЦЕ):
        {known_data_str}
        <--

        ДОСТУПНІ ШАБЛОНИ:
        {templates_str}

        {active_context_instruction}

        ІСТОРІЯ:
        {chat_context}
        
ІНСТРУКЦІЯ З ОБРОБКИ ДАНИХ (LOGIC):

При реєстрації студент вказав дані, які використовуються у кожній (або майже кожній) заяві: 
   - **STUDENTS_NAME**: Вимагай повне ПІБ (Прізвище Ім'я По батькові). Якщо вказано лише Ім'я -> CLARIFICATION_NEEDED. Автоматично виправляй регістр (перша літера велика).
   - **GROUP**: Записуй у форматі, "Коледж (буква К на початку), спеціальність (ІПЗ у цьому прикладі), форма навчання (с - стаціонар, з - заочна форма, д - індивідуальний графік), рік вступу (25, 26 тощо), номер групи (якщо є кілька груп) як подав студент (напр. КІПЗс-25-1).
   - **SPECIALITY**: Повна назва спеціальності (напр. "Інженерія програмного забезпечення", а не просто "програмування").
   - **DATE_OF_BIRTH**: Формат DD.MM.YYYY.
   - **STUDENTS_EMAIL**: Перевір наявність домену "@ukd.edu.ua".
   - **STUDENTS_NUMBER**: Телефон студента.
   - **PARENTS_NUMBER**: Телефон батьків. 

Пріоритет даних - інформація з бази даних студентів
- Якщо в базі даних поле порожнє або відсутнє, шукай його в історії чату.
- Якщо даних немає ніде - питай користувача (статус CLARIFICATION_NEEDED).

- Поля типу DATE_FROM, DATE_TO, REASON (причина) зазвичай змінюються для кожної нової заяви. Причина має бути коректно лінгвістично вказана у заяві (наприклад, "у зв'язку з сімейними обставинами", а не "у зв'язку з сімейні обставини"). 
- Навіть якщо вони є в базі, краще перевір контекст: якщо користувач в чаті вказав нові дати - використовуй нові. Якщо в чаті дат немає - питай.

1. АВТОМАТИЧНІ ПОЛЯ (Ніколи не питай про них користувача, обчислюй сам):
   - **DATE_OF_SIGNING**: Завжди дорівнює поточній даті: {current_date_str}.
   - **STUDY_YEAR (Курс)**: 
     * Алгоритм: Знайди рік вступу в назві групи (наприклад, у "КІПЗс-25-1" рік вступу - 2025). 
     * Формула: ({academic_year_start} - [Рік вступу з назви групи]) + 1.
     * Приклад: Зараз 2026. Група '25' -> (2025-2025)+1 = 1 курс. Група '24' -> 2 курс.
   - **STUDY_SEMESTER**: 
     * Якщо поточний місяць від 09 (вересень) до 12 (грудень) або 01 (січень) -> Семестр "1".
     * Якщо поточний місяць від 02 (лютий) до 06 (червень) -> Семестр "2".

2. ДАТИ ТА ПРИЧИНИ (Context-Specific):
   - **DATE_FROM** та **DATE_TO**: Вимагай точні дати. Перетворюй фрази типу "на наступний тиждень" у конкретні дати формату DD.MM.YYYY.
   - **REASON**: Короткий, чіткий опис причини (наприклад ...у зв'язку із "сімейними обставинами", "поїздкою на змагання", "виїздом закордон" тощо). Не пиши "тому що..." на початку.

3. СПЕЦИФІЧНІ ПОЛЯ (Тільки для відповідних шаблонів):
   - **SPECIALITY_TO**: Заповнюй ТІЛЬКИ для заяви на переведення. Назва спеціальності, КУДИ хоче перевестись студент.
   - **SUBJECT**: Заповнюй ТІЛЬКИ для заяв на перездачу, академрізницю або повторне вивчення. Вказуй назву предмету.
   - **PARENTS_NAME**: Обов'язково для студентів до 18 років - з'ясовуй їх вік зі змінної "DATE_OF_BIRTH".

ВАЖЛИВО: Якщо для обраного шаблону (active_template) не вистачає даних для обов'язкових полів -> повертай статус CLARIFICATION_NEEDED і перелік питань.
        
ЛОГІКА:
1. Якщо шаблон НЕ обрано (бракує хоча б однієї змінної) -> CLARIFICATION_NEEDED (пропонуй варіанти) або TEMPLATE_SELECTED (якщо користувач погодився).
2. Якщо шаблон ОБРАНО -> Перевір наявність усіх даних. Якщо є всі (включно з вирахуваними автоматично) -> READY_TO_GENERATE. Якщо ні -> CLARIFICATION_NEEDED (питай).
3. READY_TO_GENERATE став ТІЛЬКИ коли маєш повний набір даних.

Відповідай JSON.
"""

        response = self.client.models.generate_content(
            model=self.model,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=response_schema
            )
        )
        try:
            return json.loads(response.text)
        except:
            return {"status": "CLARIFICATION_NEEDED", "bot_reply": "Помилка. Повторіть."}


class SheetManager:
    def __init__(self, credentials, spreadsheet_id):
        self.service = build('sheets', 'v4', credentials=credentials)
        self.spreadsheet_id = spreadsheet_id

    def get_student_data(self, telegram_id):
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id, range="Students!A:Z"
            ).execute()
            rows = result.get('values', [])
            if not rows: return {}

            headers = rows[0]
            # Шукаємо індекс стовпця Telegram_ID
            try:
                tg_id_idx = next(i for i, h in enumerate(headers) if h.strip() == 'Telegram_ID')
            except StopIteration:
                return {}

            for row in rows[1:]:
                # Перевіряємо чи ID співпадає (як рядки)
                if len(row) > tg_id_idx and str(row[tg_id_idx]).strip() == str(telegram_id):
                    student_data = {}
                    for i, header in enumerate(headers):
                        val = row[i] if i < len(row) else ""
                        if val.strip():
                            student_data[header.strip()] = val.strip()
                    return student_data
            return {}

        except Exception as e:
            print(f"Google Sheets API Error: {e}")
            return {}


class EmailManager:
    def __init__(self, credentials):
        # Підключаємося до Gmail API
        self.service = build('gmail', 'v1', credentials=credentials)

    def send_email(self, to_email, subject, body, file_bytes_io, filename):
        """
        Відправляє лист з вкладенням.
        file_bytes_io: об'єкт BytesIO з PDF-файлом
        """
        try:
            # Створюємо повідомлення
            message = MIMEMultipart()
            message['to'] = to_email
            message['subject'] = subject

            # Тіло листа
            message.attach(MIMEText(body, 'plain'))

            # Вкладення (PDF)
            file_bytes_io.seek(0)  # Переконуємось, що курсор на початку
            part = MIMEApplication(file_bytes_io.read(), Name=filename)
            part['Content-Disposition'] = f'attachment; filename="{filename}"'
            message.attach(part)

            # Повертаємо курсор назад, щоб інші функції теж могли читати файл
            file_bytes_io.seek(0)

            # Кодування для Gmail API
            raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
            create_message = {'raw': raw_message}

            # Відправка
            self.service.users().messages().send(userId="me", body=create_message).execute()
            print(f"📧 Лист успішно відправлено на {to_email}")
            return True
        except Exception as e:
            print(f"❌ Помилка відправки пошти: {e}")
            return False


drive_mgr = DriveManager()
sheet_mgr = SheetManager(drive_mgr.creds, SPREADSHEET_ID)
email_mgr = EmailManager(drive_mgr.creds)
brain = GeminiBrain()
user_sessions = {}


async def main():
    # Цей рядок запустить процес авторизації при першому старті


    print("📥 Завантажую шаблони...")
    templates = drive_mgr.get_available_templates()
    print(f"✅ Готово. Знайдено {len(templates)} шт.")

    # --- СИМУЛЯЦІЯ КОРИСТУВАННЯ TELEGRAM ---
    print("\n🔐 Симуляція входу через Telegram.")
    tg_id_input = input("Введіть ваш Telegram ID (наприклад, 12345): ")

    # 4. Отримуємо дані студента ОДИН РАЗ при старті
    student_profile = sheet_mgr.get_student_data(tg_id_input)

    if student_profile:
        print(f"👋 Вітаю, {student_profile.get('STUDENTS_NAME')}! Я підтягнув ваші дані.")
    else:
        print("👋 Вітаю! Я вас не знайшов у базі, тому буду питати все вручну.")
    # --- --- --- --- --- --- --- --- --- --- ---

    chat_history = []
    active_template = None

    print("\n💬 Бот: Привіт! Я - ШІ-бот Фахового коледжу УКД. Яку заяву чи довідку вам потрібно оформити?")

    while True:
        user_input = input("\n👤 Студент: ")
        if not user_input: continue
        if user_input.lower() in ["exit", "вихід"]: break

        chat_history.append({"role": "user", "content": user_input})

        print("   (думаю...)")
        analysis = brain.analyze_dialog_turn(chat_history, templates, known_data=student_profile,
                                             current_active_template=active_template)
        status = analysis.get("status")
        reply = analysis.get("bot_reply")
        data = analysis.get("extracted_data", {})

        chat_history.append({"role": "model", "content": reply})
        print(f"🤖 Бот: {reply}")

        if status == "TEMPLATE_SELECTED":
            new_template = analysis.get("selected_template_name")
            if any(t['name'] == new_template for t in templates):
                active_template = new_template
                print(f"   [System] Шаблон зафіксовано: {active_template}")


        elif status == "READY_TO_GENERATE":
            # 1. СПРОБА ПОРЯТУНКУ: Якщо змінна пуста, шукаємо назву прямо у відповіді ШІ
            if not active_template:
                candidate = analysis.get("selected_template_name")
                if candidate:
                    # Перевіряємо, чи такий файл реально існує
                    if any(t['name'] == candidate for t in templates):
                        active_template = candidate
                        print(f"   [System Auto-Fix] Шаблон визначено автоматично: {active_template}")
            # 2. Тепер пробуємо генерувати
            if active_template:
                print(f"\n⚙️ Генерація PDF для '{active_template}'...")
                # Знаходимо об'єкт файлу за назвою
                template_obj = next((t for t in templates if t['name'] == active_template), None)
                if template_obj:
                    try:
                        pdf_bytes = drive_mgr.create_pdf_from_template(template_obj['id'], active_template, data)
                        filename = f"Заява_{data.get('students_name', 'Student').replace(' ', '_')}.pdf"
                        # Зберігаємо
                        with open(filename, "wb") as f:
                            f.write(pdf_bytes)
                        print(f"✅ ВАШ ФАЙЛ ГОТОВИЙ: {os.path.abspath(filename)}")
                        print("🤖 Бот: Документ створено! (Скидання сесії...)")
                        # Скидання
                        chat_history = []
                        active_template = None
                    except Exception as e:
                        print(f"❌ Помилка API: {e}")
                else:
                    print(f"❌ Помилка: Файл '{active_template}' не знайдено у списку Drive.")
            else:
                # Якщо навіть ШІ не повернув назву в selected_template_name
                print("❌ Помилка: ШІ каже, що готовий, але не вказав назву шаблону. Спробуйте ще раз.")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    user_id = str(user.id)
    chat_id = update.effective_chat.id

    # Скидаємо/створюємо сесію
    user_sessions[user_id] = {
        'history': [],
        'active_template': None,
        'profile': {}
    }

    # Запускаємо таймер при старті
    reset_timeout_timer(user_id, chat_id, context)

    # Перевіряємо базу даних
    await update.message.reply_text("🔎 Шукаю вас у базі студентів...")
    profile = sheet_mgr.get_student_data(user_id)

    if profile:
        user_sessions[user_id]['profile'] = profile
        name = profile.get('STUDENTS_NAME', user.first_name)
        await update.message.reply_text(f"👋 Вітаю, {name}! Ваші дані завантажено. Яку заяву оформлюємо?")
    else:
        await update.message.reply_text(
            f"👋 Вітаю! Я вас не знайшов у базі (ID: {user_id}). Буду питати дані вручну.\nЩо бажаєте оформити?")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    chat_id = update.effective_chat.id
    text = update.message.text

    # Якщо користувач пише вперше без /start
    if user_id not in user_sessions:
        await start(update, context)
        return

    # 🕒 ОНОВЛЮЄМО ТАЙМЕР ПРИ КОЖНОМУ ПОВІДОМЛЕННІ
    reset_timeout_timer(user_id, chat_id, context)

    session = user_sessions[user_id]

    # 1. Додаємо повідомлення користувача в історію
    session['history'].append({"role": "user", "content": text})

    # Індикатор "друкує..."
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

    # 2. Аналіз через Gemini
    templates = drive_mgr.get_available_templates()

    # Виклик мозку (це синхронна функція, тому в high-load проєктах її треба робити асинхронною,
    # але для коледжу так ок)
    try:
        analysis = brain.analyze_dialog_turn(
            history=session['history'],
            template_list=templates,
            known_data=session['profile'],
            current_active_template=session['active_template']
        )
    except Exception as e:
        await update.message.reply_text("😵 Виникла помилка при обробці. Спробуйте ще раз.")
        print(f"Brain Error: {e}")
        return

    status = analysis.get("status")
    reply = analysis.get("bot_reply")
    data = analysis.get("extracted_data", {})

    # 3. Відповідь бота
    session['history'].append({"role": "model", "content": reply})
    await update.message.reply_text(reply)

    # 4. Логіка статусів
    if status == "TEMPLATE_SELECTED":
        session['active_template'] = analysis.get("selected_template_name")

    elif status == "READY_TO_GENERATE":
        # Спроба визначити шаблон, якщо він не був явно вибраний раніше (авто-фікс)
        current_tmpl_name = session['active_template']
        if not current_tmpl_name:
            candidate = analysis.get("selected_template_name")
            if candidate and any(t['name'] == candidate for t in templates):
                current_tmpl_name = candidate

        if current_tmpl_name:
            await update.message.reply_text("⚙️ Генерую документ, зачекайте хвилинку...")
            await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="upload_document")

            tmpl_obj = next((t for t in templates if t['name'] == current_tmpl_name), None)

            if tmpl_obj:
                try:
                    # 1. Генерація PDF
                    raw_pdf = drive_mgr.create_pdf_from_template(tmpl_obj['id'], current_tmpl_name, data)

                    if isinstance(raw_pdf, bytes):
                        pdf_file = io.BytesIO(raw_pdf)
                    else:
                        pdf_file = raw_pdf

                    pdf_file.seek(0)

                    student_name = data.get('STUDENTS_NAME', 'Student')
                    clean_name = student_name.replace(' ', '_')
                    filename = f"Заява_{clean_name}.pdf"
                    pdf_file.name = filename

                    # 2. Відправка студенту в Telegram
                    await update.message.reply_document(
                        document=pdf_file,
                        filename=filename,
                        caption="Ваш документ готовий! 📄\nКопію також надіслано до друку."
                    )

                    # 3. ВІДПРАВКА НА ПОШТУ (НОВЕ)
                    email_subject = f"ДРУК: Заява - {student_name} ({data.get('GROUP', '')})"
                    email_body = f"""
                                Доброго дня.

                                Надійшла нова заява від студента для друку.

                                Студент: {student_name}
                                Група: {data.get('GROUP', 'Не вказано')}
                                Тип заяви: {current_tmpl_name}

                                Файл у вкладенні.
                                --
                                Згенеровано автоматично через UKD College Bot
                                """

                    # Викликаємо відправку
                    # Важливо: pdf_file передаємо той самий
                    email_mgr.send_email(
                        to_email=TARGET_PRINT_EMAIL,
                        subject=email_subject,
                        body=email_body,
                        file_bytes_io=pdf_file,
                        filename=filename
                    )

                    # Очищаємо історію після успіху
                    session['history'] = []
                    session['active_template'] = None
                    await update.message.reply_text("Готовий до нових завдань. Щось ще?")

                except Exception as e:
                    # Виводимо помилку і в чат, і в консоль
                    print(f"ERROR DETAILS: {e}")
                    await update.message.reply_text(f"❌ Помилка генерації: {e}")
            else:
                await update.message.reply_text("❌ Помилка: шаблон не знайдено.")
        else:
            await update.message.reply_text("⚠️ ШІ готовий, але не вказав шаблон. Спробуйте уточнити назву заяви.")


async def session_timeout_callback(context: ContextTypes.DEFAULT_TYPE):
    """Ця функція запускається автоматично через 1 годину бездіяльності"""
    job = context.job
    user_id = job.data  # Ми передали ID користувача в job.data
    chat_id = job.chat_id

    # Перевіряємо, чи є ще сесія
    if user_id in user_sessions:
        # Очищаємо історію та шаблон, але залишаємо профіль (щоб не шукати в базі знову)
        user_sessions[user_id]['history'] = []
        user_sessions[user_id]['active_template'] = None

        print(f"⌛ Сесію користувача {user_id} закрито через неактивність.")

        # Надсилаємо прощальне повідомлення
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text="⏳ Час сесії вичерпано (1 година бездіяльності).\nДякую за взаємодію! Історія діалогу очищена.\nЯкщо знадобиться нова заява — просто напишіть мені."
            )
        except Exception as e:
            print(f"Не вдалося надіслати повідомлення про тайм-аут: {e}")


def reset_timeout_timer(user_id, chat_id, context: ContextTypes.DEFAULT_TYPE):
    """Скидає старий таймер і запускає новий на 1 годину"""
    # 1. Видаляємо старі завдання для цього юзера
    current_jobs = context.job_queue.get_jobs_by_name(str(user_id))
    for job in current_jobs:
        job.schedule_removal()

    # 2. Ставимо нове завдання
    context.job_queue.run_once(
        session_timeout_callback,
        4800,  # 4800 секунд = 1 академічна пара
        chat_id=chat_id,
        name=str(user_id),
        data=user_id
    )


if __name__ == "__main__":
    print("🤖 Бот запускається з підтримкою JobQueue...")

    # JobQueue підключається автоматично в ApplicationBuilder
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))

    print("✅ Бот слухає повідомлення...")
    app.run_polling()