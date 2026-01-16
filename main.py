import time
import re
import os
import io
import json
import logging
import datetime
import base64
from datetime import timedelta
from logging.handlers import TimedRotatingFileHandler
from dotenv import load_dotenv

# Telegram Imports
from telegram import Update, BotCommand, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters, Application

# Google Imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google import genai
from google.genai import types
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Email Imports
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# Завантаження змінних
load_dotenv()

# Словник спеціальностей
SPECIALTIES_MAP = {
    "А": "Архітектура та містобудування",
    "Б": "Будівництво та цивільна інженерія",
    "ГРС": "Готельно-ресторанна справа та кейтеринг",
    "ДІ": "Дизайн інтер'єру",
    "Д": "Графічний дизайн",
    "ЕІ": "Електрична інженерія",
    "ІПЗ": "Інженерія програмного забезпечення",
    "Мн": "Менеджмент",
    "Мр": "Маркетинг",
    "М": "Музичне мистецтво",
    "О": "Облік та оподатування",
    "ПД": "Правоохоронна діяльність",
    "СМ": "Сценічне мистецтво",
    "Т": "Туризм і рекреація",
    "Ф": "Фінанси, банківська справа та страхування",
    "Х": "Хореографія",
    "Ю": "Юридичне діловодство і секретарська справа"
}

REQUIRED_FIELDS_MAP = {
    "DATE_OF_BIRTH": "📅 Введіть вашу дату народження (формат ДД.ММ.РРРР):",
    "STUDENTS_PHONE": "📱 Натисніть кнопку знизу, щоб надіслати свій номер телефону:",
    "PARENTS_NAME": "Введіть ПІБ одного з представників (наприклад, батьків, для заяв):",
    "PARENTS_PHONE": "📱 Введіть контактний телефон представника (вручну):"
}

# --- КОНФІГУРАЦІЯ ---
CLIENT_SECRET_FILE = os.getenv("CLIENT_SECRET_FILE")
TEMPLATES_FOLDER_ID = os.getenv("TEMPLATES_FOLDER_ID")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TARGET_PRINT_EMAIL = os.getenv("TARGET_PRINT_EMAIL")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_AI_MODEL = os.getenv("GEMINI_AI_MODEL")

SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/documents',
    'https://www.googleapis.com/auth/spreadsheets',  # Змінено на повний доступ для запису ID
    'https://www.googleapis.com/auth/gmail.send'
]

UI_MESSAGES = {
    # 🔴 КРИТИЧНІ ПОМИЛКИ (Система)
    "google_overload": (
        "😓 **Сервери Google зараз перевантажені.**\n"
        "Штучний інтелект бере коротку паузу. Будь ласка, спробуйте повторити запит через 30-60 секунд."
    ),
    "google_auth_error": (
        "🔑 **Проблема авторизації.**\n"
        "Я втратив доступ до дисків Google. Повідомте адміністратора, що потрібна ре-авторизація."
    ),
    "database_error": (
        "🗄️ **Помилка доступу до бази даних.**\n"
        "Не вдалося знайти ваші дані в таблиці студентів. Зверніться в 210 кабінет або спробуйте пізніше."
    ),
    "unknown_critical_error": (
        "🛠️ **Виникла технічна несправність.**\n"
        "Я вже зафіксував цю помилку і передав розробнику. Спробуйте, будь ласка, пізніше."
    ),

    # 🟠 ВАЛІДАЦІЯ (Користувач робить щось не так)
    "safety_filter_block": (
        "🛡️ **Спрацював фільтр безпеки.**\n"
        "Я не можу обробити цей запит через внутрішні політики контенту. Спробуйте сформулювати інакше."
    ),
    "missing_variables": (
        "✍️ **Не вистачає даних.**\n"
        "Для формування цього документа мені потрібно уточнити ще кілька деталей. Відповідайте на мої питання по черзі."
    ),
    "wrong_date_format": (
        "📅 **Некоректний формат дати.**\n"
        "Будь ласка, введіть дату у форматі: `ДД.ММ.РРРР` (наприклад, 15.01.2026)."
    ),
    "unknown_intent": (
        "🤔 **Я не зовсім зрозумів.**\n"
        "Я поки вчуся. Спробуйте перефразувати або скористайтеся командою /start для вибору дії."
    ),

    # 🟡 СТАТУСИ ТА РЕЖИМИ
    "editing_mode_warning": (
        "✏️ **Режим редагування активний.**\n"
        "Зараз ми змінюємо ваші дані. Введіть нове значення або натисніть /cancel, щоб скасувати."
    ),
    "session_expired": (
        "zzz **Сесія застаріла.**\n"
        "Ми довго не спілкувалися, і я втратив контекст. Почнімо спочатку: /start"
    ),
    "processing_request": (
        "⏳ **Обробляю запит...**\n"
        "Секундочку, формую відповідь."
    ),

    # 🟢 УСПІХ
    "doc_generated_success": (
        "✅ **Документ готовий!**\n"
        "Перевірте файл. Якщо все добре — роздрукуйте його та підпишіть."
    ),
    "data_updated": (
        "💾 **Дані успішно оновлено.**\n"
        "Я запам'ятав нову інформацію."
    ),
    "welcome_back": (
        "👋 **З поверненням!**\n"
        "Радий бачити. Чим можу допомогти сьогодні?"
    )
}

TEMPLATE_CONFIG = {
    "Заява про виготовлення студентського квитка": {
        "description": "Студент хоче отримати студентський квиток, вступивши до коледжу пізніше, ніж відбувся основний здобувачів",
        "required_fields": [],
        "nuances": """
    "Скільки коштує виготовлення студентського квитка?
    У 2025-2026 навчальному році студентський квиток вартує 150 грн"

    "Коли буде готовий студентський квиток?
    Зазвичай виготовлення студентського квитка займає до місяця часу. Коли ми отримаємо його, обов'язково повідомимо Вам про це офіційними каналами зв'язку: корпоративною поштою або чатом у відповідному месенджері"
    """
    },
    "Заява про відпрацювання за індивідуальним графіком": {
        "description": "Студент бажає відпрацьовувати пропущені заняття за певний проміжок часу у зв'язку з певною вагомою причиною, не в очному форматі.",
        "required_fields": ["REASON"],
        "nuances": """
        "Як мені отримати завдання для відпрацювання?
        Пишіть викладачам на корпоративну пошту (закінчується на @ukd.edu.ua)"

        "Що, якщо я не виконаю ці завдання і не відпрацюю пари за індивідуальним графіком?
        У такому разі за всі невідпрацьовані семінари (1-4 курс) або пари зі "шкільних" дисциплін (1-2 курс) у Вас у журналі стоятиме "1", а якщо це лекція з фахового предмету на 3 або 4 курсі - при наявності хоча б однієї "н" Вас не буде допущено до іспиту."
        """
    },
    "Заява про відрахування за власним бажанням": {
        "description": "Припинення навчання.",
        "required_fields": [],
        "nuances": """
        "Чи потрібно мені оплачувати щось після того, як мене було відраховано?
        Якщо Вас відраховано упродовж семестру, за який Ви не внесли оплату - Ви у будь-якому разі повинні внести цю оплату, адже Фаховий коледж надав для Вас освітню послугу у визначений період часу. Суму оплати варто уточнити у бухгалтерії за номером"

        "Чи можу я отримати виписку оцінок за період навчання?
        Так, таку виписку ми можемо сформувати, для цього потрібно сформувати іншу заяву, і очікувати виписку впродовж робочих 5 днів після подання її до нас та підпису директором Фахового коледжу. Чи готові Ви її сформувати?"
        """
    },
    "Заява про дозвіл пропустити деякі пари впродовж конкретного дня": {
        "description": "Студент хоче пропустити одну або декілька пар упродовж лише одного з найближчих днів з певної вагомої причини, і хоче попередити нас та викладачів про це.",
        "required_fields": ["DATE_FROM", "LESSONS_RANGE", "REASON"],
        "nuances": """
        Як відпрацьовувати пропущені пари?
        Після повернення Ви повинні мати при собі копію (можна фото) підписаної та погодженої з директором коледжу заяви, за якою отримали дозвіл пропустити пари, і підходити з нею до кожного із вчителів, що Вас навчають, запитуючи, як Ви можете відпрацювати пропущені заняття. Коли Ви виконаєте те, що кожен із викладачів Вам задасть - пропущені заняття будуть відпрацьовані, а у журналі з'являться відповідні відмітки ("н" або зникнуть, або заміняться на оцінку чи "н/в", що означає "відпрацьовано")

        Чи можна не відпрацьовувати пари?
        Ні, пропущені пари обов'язково потрібно відпрацювати        """
    },
    "Заява про дозвіл пропустити пари в навчальний період": {
        "description": "Студент знає, що не зможе перебувати (або вже не перебував) на парах упродовж більш, ніж 1 дня, і хоче попередити нас та викладачів про це.",
        "required_fields": ["DATE_FROM", "DATE_TO", "REASON"],
        "nuances": """
        "Що таке підтверджувальний документ?
        Якщо Ви перетинаєте кордон власним автомобілем або перевізником, і не маєте квитка на міжнародний рейс з України - підтверджуючим документом є скан-копія закордонного паспорта з печаткою про перетин кордону. Якщо ж у Вас є квиток на міжнародний рейс - підтверджуючим документом є копія даного квитка на зазначені Вами дати відсутності на парах. І те, і інше можна або відсканувати самостійно, або принести оригінали нам, щоб зробити копії"

        "Як відпрацьовувати пропущені пари?
        Після повернення Ви повинні мати при собі копію (можна фото) підписаної та погодженої з директором коледжу заяви, за якою отримали дозвіл пропустити пари, і підходити з нею до кожного із вчителів, що Вас навчають, запитуючи, як Ви можете відпрацювати пропущені заняття. Коли Ви виконаєте те, що кожен із викладачів Вам задасть - пропущені заняття будуть відпрацьовані, а у журналі з'являться відповідні відмітки ("н" або зникнуть, або заміняться на оцінку чи "н/в", що означає "відпрацьовано")

        "Чи можна не відпрацьовувати пари?
        Ні, пропущені пари обов'язково потрібно відпрацювати"

        "А що, якщо я виїжджаю закордон таким чином, що не встигну відпрацювати пропущені пари по поверненюю?
        У такому разі Вам потрібно ДО виїзду звернутися до усіх викладачів, що ведуть у Вас пари, і попросити їх надавати завдання Вам на пошту. При цьому, потрібно самостійно написати їм на пошту з проханням отримати ці завдання - таким чином Ви матимете підтвердження, що такі завдання готові були виконувати. При появі таких завдань потрібно виконати їх вчасно і відповідно до запиту викладача - це і буде способом, з допомогою якого Ви зможете відпрацювати свої пропущені пари"        """
    },
    "Заява про надання академвідпустки": {
        "description": "Студент бажає призупинити процес навчання з певної вагомої причини.",
        "required_fields": ["REASON"],
        "nuances": """
        "Чи втрачається бронь від мобілізації при отриманні академвідпустки?
        При отриманні академвідпустки бронь від мобілізації втрачається."

        "Скільки триває академвідпустка?
        Академічна відпустка триває один рік, після чого термін її дії автоматично припиняється. Якщо дію академічної відпустки необхідно продовжити - потрібно буде подати ще одну заяву про оформлення відповідної академічної відпустки."
    """
    },
    "Заява про отримання виписки оцінок": {
        "description": "Студент бажає отримати виписку своїх оцінок до завершення навчання у Фаховому коледжі з певної вагомої причини.",
        "required_fields": ["DATE_FROM", "DATE_TO", "REASON"],
        "nuances": """
        "Скільки часу формується виписка оцінок?
        Виписка оцінок формується упродовж 5 робочих днів з дня прийняття заяви"
        """
    },
    "Заява про отримання індивідуального графіка навчання": {
        "description": "Студент бажає отримати індивідуальний графік (не індивідуальну форму, це інше) навчання з певної вагомої причини.",
        "required_fields": ["SPECIALTY", "REASON"],
        "nuances": """
        "Чи можу я не ходити на пари, коли отримаю індивідуальний графік навчання?
        Так, індивідуальний графік навчання складається суто під Вас та Ваші можливості, що передбачає необов'язковість відвідування академічних пар за тим розкладом, за яким їх відвідують Ваші одногрупники та одногрупниці"        """
    },
    "Заява про перевід на денну форму навчання": {
        "description": "Студент бажає перевестися на денну форму навчання.",
        "required_fields": ["SPECIALTY"],
        "nuances": """
    !!!КОЛИ ОБИРАТИ: Якщо студент пропустив іспит/залік через хворобу, сімейні обставини або змагання.
    !!!НЮАНСИ: Обов'язково попередити студента, що у нього змінюється код групи: якщо раніше він був, до прикладу, КБз або КБд, то тепер - КБс.
    !!!ЩО ВІДПОВІДАТИ: "Студента переведуть на денну форму лише починаючи з наступного семестру"
    """
    },
    "Заява про перевід на заочну форму навчання": {
        "description": "Студент 3-4 курсу бажає перевестися на заочну форму навчання.",
        "required_fields": ["SPECIALTY"],
        "nuances": """
    !!!КОЛИ ОБИРАТИ: Якщо студент має бажання перевестися на заочну форму навчання у рамках своєї спеціальності.
    !!!НЮАНСИ: Поінформувати щодо дат та вартості переведення. попередити студента, що у нього змінюється код групи: якщо раніше він був, до прикладу, КБс або КБд, то тепер - КБз.
    !!!ЩО ВІДПОВІДАТИ: "Коли б студент не написав цю заяву - його може бути переведено на іншу спеціальність лише перед початком наступного семестру. Щодо доплати - зазвичай доплачувати за перевід не потрібно, але Ви можете уточнити це у бухгалтерії Університету Короля Данила за номером +380342...."
    """
    },
    "Заява про перевід на індивідуальну форму навчання": {
        "description": "Студент бажає перевестися на індивідуальну форму навчання (не індивідуальний графік, це інше).",
        "required_fields": ["SPECIALTY"],
        "nuances": """
    "У чому відмінність індивідуальної форми та індивідуального графіка?
    Юридично для оформлення індивідуальної форми не потрібно вказувати вагомих причин, що потрібно для індивідуального графіка навчання. Але, вартість індивідуальної форми навчання є вищою, ніж вартість індивідуального графіка, вартість якого не змінюється порівняно з денною формою навчання"
    """
    },
    "Заява про перевід на іншу спеціальність": {
        "description": "Студент бажає перевестися на іншу спеціальність.",
        "required_fields": ["SUBJECT", "SPECIALTY_TO"],
        "nuances": """
        "Коли мене буде переведено на іншу спеціальність?
        Перевід здійснюється з наступного навчального семестру. При цьому, якщо Ви подали заяву про перевід між спеціальностями, до прикладу, 2 вересня - все одно маєте дочекатися наступного семестру"

        "Чи потрібно доплачувати за перевід на іншу спеціальність?
        Загальний принцип каже, що доплачувати не потрібно - Ви платите за нову спеціальність стільки ж, скільки б заплатили за свою попередню спеціальність без переводу. Однак, не зайвим буде уточнити цю інформацію у бухгалтерії Університету за номером +380ххххххххххх"        """
    },
    "Заява про повторний курс у дистанційному форматі": {
        "description": "Студент перебуває за кордоном або за станом здоров'я не може ліквідувати повторний курс.",
        "required_fields": ["SUBJECT", "REASON"],
        "nuances": """
        "Що робити, якщо мені потрібно ліквідувати повторний курс із декількох дисциплін?
        Для кожної дисципліни, з котрої у Вас є повторний курс, Ви повинні сформувати окрему заяву"        """
    },
    "Заява про складання навчальної практики за індивідуальним графіком": {
        "description": "Студент не може складати навчальну практику очно у визначений термін.",
        "required_fields": ["REASON"],
        "nuances": """
    !!!КОЛИ ОБИРАТИ: Якщо студент не зможе складати навчальну практику (повністю або частково) через хворобу, сімейні обставини, змагання тощо.
    !!!НЮАНСИ: Обов'язково попередити студента, що пізніше треба принести оригінал підтверджувального документа у 300 кабінет (адміністрація Фахового коледжу).
    !!!ЩО ВІДПОВІДАТИ: "Ця заява підходить, якщо у вас є документальне підтвердження причини відсутності."
    """
    },
    "Заява про складання сесії в усній формі у зв'язку з наявністю особливих освітніх потреб у студента": {
        "description": "Студент не може складати іспит у кабінеті у зв'язку з наявністю особливих освітніх потреб.",
        "required_fields": [],
        "nuances": """
            Для складання іспиту у усній формі потрібно мати підтвердження наявності особливих освітніх потреб у відповідного студента
            """
    },
    "Заява про складання сесії за індивідуальним графіком": {
        "description": "Перенесення сесії через поважні причини.",
        "required_fields": ["SUBJECT", "REASON"],
        # "ignored_fields": ["DATE_FROM", "DATE_TO"] # Для певності можна явно заборонити
        "nuances": """
        !!!КОЛИ ОБИРАТИ: Якщо студент пропустив іспит/залік через хворобу, сімейні обставини, змагання тощо.
        !!!НЮАНСИ: Обов'язково попередити студента, що пізніше треба принести оригінал довідки (лікарняний, виклик на змагання тощо) у 300 кабінет (адміністрація Фахового коледжу).
        !!!ЩО ВІДПОВІДАТИ: "Ця заява підходить, якщо у вас є документальне підтвердження причини відсутності."
        """
    },
}


# --- НАЛАШТУВАННЯ ЛОГУВАННЯ ---
# Створюємо папку для логів, якщо немає
if not os.path.exists('logs'):
    os.makedirs('logs')

logger = logging.getLogger("eCollegeBot")
logger.setLevel(logging.INFO)

# Форматер: Час - Рівень - Повідомлення
formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# 1. Лог у файл (ротація щодня, зберігає історію за 30 днів)
file_handler = TimedRotatingFileHandler("logs/bot_history.log", when="midnight", interval=1, backupCount=30,
                                        encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# 2. Лог у консоль
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


class DriveManager:
    """Клас для роботи з Google Drive/Docs"""

    def __init__(self):
        self.creds = None
        if os.path.exists('token.json'):
            self.creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
                self.creds = flow.run_local_server(port=0)
            with open('token.json', 'w') as token:
                token.write(self.creds.to_json())

        self.drive_service = build('drive', 'v3', credentials=self.creds)
        self.docs_service = build('docs', 'v1', credentials=self.creds)
        logger.info("✅ Google Drive API connected successfully.")

    def get_available_templates(self):
        try:
            results = self.drive_service.files().list(
                q=f"'{TEMPLATES_FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.document' and trashed=false",
                fields="files(id, name)"
            ).execute()
            return results.get('files', [])
        except Exception as e:
            logger.error(f"❌ Failed to fetch templates: {e}")
            return []

    def create_pdf_from_template(self, template_id, template_name, data):
        # 1. Автоматичне заповнення технічних полів
        data['date_of_signing'] = datetime.date.today().strftime("%d.%m.%Y")

        # Обчислення курсу, якщо його немає
        if 'group' in data and ('study_year' not in data or not data['study_year']):
            import re
            match = re.search(r'-(\d{2})', data['group'])
            if match:
                entry_year = 2000 + int(match.group(1))
                current_year = datetime.date.today().year
                current_month = datetime.date.today().month
                course = current_year - entry_year + (1 if current_month >= 9 else 0)
                if course > 0:
                    data['study_year'] = str(course)

        doc_id = None
        try:
            # Копіювання шаблону
            copy_body = {'name': f"TEMP_{template_name}", 'parents': [TEMPLATES_FOLDER_ID]}
            file_copy = self.drive_service.files().copy(fileId=template_id, body=copy_body).execute()
            doc_id = file_copy['id']

            # Заміна тексту
            requests = []
            for key, value in data.items():
                safe_value = str(value) if value is not None else ""
                variants = [key.lower(), key.upper(), key.capitalize()]  # {{key}}, {{KEY}}, {{Key}}

                for var in variants:
                    requests.append({
                        'replaceAllText': {
                            'containsText': {'text': f'{{{{{var}}}}}', 'matchCase': True},
                            'replaceText': safe_value
                        }
                    })

            if requests:
                self.docs_service.documents().batchUpdate(documentId=doc_id, body={'requests': requests}).execute()

            # Експорт у PDF
            request = self.drive_service.files().export_media(fileId=doc_id, mimeType='application/pdf')
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()

            return fh.getvalue()

        except Exception as e:
            logger.error(f"⚠️ PDF Creation Error: {e}")
            raise e
        finally:
            if doc_id:
                try:
                    self.drive_service.files().delete(fileId=doc_id).execute()
                except:
                    pass


class SheetManager:
    def __init__(self, credentials, spreadsheet_id):
        self.service = build('sheets', 'v4', credentials=credentials)
        self.spreadsheet_id = spreadsheet_id
        logger.info("✅ Google Sheets API connected successfully.")

    def get_student_data(self, telegram_id):
        """Шукає студента за Telegram ID"""
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id, range="Students!A:Z"
            ).execute()
            rows = result.get('values', [])
            if not rows: return {}

            headers = rows[0]
            try:
                tg_id_idx = next(i for i, h in enumerate(headers) if h.strip() == 'Telegram_ID')
            except StopIteration:
                logger.warning("Column 'Telegram_ID' not found in Sheets.")
                return {}

            for row in rows[1:]:
                if len(row) > tg_id_idx and str(row[tg_id_idx]).strip() == str(telegram_id):
                    student_data = {}
                    for i, header in enumerate(headers):
                        val = row[i] if i < len(row) else ""
                        if val.strip():
                            student_data[header.strip()] = val.strip()
                    return student_data
            return {}
        except Exception as e:
            logger.error(f"Google Sheets API Error (get_student_data): {e}")
            return {}

    def get_all_students(self):
        """Отримує всіх студентів для пошуку по email"""
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id, range="Students!A:Z"
            ).execute()
            rows = result.get('values', [])
            if len(rows) < 2: return []

            headers = [h.strip() for h in rows[0]]
            students_list = []
            for row in rows[1:]:
                student_data = {}
                for i, header in enumerate(headers):
                    val = row[i] if i < len(row) else ""
                    student_data[header] = val
                students_list.append(student_data)
            return students_list
        except Exception as e:
            logger.error(f"Error fetching all students: {e}")
            return []

    def get_student_by_email(self, email):
        """Шукає студента за email"""
        try:
            all_students = self.get_all_students()
            target_email = email.strip().lower()

            for student in all_students:
                # Перевіряємо різні варіанти написання назви колонки
                raw_email = str(
                    student.get('EMAIL', '') or student.get('Email', '') or student.get('STUDENTS_EMAIL', ''))
                student_email = raw_email.strip().lower()
                if student_email == target_email:
                    return student
            return None
        except Exception as e:
            logger.error(f"Error searching by email: {e}")
            return None

    def link_telegram_id(self, email, telegram_id):
        """Прив'язує Telegram ID до Email у таблиці"""
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id, range="Students!A:Z"
            ).execute()
            rows = result.get('values', [])
            headers = [h.strip() for h in rows[0]]

            try:
                # Шукаємо індекси (більш гнучкий пошук)
                email_col_idx = next(
                    i for i, h in enumerate(headers) if h.lower() in ['email', 'пошта', 'students_email'])
                tg_col_idx = next(i for i, h in enumerate(headers) if h == 'Telegram_ID')
            except StopIteration:
                logger.error("Error: Columns 'Email' or 'Telegram_ID' missing in Sheet.")
                return False

            target_email = email.strip().lower()
            row_number = -1

            for i, row in enumerate(rows):
                if i == 0: continue
                curr_email = str(row[email_col_idx] if len(row) > email_col_idx else "").strip().lower()
                if curr_email == target_email:
                    row_number = i + 1
                    break

            if row_number == -1: return False

            col_letter = chr(ord('A') + tg_col_idx)  # Працює для колонок A-Z
            cell_range = f"Students!{col_letter}{row_number}"

            body = {'values': [[str(telegram_id)]]}
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id, range=cell_range,
                valueInputOption="RAW", body=body
            ).execute()

            logger.info(f"💾 Linked Telegram_ID {telegram_id} to {email}")
            return True
        except Exception as e:
            logger.error(f"Error linking Telegram ID: {e}")
            return False

    def update_student_field(self, telegram_id, field_name, new_value):
        """Оновлює конкретне поле (колонку) для студента за Telegram_ID."""
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id, range="Students!A:Z"
            ).execute()
            rows = result.get('values', [])
            if not rows: return False

            headers = [h.strip().upper() for h in rows[0]]
            target_field = field_name.strip().upper()

            try:
                tg_col_idx = headers.index('TELEGRAM_ID')
                target_col_idx = headers.index(target_field)
            except ValueError:
                logger.error(f"Column {target_field} or TELEGRAM_ID not found for update.")
                return False

            row_number = -1
            for i, row in enumerate(rows):
                if i == 0: continue
                uid = str(row[tg_col_idx]) if len(row) > tg_col_idx else ""
                if uid == str(telegram_id):
                    row_number = i + 1
                    break

            if row_number == -1: return False

            col_letter = chr(ord('A') + target_col_idx)
            cell_range = f"Students!{col_letter}{row_number}"

            body = {'values': [[str(new_value)]]}
            self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id, range=cell_range,
                valueInputOption="RAW", body=body
            ).execute()

            logger.info(f"✏️ Updated {field_name} for ID {telegram_id}: {new_value}")
            return True
        except Exception as e:
            logger.error(f"Error updating sheet: {e}")
            return False

    def log_event(self, student_data, doc_type, status="✅ SUCCESS"):
        """Записує подію (генерацію документа) у вкладку Logs"""
        try:
            now = datetime.datetime.now()
            date_str = now.strftime("%d.%m.%Y")
            time_str = now.strftime("%H:%M:%S")

            # Витягуємо дані (безпечно, якщо якихось полів немає)
            name = student_data.get('STUDENTS_NAME', 'Невідомо')
            group = student_data.get('GROUP', '—')

            # Формуємо рядок для запису
            values = [[date_str, time_str, name, group, doc_type, status]]

            body = {'values': values}

            # append додає дані в перший порожній рядок
            self.service.spreadsheets().values().append(
                spreadsheetId=self.spreadsheet_id,
                range="Logs!A:F",  # Діапазон колонок
                valueInputOption="USER_ENTERED",  # Щоб дати розпізнались як дати
                insertDataOption="INSERT_ROWS",
                body=body
            ).execute()

            logger.info(f"📊 Logged to Sheets: {doc_type} for {name}")
            return True

        except Exception as e:
            logger.error(f"❌ Failed to log to Sheets: {e}")
            return False


class EmailManager:
    def __init__(self, credentials):
        self.service = build('gmail', 'v1', credentials=credentials)
        logger.info("✅ Gmail API connected.")

    def send_email(self, to_email, subject, body, file_bytes_io, filename):
        try:
            message = MIMEMultipart()
            message['to'] = to_email
            message['subject'] = subject
            message.attach(MIMEText(body, 'plain'))

            file_bytes_io.seek(0)
            part = MIMEApplication(file_bytes_io.read(), Name=filename)
            part['Content-Disposition'] = f'attachment; filename="{filename}"'
            message.attach(part)
            file_bytes_io.seek(0)

            raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
            self.service.users().messages().send(userId="me", body={'raw': raw_message}).execute()
            logger.info(f"📧 Email sent to {to_email} with {filename}")
            return True
        except Exception as e:
            logger.error(f"❌ Email sending error: {e}")
            return False


class GeminiBrain:
    def __init__(self):
        self.client = genai.Client(api_key=GEMINI_API_KEY)
        self.model = GEMINI_AI_MODEL
        logger.info("✅ Gemini AI connected.")

    def analyze_dialog_turn(self, history, template_list, known_data=None, current_active_template=None, mode="NORMAL"):
        if known_data is None: known_data = {}

        response_schema = {
            "type": "OBJECT",
            "properties": {
                "status": {
                    "type": "STRING",
                    "enum": ["CLARIFICATION_NEEDED", "PROFILE_UPDATE", "TEMPLATE_SELECTED", "WAITING_FOR_CONFIRMATION",
                             "READY_TO_GENERATE"]
                },
                "bot_reply": {"type": "STRING"},
                "selected_template_name": {"type": "STRING", "nullable": True},
                "extracted_data": {
                    "type": "OBJECT",
                    "properties": {
                        # Основні поля профілю
                        "STUDENTS_NAME": {"type": "STRING"},
                        "STUDENTS_PHONE": {"type": "STRING"},
                        "STUDENTS_EMAIL": {"type": "STRING"},
                        "DATE_OF_BIRTH": {"type": "STRING"},
                        "SPECIALTY": {"type": "STRING"},
                        "GROUP": {"type": "STRING"},
                        "PARENTS_NAME": {"type": "STRING"},
                        "PARENTS_PHONE": {"type": "STRING"},

                        # Поля для заяв (динамічні)
                        "DATE_FROM": {"type": "STRING"},
                        "DATE_TO": {"type": "STRING"},
                        "REASON": {"type": "STRING"},
                        "SPECIALITY_TO": {"type": "STRING"},
                        "SUBJECT": {"type": "STRING"},
                        "DATE_OF_SIGNING": {"type": "STRING"},
                        "STUDY_YEAR": {"type": "STRING"},
                        "STUDY_SEMESTER": {"type": "STRING"},
                        "LESSONS_RANGE": {"type": "STRING"},
                    },
                    "nullable": True
                }
            },
            "required": ["status", "bot_reply"]
        }

        templates_info = []
        for t in template_list:
            name = t['name']
            # Отримуємо конфіг або пустий словник, якщо шаблону немає в списку
            config = TEMPLATE_CONFIG.get(name, {})

            # 1. Формуємо інструкцію по змінних (ЖОРСТКО)
            req_fields = config.get("required_fields", [])
            if req_fields:
                vars_instruction = f"   [STRICT VARIABLES]: You MUST collect ONLY: {', '.join(req_fields)} + standard Profile Data."
            else:
                vars_instruction = "   [STRICT VARIABLES]: Collect ONLY standard Profile Data (Name, Group, etc)."

            # 2. Формуємо інструкцію по нюансах (М'ЯКО)
            nuances = config.get("nuances", "No specific local rules. Use general knowledge.")

            # Збираємо блок
            info_block = (
                f"TEMPLATE: '{name}'\n"
                f"   Description: {config.get('description', '')}\n"
                f"   COLLEGE RULES & NUANCES: {nuances}\n"
                f"{vars_instruction}"
            )
            templates_info.append(info_block)

        templates_str = "\n".join(templates_info)

        current_date_obj = datetime.date.today()
        current_date_str = current_date_obj.strftime("%d.%m.%Y")

        # Логіка семестру і року
        if current_date_obj.month >= 8:  # Серпень-Грудень
            academic_year_base = current_date_obj.year
            current_semester = "1"
        else:  # Січень-Липень
            academic_year_base = current_date_obj.year - 1
            current_semester = "2"

        known_data_str = json.dumps(known_data, ensure_ascii=False, indent=2)
        chat_context = "\n".join([f"{msg['role']}: {msg['content']}" for msg in history])

        focus_instruction = ""
        if mode == 'EDITING':
            focus_instruction = "⚠️ URGENT: USER IS IN 'EDITING_MODE'. IGNORE requests for new documents. FOCUS ONLY on extracting Profile Data (Group, Phone, etc)."
        if current_active_template:
            focus_instruction = f"URGENT: Active template is '{current_active_template}'. Focus ONLY on gathering missing fields for this document."

        prompt = f"""
        You are the AI Administrator for King Danylo University College Administration. (NO DEANS OFFICE, ONLY COLLEGE ADMINISTRATION)
        Identify request, consult the student using COLLEGE RULES, gather data, and output JSON based on the SCHEMA.

        ### ROLE & BEHAVIOR
        - You are helpful, polite, and professional.
        - **CONSULTANT MODE:** If the user asks "What should I do?" or "Which application is needed?", use the "ADVICE/FAQ" section from the templates list to guide them.
        - **CLARIFICATION:** If the user's situation matches a template description, suggest that template.

        ### 1. SYSTEM CONTEXT
        - Mode: {mode} (If EDITING -> Only update profile).
        - Date: {current_date_str}
        - Year Base: {academic_year_base}
        - Semester: {current_semester}
        - Available Templates & Knowledge Base:
        {templates_str}
        {focus_instruction}

        ### 2. STUDENT DATA
        {known_data_str}

        ### 3. RULES FOR PROFILE UPDATES
        If user wants to change info (e.g., "New phone"):
        - ALLOWED: "STUDENTS_PHONE", "PARENTS_PHONE". (Specialty updates automatically via Group, Parents phone has to differ from students phone).
        - ACTION:
          - If field is allowed and new value provided -> Output `PROFILE_UPDATE` + `extracted_data` (ONE key-value).
          - If value missing -> Output `CLARIFICATION_NEEDED`.

        ### 4. DATA PROCESSING & RESTRICTIONS (CRITICAL)
        - **STRICT SCHEMA:** You can ONLY output fields defined in the 'extracted_data' schema. Do not invent new fields.
        - **NO FILE UPLOADS:** DO NOT ask the user to upload photos, scans, or documents (medical certs, passports, tickets). 
        - **HANDLING PROOFS:** If a reason implies a document (e.g., "sick leave" -> medical cert, "border crossing" -> passport stamp):
        - **DO NOT ASK** for dates, periods, or numbers if they are NOT in the "REQUIRED VARIABLES" list for the specific template.          
        - Accept the user's text explanation.
        - In `bot_reply`, just REMIND the user to bring the physical original to the college office.
        - **ONLY REQUESTED DATA:** Each template has its own variables. When TEMPLATE_SELECTED, request ONLY including but not already accessible variables - they are written in double curly brackets.  
        - **REASON:** Concise linguistic construction (e.g., "сімейними обставинами" not "сімейні обставини", "поїздкою за кордон" not "поїздка за кордон" etc. And NEVER paste "у зв'язку з", because it is already given in the template).  
        - **DATES:** Convert relative dates ("last week", "yesterday") to specific "DD.MM.YYYY".

        ### 5. WORKFLOW
        1. Identify Intent (Update Profile OR Create Document).
        2. Identify Template -> `TEMPLATE_SELECTED`.
        3. Gather Data -> `CLARIFICATION_NEEDED`.
        4. VERIFICATION (CRITICAL):
           - If all text data ready -> `WAITING_FOR_CONFIRMATION` (List data, ask "Correct?").
        5. Finalize:
           - If confirmed -> `READY_TO_GENERATE`.
           - If corrected -> Go back.

        ### 6. CHAT
        {chat_context}

        Respond strictly in JSON:
        {{
          "status": "CLARIFICATION_NEEDED | PROFILE_UPDATE | TEMPLATE_SELECTED | WAITING_FOR_CONFIRMATION | READY_TO_GENERATE",
          "bot_reply": "String (Ukrainian)",
          "selected_template_name": "String or null",
          "extracted_data": {{ "FIELD": "VALUE" }}
        }}
        """

        # Спрощений виклик без складної схеми (для економії токенів і швидкості), але з JSON enforcement
        max_retries = 5
        for attempt in range(max_retries):
            try:
                # Виклик Gemini (з налаштуваннями безпеки, які ми додали раніше)
                response = self.client.models.generate_content(
                    model=self.model,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        safety_settings=[
                            types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="BLOCK_NONE"),
                            types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="BLOCK_NONE"),
                            types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="BLOCK_NONE"),
                            types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="BLOCK_NONE")
                        ]
                    )
                )

                if not response.text:
                    logger.warning("⚠️ Gemini returned EMPTY response.")
                    return {"status": "CLARIFICATION_NEEDED",
                            "bot_reply": "Я не знаю, що відповісти. Сформулюйте запит інакше."}

                raw_text = response.text.strip()
                if raw_text.startswith("```"):
                    raw_text = raw_text.strip("`").replace("json\n", "", 1).strip()

                # Якщо успішно розпарсили JSON — повертаємо результат і виходимо з циклу
                return json.loads(raw_text)

            except Exception as e:
                error_str = str(e)
                is_last_attempt = (attempt == max_retries - 1)

                # Перевіряємо, чи це помилка перевантаження (503 або 429)
                if "503" in error_str or "overloaded" in error_str or "429" in error_str:
                    if is_last_attempt:
                        logger.error(f"❌ Gemini Failed: {e}")
                        # ВИКОРИСТОВУЄМО НАШ СЛОВНИК:
                        return {"status": "CLARIFICATION_NEEDED", "bot_reply": UI_MESSAGES["google_overload"]}

                    time.sleep(2)
                    continue
                else:
                    logger.error(f"❌ Critical Error: {e}")
                    # ВИКОРИСТОВУЄМО НАШ СЛОВНИК:
                    return {"status": "CLARIFICATION_NEEDED", "bot_reply": UI_MESSAGES["unknown_critical_error"]}
# Ініціалізація
drive_mgr = DriveManager()
sheet_mgr = SheetManager(drive_mgr.creds, SPREADSHEET_ID)
email_mgr = EmailManager(drive_mgr.creds)
brain = GeminiBrain()
user_sessions = {}



def get_specialty_from_group(group_name):
    import re
    # Шукаємо текст між "К" та формою навчання (с/з/і/д)
    match = re.search(r'^К(.+?)[сзід]-', group_name.strip(), re.IGNORECASE)
    if match:
        code = match.group(1).strip() # Наприклад "ІПЗ" або "Мр"
        # Шукаємо в словнику (враховуючи регістр ключів)
        for key, value in SPECIALTIES_MAP.items():
            if key.lower() == code.lower():
                return value
    return None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    chat_id = update.effective_chat.id
    reset_timeout_timer(user_id, chat_id, context)

    logger.info(f"[{user_id}] STARTED Interaction.")

    if user_id not in user_sessions:
        user_sessions[user_id] = {
            'history': [], 'profile': {}, 'active_template': None,
            'msg_count': 0, 'blocked_until': None
        }

    student = sheet_mgr.get_student_data(user_id)
    if student:
        user_sessions[user_id] = {'history': [], 'profile': student, 'active_template': None}
        logger.info(f"[{user_id}] AUTH SUCCESS via Database.")
        await update.message.reply_text(f"👋 З поверненням, {student.get('STUDENTS_NAME')}! Радий бачити. Чим можу допомогти?", parse_mode="Markdown")
        return

    # 3. New
    user_sessions[user_id] = {'history': [], 'profile': {}, 'active_template': None}
    logger.info(f"[{user_id}] New User - Requesting Email.")
    await update.message.reply_text(
        "👋 Вітаю! Я ШІ-адміністратор Фахового коледжу УКД.\n📧 Для того, щоб я міг надавати Вам послуги, вкажіть Вашу корпоративну пошту (з доменом @ukd.edu.ua) для входу.",
            parse_mode = "Markdown"
    )


async def ask_next_field(update, context, field):
    """Відправляє питання з правильною клавіатурою"""
    question = REQUIRED_FIELDS_MAP[field]

    if field == "STUDENTS_PHONE":
        # Спеціальна кнопка для телефону
        keyboard = [[KeyboardButton("📱 Поділитися номером", request_contact=True)]]
        markup = ReplyKeyboardMarkup(keyboard, one_time_keyboard=True, resize_keyboard=True)
    else:
        # Прибираємо кнопку для звичайних текстових питань
        markup = ReplyKeyboardRemove()

    await update.message.reply_text(question, reply_markup=markup)


def check_missing_fields(profile):
    """Повертає список полів, які пусті в профілі"""
    missing = []
    for field, question in REQUIRED_FIELDS_MAP.items():
        # Перевіряємо, чи поле існує і чи воно не пусте
        val = profile.get(field)
        if not val or str(val).strip() == "":
            missing.append(field)
    return missing


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    chat_id = update.effective_chat.id
    text = update.message.text.strip() if update.message.text else ""

    if user_id not in user_sessions:
        user_sessions[user_id] = {
            'history': [], 'profile': {}, 'active_template': None,
            'msg_count': 0, 'blocked_until': None, 'mode': 'NORMAL',
            'missing_fields': []
        }
    session = user_sessions[user_id]

    # === БЛОК БЕЗПЕКИ (ДОДАНО) ===
    # Гарантує, що нові змінні існують, навіть якщо сесія створена давно
    if 'msg_count' not in session: session['msg_count'] = 0
    if 'blocked_until' not in session: session['blocked_until'] = None
    if 'mode' not in session: session['mode'] = 'NORMAL'
    if 'missing_fields' not in session: session['missing_fields'] = []

    if not text and not update.message.contact:

        # Перелік того, на що ми реагуємо попередженням
        if (update.message.voice or update.message.video or update.message.video_note or
                update.message.audio or update.message.document or update.message.photo or
                update.message.sticker or update.message.location or update.message.animation):
            await update.message.reply_text(
                "⚠️ **Наразі я - текстовий бот.**\n"
                "Я не вмію слухати голосові, дивитися відео чи файли.\n"
                "Будь ласка, напишіть ваше запитання текстом ✍️",
                parse_mode="Markdown"
            )
            # Ми просто виходимо з функції. Стан сесії (session) не змінюється.
            # Бот "забуває", що це повідомлення було, і чекає наступного.
            return

    # 2. АНТИСПАМ: Перевірка БАНУ
    if session.get('blocked_until'):
        if datetime.datetime.now() < session['blocked_until']:
            logger.info(f"[{user_id}] User BLOCKED. Ignoring message.")
            return  # Ігноруємо
        else:
            # Бан закінчився
            session['blocked_until'] = None
            session['msg_count'] = 0
            logger.info(f"[{user_id}] User UNBLOCKED.")

    if session['mode'] == 'ONBOARDING':
        # Якщо користувач хоче вийти
        if text == "/cancel":
            session['mode'] = 'NORMAL'
            session['missing_fields'] = []
            await update.message.reply_text("⚠️ Заповнення даних перервано.", reply_markup=ReplyKeyboardRemove())
            return

        # Беремо поточне поле, яке ми питали
        if session['missing_fields']:
            current_field = session['missing_fields'][0]
            value_to_save = None

            if update.message.contact:
                phone_num = update.message.contact.phone_number
                # Telegram може надсилати номер без "+", додамо якщо треба
                if not phone_num.startswith('+'):
                    phone_num = '+' + phone_num
                value_to_save = phone_num
                # Б) Якщо користувач все ж таки ввів текст (для телефону або іншого поля)
            elif text:
                value_to_save = text
                # В) Якщо прислали щось не те (наприклад, стікер замість контакту)
            else:
                await update.message.reply_text(
                    "⚠️ Будь ласка, надішліть текстову відповідь або скористайтеся кнопкою.")
                return

                # 1. Валідація номера представника (Анти-Дублікат)
            if current_field == "PARENTS_PHONE":
                student_phone = session['profile'].get('STUDENTS_PHONE', '')

                    # Функція для очистки номера (залишає тільки цифри)
                def clean_phone(p):
                    return re.sub(r'\D', '', str(p))

                    # Порівнюємо "чисті" цифри
                if clean_phone(value_to_save) == clean_phone(student_phone):
                    await update.message.reply_text(
                        "⛔ **Помилка:** Ви ввели свій власний номер.\n"
                        "Нам потрібен номер когось із представників для екстреного зв'язку.\n"
                        "Будь ласка, введіть інший номер."
                    )
                    return  # Не зберігаємо, чекаємо нового вводу

                # 2. Валідація Дати (Базова перевірка формату)
            if current_field == "DATE_OF_BIRTH":
                # Перевіряємо, чи схоже це на дату (цифри і крапки)
                if not re.match(r'^\d{2}\.\d{2}\.\d{4}$', value_to_save):
                    await update.message.reply_text(
                        "⚠️ Невірний формат. Введіть дату як **ДД.ММ.РРРР** (наприклад, 07.02.2002).",
                        parse_mode="Markdown")
                    return

                # --- ЗБЕРЕЖЕННЯ ---
                # Видаляємо клавіатуру перед збереженням
            wait_msg = await update.message.reply_text("💾 Зберігаю...", reply_markup=ReplyKeyboardRemove())

            if sheet_mgr.update_student_field(user_id, current_field, value_to_save):
                session['profile'][current_field] = value_to_save
                session['missing_fields'].pop(0)  # Видаляємо питання зі списку
                await context.bot.delete_message(chat_id, wait_msg.message_id)
            else:
                await update.message.reply_text("❌ Помилка запису в базу. Спробуйте ще раз.")
                return

            # --- НАСТУПНИЙ КРОК ---
            if session['missing_fields']:
                next_field = session['missing_fields'][0]
                await update.message.reply_text("✅ Прийнято.")
                await ask_next_field(update, context, next_field)
                return
            else:
                session['mode'] = 'NORMAL'
                await update.message.reply_text(
                    "🎉 **Всі дані успішно збережено!**\nПрофіль готовий. Чим можу допомогти?",
                    reply_markup=ReplyKeyboardRemove(),
                    parse_mode="Markdown"
                )
                return

    # 3. АНТИСПАМ: Лічильник
    if session['mode'] == 'NORMAL' and not session.get('active_template'):
        session['msg_count'] = session.get('msg_count', 0) + 1
        logger.info(f"[{user_id}] Spam Counter: {session['msg_count']}/10")

        # Якщо більше 10 - БАН
        if session['msg_count'] > 10:
            block_time = datetime.datetime.now() + timedelta(hours=1)
            session['blocked_until'] = block_time

            logger.warning(f"[{user_id}] 🚫 SPAM BLOCK triggered.")
            sheet_mgr.log_event(session.get('profile', {}), "SPAM FILTER", "🚫 BLOCKED (1h)")

            await update.message.reply_text(
                "⛔ **Система визначила вашу поведінку як спам.**\n\n"
                "Ви надіслали занадто багато повідомлень без конкретної мети.\n"
                "⏳ Бот тимчасово заблокований для вас на 1 годину.\n"
                "Якщо у вас є термінове питання — зверніться в Дирекцію коледжу."
                , parse_mode="Markdown")

            # Очистка, щоб після розбану почати з нуля
            session['history'] = []
            session['msg_count'] = 0
            return

    # Скидаємо таймер "антихвіст"
    reset_timeout_timer(user_id, chat_id, context)

    # --- АВТОРИЗАЦІЯ ---
    if not session.get('profile'):
        # Спроба DB (раптом перезапуск)
        student_db = sheet_mgr.get_student_data(user_id)
        if student_db:
            session['profile'] = student_db
            logger.info(f"[{user_id}] Silent Auth Success.")
            if text == "/start":
                await update.message.reply_text(f"👋 З поверненням, {student_db.get('STUDENTS_NAME')}! Радий бачити. Чим можу допомогти?", parse_mode="Markdown")
                return
        else:
            # Перевірка Email
            if "@ukd.edu.ua" not in text.lower():
                await update.message.reply_text("🔒 Пошта повинна бути у форматі @ukd.edu.ua.", parse_mode="Markdown")
                return

            msg = await update.message.reply_text("🔎 Шукаю вас у базі...", parse_mode="Markdown")
            student = sheet_mgr.get_student_by_email(text)

            if student:
                sheet_mgr.link_telegram_id(text, user_id)
                session['profile'] = student
                logger.info(f"[{user_id}] Auth Linked via Email: {text}")
                await context.bot.delete_message(chat_id, msg.message_id)
                await update.message.reply_text(f"✅ Вітаю, {student.get('STUDENTS_NAME')}! Ви авторизовані.", parse_mode="Markdown")

                missing = check_missing_fields(student)
                if missing:
                    session['missing_fields'] = missing
                    session['mode'] = 'ONBOARDING'

                    first_field = missing[0]
                    question = REQUIRED_FIELDS_MAP[first_field]

                    await update.message.reply_text(
                        "⚠️ **Увага!** У вашому профілі не вистачає деяких даних, необхідних для заяв.\n"
                        "Будь ласка, заповніть їх зараз (це потрібно зробити один раз).\n\n"
                        f"👉 {question}",
                        parse_mode="Markdown"
                    )
                    return  # Зупиняємось, чекаємо відповіді користувача
            else:
                logger.warning(f"[{user_id}] Auth Failed: Email {text} not found.")
                await context.bot.edit_message_text(chat_id, msg.message_id, text="⛔ Пошту не знайдено. Спробуйте ще раз або зверніться у каб.300 до Білоуса Святослава Олеговича")
            return

    # --- ШІ ОБРОБКА ---
    session['history'].append({"role": "user", "content": text})
    await context.bot.send_chat_action(chat_id, action="typing")

    try:
        templates = drive_mgr.get_available_templates()

        logger.info(f"[{user_id}] 🗣️ USER MESSAGE: \"{text}\"")

        analysis = brain.analyze_dialog_turn(
            session['history'],
            templates,
            session['profile'],
            session['active_template'],
            mode=session['mode']  # <--- НОВЕ
        )

        # Для більш детальної перевірки
        # logger.info(f"[{user_id}] 🧠 AI DECISION: {json.dumps(analysis, ensure_ascii=False)}")

    except Exception as e:
        logger.error(f"[{user_id}] Brain Critical Error: {e}")
        await update.message.reply_text("😵 Технічна помилка.", parse_mode="Markdown")
        return

    status = analysis.get("status")
    reply = analysis.get("bot_reply")
    data = analysis.get("extracted_data", {})

    logger.info(f"[{user_id}] AI Status: {status} | Tmpl: {analysis.get('selected_template_name')}")
    session['history'].append({"role": "model", "content": reply})

    # 1. ЗМІНА ДАНИХ
    if status == "PROFILE_UPDATE":
        updates = data
        if updates:
            key, val = list(updates.items())[0]

            # --- НОВА ЛОГІКА СПЕЦІАЛЬНОСТЕЙ ---

            # 1. Заборона прямого редагування спеціальності (вона автоматична)
            if key == "SPECIALTY":
                await update.message.reply_text(
                    "⚠️ Спеціальність змінюється автоматично при зміні групи.\n"
                    "Будь ласка, змініть назву групи, і спеціальність оновиться сама.",
                    parse_mode="Markdown"
                )
                return

            # 2. Якщо змінюється ГРУПА -> Оновлюємо також і спеціальність (візуально для юзера)
            extra_msg = ""
            if key == "GROUP":
                new_spec = get_specialty_from_group(val)
                if new_spec:
                    # Оновлюємо в RAM (в базу писати не треба, там формула сама порахує)
                    session['profile']['SPECIALTY'] = new_spec
                    extra_msg = f"\n🎓 Спеціальність автоматично визначено: **{new_spec}**"
                else:
                    extra_msg = "\n⚠️ Не вдалося автоматично визначити спеціальність. Перевірте формат групи."

            # Запис в базу
            if sheet_mgr.update_student_field(user_id, key, val):
                session['profile'][key] = val

                # Формуємо відповідь
                ua_names = {"GROUP": "групу", "STUDENTS_PHONE": "телефон", "PARENTS_PHONE": "телефон представника"}
                readable_key = ua_names.get(key, key)

                await update.message.reply_text(f"✅ Змінено {readable_key} на: **{val}**{extra_msg}",
                                                parse_mode="Markdown")

                if reply: await update.message.reply_text(reply, parse_mode="Markdown")
            else:
                await update.message.reply_text("❌ Помилка запису в базу.", parse_mode="Markdown")
        return

    # 2. ДІАЛОГ
    if status in ["CLARIFICATION_NEEDED", "TEMPLATE_SELECTED", "WAITING_FOR_CONFIRMATION"]:
        if reply: await update.message.reply_text(reply, parse_mode="Markdown")
        if status == "TEMPLATE_SELECTED":
            session['active_template'] = analysis.get("selected_template_name")
            session['msg_count'] = 0
        return


    # 3. ГЕНЕРАЦІЯ
    elif status == "READY_TO_GENERATE":
        tmpl_name = session.get('active_template') or analysis.get("selected_template_name")

        if not tmpl_name:
            await update.message.reply_text("⚠️ Шаблон втрачено. Уточніть назву.", parse_mode="Markdown")
            return

        tmpl_obj = next((t for t in templates if t['name'] == tmpl_name), None)
        if not tmpl_obj:
            await update.message.reply_text("❌ Файл шаблону не знайдено.", parse_mode="Markdown")
            return

        status_msg = await update.message.reply_text("⏳ Генерую документ...", parse_mode="Markdown")

        try:
            # 1. Об'єднання даних (FIX з минулого кроку)
            full_data = session['profile'].copy()
            full_data.update(data)

            # 2. Генерація PDF
            await context.bot.send_chat_action(chat_id, action="upload_document")
            pdf_bytes = drive_mgr.create_pdf_from_template(tmpl_obj['id'], tmpl_name, full_data)

            pdf_file = io.BytesIO(pdf_bytes)
            clean_name = full_data.get('STUDENTS_NAME', 'Doc').replace(' ', '_')
            filename = f"Заява_{clean_name}.pdf"
            pdf_file.name = filename

            # 3. Відправка Email
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg.message_id,
                text="📤 Відправляю на пошту..."
            )

            email_body = f"Студент: {full_data.get('STUDENTS_NAME')}\nТип: {tmpl_name}"
            email_success = email_mgr.send_email(TARGET_PRINT_EMAIL, f"ДРУК: {filename}", email_body, pdf_file,
                                                 filename)

            # 4. Логування результату в Google Sheets (НОВЕ!)
            log_status = "✅ SUCCESS" if email_success else "❌ EMAIL FAILED"
            sheet_mgr.log_event(full_data, tmpl_name, log_status)

            # 5. Фінал для користувача
            if email_success:
                logger.info(f"[{user_id}] Email sent to print.")
                final_text = "✅ Готово! Заява на друці."
            else:
                logger.error(f"[{user_id}] Email failed.")
                final_text = "⚠️ Заяву згенеровано, але не вдалося відправити на друк."

            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg.message_id,
                text=final_text
            )

            pdf_file.seek(0)
            await update.message.reply_document(pdf_file, filename=filename, caption="Ваша копія 📄")

            # Очистка сесії
            session['history'] = []
            session['active_template'] = None
            logger.info(f"[{user_id}] Document Transaction Complete.")

        except Exception as e:
            logger.error(f"[{user_id}] Gen Error: {e}")
            # Логуємо помилку в таблицю також!
            sheet_mgr.log_event(session.get('profile', {}), tmpl_name, f"🔥 CRITICAL ERROR: {str(e)}")

            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=status_msg.message_id,
                text=f"❌ Помилка: {e}"
            )

    else:
        # Fallback
        if reply: await update.message.reply_text(reply, parse_mode="Markdown")


async def session_timeout_callback(context: ContextTypes.DEFAULT_TYPE):
    """Спрацьовує, якщо користувач мовчав 60 хвилин"""
    user_id = context.job.data
    chat_id = context.job.chat_id

    # Логування події "TIMEOUT" в таблицю
    if user_id in user_sessions:
        profile = user_sessions[user_id].get('profile', {})
        # Записуємо в Logs: "Session Timeout", статус "🕒 DELAY"
        sheet_mgr.log_event(profile, "Session Timeout", "🕒 DELAY")

        # Очищення сесії
        user_sessions[user_id]['history'] = []
        user_sessions[user_id]['active_template'] = None
        user_sessions[user_id]['msg_count'] = 0  # Скидаємо лічильник спаму

        logger.info(f"[{user_id}] Session Timeout (Cleaned history).")

        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text="⏳ **Сесію завершено через неактивність (60 хв).**\n"
                     "Якщо бажаєте створити нову заяву — просто напишіть запит знову.",
                parse_mode="Markdown"
            )
        except:
            pass


def reset_timeout_timer(user_id, chat_id, context):
    """Скидає таймер на 60 хвилин (3600 сек)"""
    jobs = context.job_queue.get_jobs_by_name(str(user_id))
    for job in jobs: job.schedule_removal()

    context.job_queue.run_once(
        session_timeout_callback,
        3600,  # <--- ЗМІНЕНО НА 60 ХВИЛИН
        chat_id=chat_id,
        name=str(user_id),
        data=user_id
    )


async def post_init(application: Application):
    commands = [
        BotCommand("start", "🏠 Головна"),
        BotCommand("newdoc", "📝 Нова заява"),
        BotCommand("edit", "✏️ Редагувати дані"),
        BotCommand("cancel", "❌ Скасувати / Назад"),
        BotCommand("help", "ℹ️ Допомога")
    ]
    await application.bot.set_my_commands(commands)

    mode = os.getenv("MODE", "PROD")  # За замовчуванням PROD
    admin_id = os.getenv("ADMIN_ID")  # Ваш ID з .env

    if mode == "DEV" and admin_id:
        try:
            await application.bot.send_message(
                chat_id=admin_id,
                text="👨‍💻 **УВАГА:** Бот запущено в локальному (DEV) режимі!",
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.warning(f"Не вдалося надіслати сповіщення адміну: {e}")

    logger.info("✅ Bot Started.")


async def newdoc_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Ця команда просто скидає контекст і пропонує почати
    user_id = str(update.effective_user.id)
    if user_id in user_sessions:
        user_sessions[user_id]['history'] = []
        user_sessions[user_id]['active_template'] = None

    await update.message.reply_text(
        "📝 **Створення нового документа**\n\n"
        "Яку заяву ви хочете оформити? (Наприклад: *\"Пропуск занять\"*, *\"Академвідпустка\"* тощо)",
        parse_mode="Markdown"
    )


async def edit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Вмикає режим редагування, не стираючи контекст поточної заяви"""
    user_id = str(update.effective_user.id)

    # Ініціалізація сесії, якщо немає
    if user_id not in user_sessions:
        user_sessions[user_id] = {'history': [], 'profile': {}, 'active_template': None, 'mode': 'NORMAL'}

    # Вмикаємо режим редагування
    user_sessions[user_id]['mode'] = 'EDITING'

    """Інструкція для редагування"""
    await update.message.reply_text(
        "✏️ **Режим редагування активовано.**\n\n"
        "Зараз ви можете написати нові дані (наприклад: *\"Нова група КІПЗс-25-1\"* або *\"Мій телефон 050...\"*).\n"
        "Редагування можливе лише у разі зміни номера телефону та групи. Інші поля відредаговані не будуть\n"
        "Бот сприйматиме це тільки як оновлення профілю.\n\n"
        "❌ Щоб завершити редагування і повернутися назад, натисніть /cancel"
        , parse_mode="Markdown")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "🤖 **Довідка по командам:**\n\n"
        "/start - Почати роботу / Головне меню\n"
        "/newdoc - Створити нову заяву\n"
        "/edit - Змінити дані (Групу, Номер телефону)\n"
        "/cancel - Скасувати дію або вийти з редагування\n\n"
        "ℹ️ *Як користуватися:*\n"
        "Просто напишіть мені, що вам потрібно (наприклад: *\"Хочу заяву на відрахування\"*). "
        "Я підготую документ, покажу вам ваші дані для перевірки, і якщо все ок — відправлю його на друк.\n\n"
        "Для отримання допомоги у роботі з ботом - звертайтеся у каб. 300 до Білоуса Святослава Олеговича або на пошту sviatoslav.bilous@ukd.edu.ua"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Розумна скасування: вихід з редагування АБО скидання діалогу"""
    user_id = str(update.effective_user.id)

    if user_id not in user_sessions:
        await update.message.reply_text("🤷‍♂️ Немає активних дій для скасування.")
        return

    session = user_sessions[user_id]
    current_mode = session.get('mode', 'NORMAL')

    # СЦЕНАРІЙ 1: Вихід з режиму редагування (повернення до заяви)
    if current_mode == 'EDITING':
        session['mode'] = 'NORMAL'

        # Перевіряємо, чи була активна заява до цього
        if session.get('active_template'):
            await update.message.reply_text(
                f"✅ Редагування завершено.\n"
                f"Повертаємось до оформлення документа: **{session['active_template']}**.\n"
                f"Що пишемо далі?"
            )
        else:
            await update.message.reply_text("✅ Редагування завершено. Чим можу допомогти ще?")

    # СЦЕНАРІЙ 2: Повне скидання (якщо ми не редагували, а просто спілкувались)
    else:
        session['history'] = []
        session['active_template'] = None
        session['msg_count'] = 0
        await update.message.reply_text(
            "🔄 **Діалог очищено.**\n"
            "Всі попередні контексти скинуто. Ви можете почати спочатку.",
            parse_mode="Markdown"
        )


if __name__ == "__main__":
    logger.info("🤖 Bot Starting...")
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).post_init(post_init).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("newdoc", newdoc_command))
    application.add_handler(CommandHandler("edit", edit_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(CommandHandler("help", help_command))

    # application.add_handler(MessageHandler(filters.TEXT | filters.CONTACT & (~filters.COMMAND), handle_message))
    application.add_handler(MessageHandler(filters.ALL & (~filters.COMMAND), handle_message))

    application.run_polling()
