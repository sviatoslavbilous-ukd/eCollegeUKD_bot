"""
eCollege UKD Telegram Bot
Повністю рефакторизована версія — модульна, схема-орієнтована, без "магічних рядків".

Архітектура:
  - StudentFields / SessionKeys / SheetConfig  — єдині джерела правди для всіх імен колонок і ключів
  - SchemaManager   — завантажує та кешує схему таблиці (заголовки → індекси)
  - SheetManager    — CRUD для Google Sheets, жодних хардкоджених індексів
  - DriveManager    — робота з Google Drive / Docs
  - EmailManager    — відправка через Gmail
  - GeminiBrain     — взаємодія з Gemini API
  - RegistrationFSM — скінченний автомат реєстрації
  - BotHandlers     — обробники команд і повідомлень
"""

# ── Стандартна бібліотека ────────────────────────────────────────────────────
import io
import re
import os
import json
import time
import base64
import logging
import datetime
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum, auto
from logging.handlers import TimedRotatingFileHandler
from typing import Any, Dict, List, Optional

# ── Зовнішні залежності ──────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv as _load_dotenv
    _HAS_DOTENV = True
except ImportError:
    _HAS_DOTENV = False

from telegram import Update, BotCommand, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder, ContextTypes, CommandHandler,
    MessageHandler, CallbackQueryHandler, filters, Application,
)

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google import genai
from google.genai import types
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# ════════════════════════════════════════════════════════════════════════════════
# 1.  КОНФІГУРАЦІЯ ТА КОНСТАНТИ
# ════════════════════════════════════════════════════════════════════════════════

# ════════════════════════════════════════════════════════════════════════════════
# 0.  ЗАВАНТАЖЕННЯ КОНФІГУРАЦІЇ — Secret Manager (prod) або .env (dev/local)
# ════════════════════════════════════════════════════════════════════════════════

_GCP_PROJECT = os.getenv("GCP_PROJECT", "ecollegebot")

# Імена всіх секретів, що зберігаються в Secret Manager
_SECRET_NAMES = [
    "TELEGRAM_TOKEN",
    "GEMINI_API_KEY",
    "GEMINI_AI_MODEL",
    "SPREADSHEET_ID",
    "TEMPLATES_FOLDER_ID",
    "TARGET_PRINT_EMAIL",
    "CLIENT_SECRET_FILE",
    "ADMIN_ID",
    "ADMIN_EMAIL",
    "UNKNOWN_NOTIF_COOLDOWN_MIN",
]


def _load_from_secret_manager() -> bool:
    """
    Завантажує секрети з Google Cloud Secret Manager у os.environ.
    Повертає True якщо успішно, False якщо недоступно (локальна розробка).
    Пропускає секрети, яких немає — щоб опційні поля не ламали старт.
    """
    try:
        from google.cloud import secretmanager as _sm
        client = _sm.SecretManagerServiceClient()
        loaded, skipped = [], []

        for name in _SECRET_NAMES:
            try:
                path     = f"projects/{_GCP_PROJECT}/secrets/{name}/versions/latest"
                response = client.access_secret_version(request={"name": path})
                value    = response.payload.data.decode("utf-8").strip()
                os.environ[name] = value
                loaded.append(name)
            except Exception:
                skipped.append(name)

        print(f"[SecretsLoader] ✅ Loaded: {loaded}")
        if skipped:
            print(f"[SecretsLoader] ⚠️  Skipped (not found): {skipped}")
        return True

    except ImportError:
        return False
    except Exception as exc:
        print(f"[SecretsLoader] ❌ Secret Manager unavailable: {exc}")
        return False


# Спочатку пробуємо Secret Manager, fallback — .env для локальної розробки
if not _load_from_secret_manager():
    if _HAS_DOTENV:
        _load_dotenv()
        print("[SecretsLoader] 📄 Loaded from .env (local dev mode)")
    else:
        print("[SecretsLoader] ⚠️  No secrets source available — relying on env vars")


class Env:
    """Усі змінні середовища в одному місці."""
    CLIENT_SECRET_FILE  = os.getenv("CLIENT_SECRET_FILE")
    TEMPLATES_FOLDER_ID = os.getenv("TEMPLATES_FOLDER_ID")
    SPREADSHEET_ID      = os.getenv("SPREADSHEET_ID")
    TARGET_PRINT_EMAIL  = os.getenv("TARGET_PRINT_EMAIL")
    TELEGRAM_TOKEN      = os.getenv("TELEGRAM_TOKEN")
    GEMINI_API_KEY      = os.getenv("GEMINI_API_KEY")
    GEMINI_AI_MODEL     = os.getenv("GEMINI_AI_MODEL", "gemini-2.0-flash")
    ADMIN_ID            = os.getenv("ADMIN_ID")           # Telegram ID розробника
    ADMIN_EMAIL         = os.getenv("ADMIN_EMAIL")        # Пошта розробника (опційно)
    MODE                = os.getenv("MODE", "PROD")
    # Мінімальний інтервал між сповіщеннями про невідомий запит від одного юзера (хвилини)
    UNKNOWN_NOTIF_COOLDOWN_MIN: int = int(os.getenv("UNKNOWN_NOTIF_COOLDOWN_MIN", "30"))


GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/gmail.send",
]


# ── Назви аркушів ────────────────────────────────────────────────────────────
class SheetName:
    STUDENTS      = "Students"
    CONFIG        = "Config"
    LOGS          = "Logs"       # Apps Script логи (незмінно)
    BOT_LOGS      = "Bot_Logs"   # Логи бота з аналітикою
    JOURNAL_LINKS = "Journal_Links"
    CHANGE_LOG    = "Change_Log"


# ── Назви колонок таблиці Students ──────────────────────────────────────────
class Col:
    """
    Єдине джерело правди для назв колонок у Students.
    Щоб перейменувати колонку — змінити рядок тут і більше ніде.
    """
    TELEGRAM_ID         = "TELEGRAM_ID"
    PARENTS_TELEGRAM_ID = "PARENTS_TELEGRAM_ID"
    EMAIL               = "STUDENTS_EMAIL"
    NAME                = "STUDENTS_NAME"
    GROUP               = "GROUP"
    BIRTH_DATE          = "BIRTH_DATE"
    PARENTS_NAME        = "PARENTS_NAME"
    APPLICATION_BASIS   = "APPLICATION_BASIS"
    SPECIALTY           = "SPECIALTY"
    STUDY_FORM          = "STUDY_FORM"
    STUDY_YEAR          = "STUDY_YEAR"
    STUDENTS_PHONE      = "STUDENTS_PHONE"
    PARENTS_PHONE       = "PARENTS_PHONE"
    SOCIAL_BENEFITS     = "SOCIAL_BENEFITS"
    WHO_RECOMMENDED     = "WHO_RECOMMENDED"
    IS_GROUP_LEADER     = "IS_GROUP_LEADER"
    IS_STUDCOUNCIL      = "IS_STUDCOUNCIL_MEMBER"
    TO_PAY              = "TO_PAY"
    ABSENCE_TIMES       = "ABSENCE_TIMES"

    # Поля, які дозволено редагувати через /edit
    EDITABLE: tuple = (STUDENTS_PHONE, PARENTS_PHONE, PARENTS_NAME)


# ── Поля профілю, що збираються під час онбордингу ──────────────────────────
# Порядок важливий — саме в такому порядку ставляться питання.
ONBOARDING_FIELDS: List[Dict[str, Any]] = [
    {
        "col":      Col.BIRTH_DATE,
        "prompt":   "📅 Введіть вашу дату народження (формат ДД.ММ.РРРР):",
        "use_button": False,
    },
    {
        "col":      Col.STUDENTS_PHONE,
        "prompt":   "📱 Натисніть кнопку нижче, щоб поділитися номером телефону:",
        "use_button": True,
    },
    {
        "col":      Col.PARENTS_NAME,
        "prompt":   "👤 Введіть ПІБ одного з представників (батьків / опікуна):",
        "use_button": False,
    },
    {
        "col":      Col.PARENTS_PHONE,
        "prompt":   "📱 Введіть контактний телефон представника (вручну, формат +380...):",
        "use_button": False,
    },
]

# Швидкий доступ: col → prompt
FIELD_PROMPT: Dict[str, str] = {f["col"]: f["prompt"] for f in ONBOARDING_FIELDS}


# ── Регулярні вирази ─────────────────────────────────────────────────────────
class Regex:
    DATE  = re.compile(r"^\d{2}\.\d{2}\.\d{4}$")
    PHONE = re.compile(r"^\+380\d{9}$")


# ── Ключі сесії ─────────────────────────────────────────────────────────────
class SK:
    HISTORY              = "history"
    PROFILE              = "profile"
    ACTIVE_TEMPLATE      = "active_template"
    MSG_COUNT            = "msg_count"
    BLOCKED_UNTIL        = "blocked_until"
    MODE                 = "mode"
    REG_STEP             = "reg_step"
    MISSING_FIELDS       = "missing_fields"
    LAST_UNKNOWN_NOTIF   = "last_unknown_notif"   # datetime останнього сповіщення розробника
    SESSION_START          = "session_start"           # datetime початку діалогу
    ANALYTICS_MSG_COUNT    = "analytics_msg_count"     # лічильник повідомлень для аналітики
    CLARIFICATION_COUNT    = "clarification_count"     # лічильник кроків уточнення
    TOTAL_STEPS            = "total_steps"             # загальна к-сть кроків для поточного шаблону
    AWAITING_PHONE_UPDATE  = "awaiting_phone_update"    # чекаємо підтвердження нового номера кнопкою
    AWAITING_EDIT_FIELD    = "awaiting_edit_field"      # яке поле чекає нового значення
    DOCS_COUNT             = "docs_count"               # к-сть заяв за поточну сесію (не скидається між заявами)


class RegStep(str, Enum):
    WAITING_EMAIL = "WAITING_EMAIL"
    WAITING_DATA  = "WAITING_DATA"
    COMPLETED     = "COMPLETED"


class BotMode(str, Enum):
    NORMAL   = "NORMAL"
    EDITING  = "EDITING"
    ONBOARDING = "ONBOARDING"


# ── UI-повідомлення ──────────────────────────────────────────────────────────
class UI:
    GOOGLE_OVERLOAD = (
        "😓 **Сервери Google зараз перевантажені.**\n"
        "Спробуйте повторити запит через 30–60 секунд."
    )
    UNKNOWN_ERROR = (
        "🛠️ **Виникла технічна несправність.**\n"
        "Помилку зафіксовано. Спробуйте, будь ласка, пізніше."
    )
    SAFETY_BLOCK = (
        "🛡️ **Спрацював фільтр безпеки.**\n"
        "Спробуйте сформулювати запит інакше."
    )
    NOT_UNDERSTOOD = (
        "🤔 **Я не зовсім зрозумів.**\n"
        "Спробуйте перефразувати або скористайтеся /start."
    )
    NON_TEXT = (
        "⚠️ **Наразі я — текстовий бот.**\n"
        "Надсилайте, будь ласка, лише текстові повідомлення ✍️"
    )
    SESSION_EXPIRED = (
        "⏳ **Сесію завершено через неактивність (60 хв).**\n"
        "Щоб створити нову заяву — просто напишіть запит знову."
    )
    SPAM_BLOCK = (
        "⛔ **Система визначила вашу поведінку як спам.**\n\n"
        "Надіслано занадто багато повідомлень без конкретної мети.\n"
        "⏳ Бот тимчасово заблокований для вас на 1 годину.\n"
        "Якщо є термінове питання — зверніться до адміністрації коледжу."
    )
    UNKNOWN_INTENT = (
        "🤔 **Я поки не вмію це робити.**\n\n"
        "Але я вже передав ваш запит розробнику — він врахує це при наступних оновленнях.\n"
        "Якщо питання термінове — зверніться до адміністрації коледжу (каб. 300)."
    )


# ════════════════════════════════════════════════════════════════════════════════
# 2.  НАЛАШТУВАННЯ ЛОГУВАННЯ
# ════════════════════════════════════════════════════════════════════════════════

def _build_logger() -> logging.Logger:
    os.makedirs("logs", exist_ok=True)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("eCollegeBot")
    log.setLevel(logging.INFO)

    fh = TimedRotatingFileHandler(
        "logs/bot_history.log", when="midnight", backupCount=30, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    log.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    log.addHandler(ch)
    return log


logger = _build_logger()


# ════════════════════════════════════════════════════════════════════════════════
# 3.  ВАЛІДАЦІЯ
# ════════════════════════════════════════════════════════════════════════════════

def validate_field(
    col: str,
    raw: str,
    profile: Optional[Dict[str, Any]] = None,
) -> tuple[bool, Optional[str]]:
    """
    Повертає (True, None) або (False, повідомлення про помилку).
    profile використовується для перехресних перевірок (напр. телефон батьків ≠ телефон студента).
    """
    value = raw.strip()

    if col == Col.BIRTH_DATE:
        if not Regex.DATE.match(value):
            return False, "⛔ Невірний формат дати. Потрібно `ДД.ММ.РРРР` (наприклад, 15.01.2005)."
        try:
            d, m, y = map(int, value.split("."))
            datetime.date(y, m, d)
        except ValueError:
            return False, "⛔ Такої дати не існує. Вкажіть коректну дату."

    elif col in (Col.STUDENTS_PHONE, Col.PARENTS_PHONE):
        if not Regex.PHONE.match(value):
            return False, "⛔ Невірний формат. Вкажіть телефон у форматі `+380XXXXXXXXX`."

        if col == Col.PARENTS_PHONE and profile:
            student_digits = re.sub(r"\D", "", str(profile.get(Col.STUDENTS_PHONE, "")))
            parent_digits  = re.sub(r"\D", "", value)
            if student_digits and student_digits[-9:] == parent_digits[-9:]:
                return False, (
                    "⛔ Номер представника збігається з вашим.\n"
                    "Введіть номер батька / матері / опікуна."
                )

    elif col == Col.PARENTS_NAME:
        parts = value.split()
        if len(parts) < 3:
            return False, "⛔ Потрібно ввести рівно 3 слова: Прізвище Ім'я По-батькові."
        if profile:
            student_words = set(profile.get(Col.NAME, "").lower().split())
            parent_words  = set(value.lower().split())
            if len(student_words & parent_words) >= 2:
                return False, "⛔ Ви ввели власне ім'я. Введіть ПІБ батька / матері / опікуна."

    return True, None


# ════════════════════════════════════════════════════════════════════════════════
# 4.  СХЕМА ТАБЛИЦІ (динамічне кешування заголовків)
# ════════════════════════════════════════════════════════════════════════════════

class SchemaCache:
    """
    Кешує mapping {назва_колонки: індекс} для кожного аркуша.
    При першому зверненні до аркуша завантажує заголовки з Google Sheets.
    Метод refresh() примусово оновлює кеш (викликати при зміні структури таблиці).
    """

    def __init__(self, sheets_service, spreadsheet_id: str):
        self._svc     = sheets_service
        self._sid     = spreadsheet_id
        self._cache: Dict[str, Dict[str, int]] = {}

    def get_index(self, sheet: str, col_name: str) -> Optional[int]:
        """Повертає 0-базований індекс колонки або None."""
        mapping = self._get_mapping(sheet)
        return mapping.get(col_name.strip().upper())

    def get_all_headers(self, sheet: str) -> List[str]:
        return list(self._get_mapping(sheet).keys())

    def refresh(self, sheet: Optional[str] = None) -> None:
        if sheet:
            self._cache.pop(sheet, None)
        else:
            self._cache.clear()

    def _get_mapping(self, sheet: str) -> Dict[str, int]:
        if sheet not in self._cache:
            result = (
                self._svc.spreadsheets()
                .values()
                .get(spreadsheetId=self._sid, range=f"{sheet}!1:1")
                .execute()
            )
            headers = result.get("values", [[]])[0]
            self._cache[sheet] = {h.strip().upper(): i for i, h in enumerate(headers)}
        return self._cache[sheet]


# ════════════════════════════════════════════════════════════════════════════════
# 5.  GOOGLE-МЕНЕДЖЕРИ
# ════════════════════════════════════════════════════════════════════════════════

class DriveManager:
    """Google Drive + Google Docs: копіювання шаблонів і генерація PDF."""

    _TEMPLATES_TTL_SEC = 1800  # 30 хвилин

    def __init__(self, creds: Credentials):
        self.creds        = creds
        self.drive_svc    = build("drive", "v3", credentials=creds)
        self.docs_svc     = build("docs", "v1", credentials=creds)
        self._templates_cache:    List[Dict[str, str]]       = []
        self._templates_cache_at: Optional[datetime.datetime] = None
        logger.info("✅ DriveManager ready.")

    def get_templates(self) -> List[Dict[str, str]]:
        now = datetime.datetime.now()
        if (
            self._templates_cache
            and self._templates_cache_at
            and (now - self._templates_cache_at).seconds < self._TEMPLATES_TTL_SEC
        ):
            return self._templates_cache
        try:
            res = (
                self.drive_svc.files()
                .list(
                    q=(
                        f"'{Env.TEMPLATES_FOLDER_ID}' in parents "
                        "and mimeType='application/vnd.google-apps.document' "
                        "and trashed=false"
                    ),
                    fields="files(id, name)",
                )
                .execute()
            )
            fresh = res.get("files", [])
            if fresh:
                self._templates_cache    = fresh
                self._templates_cache_at = now
                logger.info(f"[DriveManager] Templates cache updated: {len(fresh)} templates")
            return self._templates_cache or fresh
        except Exception as exc:
            logger.error(f"get_templates: {exc}")
            return self._templates_cache  # повертаємо кеш якщо є

    def create_pdf(self, template_id: str, template_name: str, data: Dict[str, Any]) -> bytes:
        """
        Копіює шаблон, замінює {{PLACEHOLDER}}-и та повертає PDF-байти.
        Тимчасову копію завжди видаляє у finally-блоці.
        """
        # Автоматичні технічні поля
        data.setdefault("date_of_signing", datetime.date.today().strftime("%d.%m.%Y"))
        self._fill_study_year(data)

        doc_id: Optional[str] = None
        try:
            copy = (
                self.drive_svc.files()
                .copy(
                    fileId=template_id,
                    body={"name": f"TEMP_{template_name}", "parents": [Env.TEMPLATES_FOLDER_ID]},
                )
                .execute()
            )
            doc_id = copy["id"]

            requests = []
            for key, value in data.items():
                safe = str(value) if value is not None else ""
                for variant in (key.lower(), key.upper(), key.capitalize()):
                    requests.append({
                        "replaceAllText": {
                            "containsText": {"text": f"{{{{{variant}}}}}", "matchCase": True},
                            "replaceText": safe,
                        }
                    })

            if requests:
                self.docs_svc.documents().batchUpdate(
                    documentId=doc_id, body={"requests": requests}
                ).execute()

            req = self.drive_svc.files().export_media(fileId=doc_id, mimeType="application/pdf")
            buf = io.BytesIO()
            dl  = MediaIoBaseDownload(buf, req)
            done = False
            while not done:
                _, done = dl.next_chunk()
            return buf.getvalue()

        finally:
            if doc_id:
                try:
                    self.drive_svc.files().delete(fileId=doc_id).execute()
                except Exception:
                    pass

    @staticmethod
    def _fill_study_year(data: Dict[str, Any]) -> None:
        if data.get("study_year"):
            return
        group = data.get("group") or data.get("GROUP", "")
        m = re.search(r"-(\d{2})-", str(group))
        if not m:
            return
        entry_year = 2000 + int(m.group(1))
        today      = datetime.date.today()
        course     = today.year - entry_year + (1 if today.month >= 9 else 0)
        if course > 0:
            data["study_year"] = str(course)


class SheetManager:
    """
    Повний CRUD для Google Sheets.
    Використовує SchemaCache — жодних хардкоджених індексів колонок.
    """

    def __init__(self, creds: Credentials, spreadsheet_id: str):
        self._sid   = spreadsheet_id
        self._svc   = build("sheets", "v4", credentials=creds)
        self.schema = SchemaCache(self._svc, spreadsheet_id)
        logger.info("✅ SheetManager ready.")

    # ── Утиліти ──────────────────────────────────────────────────────────────

    def _get_sheet_rows(self, sheet: str) -> List[List[Any]]:
        res = (
            self._svc.spreadsheets()
            .values()
            .get(spreadsheetId=self._sid, range=f"{sheet}!A:ZZ")
            .execute()
        )
        return res.get("values", [])

    def _col_letter(self, sheet: str, col_name: str) -> Optional[str]:
        idx = self.schema.get_index(sheet, col_name)
        if idx is None:
            return None
        # Підтримка до колонки ZZ (702 колонки)
        if idx < 26:
            return chr(ord("A") + idx)
        return chr(ord("A") + idx // 26 - 1) + chr(ord("A") + idx % 26)

    def _row_to_dict(self, headers: List[str], row: List[Any]) -> Dict[str, Any]:
        return {h: (row[i] if i < len(row) else "") for i, h in enumerate(headers)}

    def _find_row_by_field(
        self, sheet: str, col_name: str, value: str
    ) -> Optional[int]:
        """Повертає 1-базований номер рядка або None."""
        rows = self._get_sheet_rows(sheet)
        if not rows:
            return None
        idx = self.schema.get_index(sheet, col_name)
        if idx is None:
            return None
        target = str(value).strip().lower()
        for i, row in enumerate(rows[1:], start=2):
            cell = str(row[idx]).strip().lower() if idx < len(row) else ""
            if cell == target:
                return i
        return None

    # ── Публічний API ─────────────────────────────────────────────────────────

    def get_student_by_telegram_id(self, telegram_id: str) -> Optional[Dict[str, Any]]:
        rows = self._get_sheet_rows(SheetName.STUDENTS)
        if len(rows) < 2:
            return None
        headers = [h.strip().upper() for h in rows[0]]
        idx = self.schema.get_index(SheetName.STUDENTS, Col.TELEGRAM_ID)
        if idx is None:
            logger.warning("Column TELEGRAM_ID not found.")
            return None
        for row in rows[1:]:
            if idx < len(row) and str(row[idx]).strip() == str(telegram_id):
                return self._row_to_dict(headers, row)
        return None

    def get_student_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        rows = self._get_sheet_rows(SheetName.STUDENTS)
        if len(rows) < 2:
            return None
        headers = [h.strip().upper() for h in rows[0]]
        idx = self.schema.get_index(SheetName.STUDENTS, Col.EMAIL)
        if idx is None:
            logger.warning("Column STUDENTS_EMAIL not found.")
            return None
        target = email.strip().lower()
        for row in rows[1:]:
            cell = str(row[idx]).strip().lower() if idx < len(row) else ""
            if cell == target:
                return self._row_to_dict(headers, row)
        return None

    def link_telegram_id(self, email: str, telegram_id: str) -> bool:
        row_num = self._find_row_by_field(SheetName.STUDENTS, Col.EMAIL, email)
        if row_num is None:
            return False
        return self._write_cell(SheetName.STUDENTS, Col.TELEGRAM_ID, row_num, telegram_id)

    def update_field_by_telegram_id(self, telegram_id: str, col_name: str, value: Any) -> bool:
        row_num = self._find_row_by_field(SheetName.STUDENTS, Col.TELEGRAM_ID, telegram_id)
        if row_num is None:
            logger.warning(f"update_field: student {telegram_id} not found.")
            return False
        return self._write_cell(SheetName.STUDENTS, col_name, row_num, value)

    def load_specialties(self) -> Dict[str, str]:
        """Завантажує словник спеціальностей з аркуша Config."""
        try:
            rows = self._get_sheet_rows(SheetName.CONFIG)
            return {str(r[0]).strip(): str(r[1]).strip() for r in rows[1:] if len(r) >= 2 and r[0]}
        except Exception as exc:
            logger.error(f"load_specialties: {exc}")
            return {}

    def log_event(
        self,
        profile: Dict[str, Any],
        doc_type: str,
        status: str = "✅ SUCCESS",
        duration_sec: Optional[int] = None,
        msg_count: Optional[int] = None,
    ) -> None:
        now      = datetime.datetime.now()
        name     = profile.get(Col.NAME, "Невідомо")
        group    = profile.get(Col.GROUP, "—")
        tg_id    = profile.get(Col.TELEGRAM_ID, "—")
        dur_str  = f"{duration_sec // 60}хв {duration_sec % 60}с" if duration_sec is not None else "—"
        msg_str  = str(msg_count) if msg_count is not None else "—"
        row = [
            now.strftime("%d.%m.%Y"),
            now.strftime("%H:%M:%S"),
            name,
            group,
            doc_type,
            status,
            dur_str,
            msg_str,
            str(tg_id),
        ]
        try:
            self._svc.spreadsheets().values().append(
                spreadsheetId=self._sid,
                range=f"{SheetName.BOT_LOGS}!A:I",
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": [row]},
            ).execute()
        except Exception as exc:
            logger.error(f"log_event: {exc}")

    # ── Приватні ─────────────────────────────────────────────────────────────

    def _write_cell(self, sheet: str, col_name: str, row_num: int, value: Any) -> bool:
        letter = self._col_letter(sheet, col_name)
        if letter is None:
            logger.error(f"_write_cell: column {col_name} not found in {sheet}.")
            return False
        try:
            self._svc.spreadsheets().values().update(
                spreadsheetId=self._sid,
                range=f"{sheet}!{letter}{row_num}",
                valueInputOption="RAW",
                body={"values": [[str(value)]]},
            ).execute()
            logger.info(f"✏️  {col_name}[row {row_num}] → {value!r}")
            return True
        except Exception as exc:
            logger.error(f"_write_cell: {exc}")
            return False


class EmailManager:
    """Відправляє листи через Gmail API."""

    def __init__(self, creds: Credentials):
        self._svc = build("gmail", "v1", credentials=creds)
        logger.info("✅ EmailManager ready.")

    def send(
        self,
        to: str,
        subject: str,
        body: str,
        attachment: io.BytesIO,
        filename: str,
    ) -> bool:
        try:
            msg = MIMEMultipart()
            msg["to"]      = to
            msg["subject"] = subject
            msg.attach(MIMEText(body, "plain"))

            attachment.seek(0)
            part = MIMEApplication(attachment.read(), Name=filename)
            part["Content-Disposition"] = f'attachment; filename="{filename}"'
            msg.attach(part)
            attachment.seek(0)

            raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
            self._svc.users().messages().send(userId="me", body={"raw": raw}).execute()
            logger.info(f"📧 Email → {to} | {filename}")
            return True
        except Exception as exc:
            logger.error(f"EmailManager.send: {exc}")
            return False



# ════════════════════════════════════════════════════════════════════════════════
# 5б. DEV NOTIFIER — сповіщення розробника про невідомі запити
# ════════════════════════════════════════════════════════════════════════════════

class DevNotifier:
    """
    Надсилає розробнику сповіщення, коли студент задає запит,
    який бот не може обробити (статус UNKNOWN_INTENT).

    Канали:
      • Telegram (ADMIN_ID)  — завжди, якщо налаштовано
      • Email (ADMIN_EMAIL)  — опційно, якщо налаштовано

    Захист від флуду:
      • Cooldown per-user: не більше 1 сповіщення кожні UNKNOWN_NOTIF_COOLDOWN_MIN хвилин
        від одного й того самого студента.
      • Дедуплікація: якщо повідомлення ідентичне попередньому — не надсилати.
    """

    def __init__(self, email_mgr: "EmailManager"):
        self._email = email_mgr
        # {user_id: {"last_at": datetime, "last_text": str}}
        self._state: Dict[str, Dict[str, Any]] = {}

    async def notify(
        self,
        bot,
        user_id:  str,
        profile:  Dict[str, Any],
        message:  str,
        history:  List[Dict[str, str]],
    ) -> None:
        """
        Основний метод — викликати з handle_message при status == UNKNOWN_INTENT.
        bot — telegram.Bot instance (context.bot).
        """
        if not Env.ADMIN_ID and not Env.ADMIN_EMAIL:
            return  # нічого не налаштовано — тихо пропускаємо

        now   = datetime.datetime.now()
        state = self._state.get(user_id, {})

        # ── Cooldown ─────────────────────────────────────────────────────────
        last_at: Optional[datetime.datetime] = state.get("last_at")
        if last_at:
            elapsed = (now - last_at).total_seconds() / 60
            if elapsed < Env.UNKNOWN_NOTIF_COOLDOWN_MIN:
                logger.debug(f"[DevNotifier] cooldown active for {user_id}, skipping.")
                return

        # ── Дедуплікація ─────────────────────────────────────────────────────
        if state.get("last_text") == message.strip():
            logger.debug(f"[DevNotifier] duplicate message from {user_id}, skipping.")
            return

        self._state[user_id] = {"last_at": now, "last_text": message.strip()}

        # ── Формуємо текст сповіщення ────────────────────────────────────────
        name    = profile.get(Col.NAME,  "невідомо")
        group   = profile.get(Col.GROUP, "—")
        ts      = now.strftime("%d.%m.%Y %H:%M")

        # Останні 5 повідомлень діалогу для контексту
        ctx_lines = [
            f"  [{m['role'].upper()}]: {m['content']}"
            for m in history[-5:]
        ]
        ctx_block = "\n".join(ctx_lines) if ctx_lines else "  (порожньо)"

        tg_text = (
            f"🔔 *Невідомий запит студента*\n\n"
            f"👤 *{name}* | {group}\n"
            f"🆔 `{user_id}`\n"
            f"🕐 {ts}\n\n"
            f"💬 *Запит:*\n`{message}`\n\n"
            f"📜 *Контекст діалогу:*\n```\n{ctx_block}\n```"
        )

        email_body = (
            f"Студент: {name} ({group})\n"
            f"Telegram ID: {user_id}\n"
            f"Час: {ts}\n\n"
            f"Запит:\n{message}\n\n"
            f"Контекст (останні 5 повідомлень):\n{ctx_block}\n"
        )

        # ── Telegram ─────────────────────────────────────────────────────────
        if Env.ADMIN_ID:
            try:
                await bot.send_message(
                    chat_id=Env.ADMIN_ID,
                    text=tg_text,
                    parse_mode="Markdown",
                )
                logger.info(f"[DevNotifier] Telegram notif sent for user {user_id}.")
            except Exception as exc:
                logger.error(f"[DevNotifier] Telegram send failed: {exc}")

        # ── Email ────────────────────────────────────────────────────────────
        if Env.ADMIN_EMAIL:
            try:
                dummy_buf = io.BytesIO(email_body.encode("utf-8"))
                # Відправляємо як plain-text вкладення (зручно для архіву)
                self._email.send(
                    to       = Env.ADMIN_EMAIL,
                    subject  = f"[eCollege Bot] Невідомий запит — {name} — {ts}",
                    body     = email_body,
                    attachment = dummy_buf,
                    filename = f"unknown_request_{user_id}_{now.strftime('%Y%m%d_%H%M%S')}.txt",
                )
                logger.info(f"[DevNotifier] Email notif sent for user {user_id}.")
            except Exception as exc:
                logger.error(f"[DevNotifier] Email send failed: {exc}")

        # ── Logs sheet ───────────────────────────────────────────────────────
        # Записуємо в той самий аркуш Logs з окремим статусом
        sheet_mgr.log_event(profile, f"❓ UNKNOWN: {message[:80]}", "🔔 NOTIFIED DEV")

    async def notify_data_change(
        self,
        bot,
        user_id:   str,
        profile:   Dict[str, Any],
        field:     str,
        old_value: str,
        new_value: str,
    ) -> None:
        """Сповіщення адміна про зміну даних представника. Без cooldown."""
        if not Env.ADMIN_ID:
            return
        # Якщо профіль порожній — дочитуємо з бази
        if not profile.get(Col.NAME):
            try:
                fresh = sheet_mgr.get_student_by_telegram_id(user_id)
                if fresh:
                    profile = fresh
            except Exception:
                pass
        name  = profile.get(Col.NAME,  "невідомо")
        group = profile.get(Col.GROUP, "—")
        ts    = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
        field_labels = {
            Col.PARENTS_PHONE: "Номер представника",
            Col.PARENTS_NAME:  "ПІБ представника",
        }
        label = field_labels.get(field, field)
        text = (
            f"\U0001f514 Зміна даних представника\n"
            f"\U0001f464 {name} | {group}\n"
            f"\U0001f194 {user_id}\n"
            f"\U0001f550 {ts}\n\n"
            f"\U0001f4dd {label}:\n"
            f"  \u0411\u0443\u043b\u043e: {old_value or '(порожньо)'}\n"
            f"  \u0421\u0442\u0430\u043b\u043e: {new_value}\n\n"
            f"\u2139\ufe0f \u041f\u0435\u0440\u0435\u0432\u0456\u0440\u0442\u0435 \u0456 "
            f"\u043f\u0456\u0434\u0442\u0432\u0435\u0440\u0434\u0456\u0442\u044c \u0432\u0440\u0443\u0447\u043d\u0443 \u044f\u043a\u0449\u043e \u043f\u043e\u0442\u0440\u0456\u0431\u043d\u043e."
        )
        try:
            await bot.send_message(chat_id=int(Env.ADMIN_ID), text=text)
            logger.info(f"[DevNotifier] Data change notif sent for user {user_id}, field={field}")
        except Exception as exc:
            logger.error(f"[DevNotifier] Data change notify failed: {exc}")



TEMPLATE_CONFIG: Dict[str, Dict[str, Any]] = {
    "Заява про виготовлення студентського квитка": {
        "description": "Студент хоче отримати студентський квиток",
        "required_fields": [],
        "nuances": (
            "Вартість студентського квитка у 2025–2026 н.р. — 150 грн. "
            "Виготовлення займає до місяця."
        ),
    },
    "Заява про відпрацювання за індивідуальним графіком": {
        "description": "Студент бажає відпрацювати пропущені заняття не в очному форматі.",
        "required_fields": ["REASON"],
        "nuances": "Пишіть викладачам на корпоративну пошту (@ukd.edu.ua).",
    },
    "Заява про відрахування за власним бажанням": {
        "description": "Припинення навчання.",
        "required_fields": [],
        "nuances": (
            "Якщо відраховано всередині семестру без оплати — оплата все одно обов'язкова. "
            "Виписку оцінок можна отримати окремою заявою."
        ),
    },
    "Заява про дозвіл пропустити деякі пари впродовж конкретного дня": {
        "description": "Студент хоче пропустити одну або кілька пар протягом одного дня.",
        "required_fields": ["DATE_FROM", "LESSONS_RANGE", "REASON"],
        "nuances": "Пропущені пари обов'язково потрібно відпрацювати.",
    },
    "Заява про дозвіл пропустити пари в навчальний період": {
        "description": "Відсутність більше одного дня.",
        "required_fields": ["DATE_FROM", "DATE_TO", "REASON"],
        "nuances": (
            "Підтверджувальний документ: квиток на рейс або скан паспорта з печаткою. "
            "Пропущені пари обов'язково відпрацювати."
        ),
    },
    "Заява про надання академвідпустки": {
        "description": "Студент бажає призупинити навчання.",
        "required_fields": ["REASON"],
        "nuances": "При академвідпустці бронь від мобілізації втрачається. Тривалість — 1 рік.",
    },
    "Заява про отримання виписки оцінок": {
        "description": "Виписка оцінок до закінчення навчання.",
        "required_fields": ["DATE_FROM", "DATE_TO", "REASON"],
        "nuances": "Формується впродовж 5 робочих днів.",
    },
    "Заява про отримання індивідуального графіка навчання": {
        "description": "Індивідуальний графік (не форма) навчання.",
        "required_fields": ["SPECIALTY", "REASON"],
        "nuances": "За індивідуального графіка відвідування пар не обов'язкове.",
    },
    "Заява про перевід на денну форму навчання": {
        "description": "Перевід на денну форму.",
        "required_fields": ["SPECIALTY"],
        "nuances": "Перевід — з наступного семестру. Код групи зміниться на *с.",
    },
    "Заява про перевід на заочну форму навчання": {
        "description": "Перевід на заочну форму (3–4 курс).",
        "required_fields": ["SPECIALTY"],
        "nuances": "Перевід — з наступного семестру. Код групи зміниться на *з.",
    },
    "Заява про перевід на індивідуальну форму навчання": {
        "description": "Перевід на індивідуальну форму.",
        "required_fields": ["SPECIALTY"],
        "nuances": "Вартість індивідуальної форми вища, ніж індивідуального графіка.",
    },
    "Заява про перевід на іншу спеціальність": {
        "description": "Зміна спеціальності.",
        "required_fields": ["SUBJECT", "SPECIALTY_TO"],
        "nuances": "Перевід — з наступного семестру. Доплата уточнюється в бухгалтерії.",
    },
    "Заява про повторний курс у дистанційному форматі": {
        "description": "Ліквідація повторного курсу дистанційно.",
        "required_fields": ["SUBJECT", "REASON"],
        "nuances": "Для кожної дисципліни — окрема заява.",
    },
    "Заява про складання навчальної практики за індивідуальним графіком": {
        "description": "Неможливість складання навчальної практики очно.",
        "required_fields": ["REASON"],
        "nuances": "Потрібен оригінал підтверджувального документа у каб. 300.",
    },
    "Заява про складання сесії в усній формі у зв'язку з наявністю особливих освітніх потреб у студента": {
        "description": "Усна форма іспиту через особливі освітні потреби.",
        "required_fields": [],
        "nuances": "Необхідне підтвердження особливих освітніх потреб.",
    },
    "Заява про складання сесії за індивідуальним графіком": {
        "description": "Перенесення сесії через поважні причини.",
        "required_fields": ["SUBJECT", "REASON"],
        "nuances": "Оригінал довідки — у каб. 300.",
    },
}



CALLBACK_TEMPLATE_PREFIX = "tmpl:"
CALLBACK_CONFIRM         = "confirm:"   # confirm:yes / confirm:no
CALLBACK_EDIT_FIELD      = "edit:"      # edit:STUDENTS_PHONE / edit:PARENTS_PHONE / edit:PARENTS_NAME
CALLBACK_DONE            = "done:"      # done:new / done:bye

# Статичний fallback — використовується якщо Bot_Logs недоступний
_FALLBACK_TOP_TEMPLATES = [
    "Заява про дозвіл пропустити деякі пари впродовж конкретного дня",
    "Заява про дозвіл пропустити пари в навчальний період",
    "Заява про складання сесії за індивідуальним графіком",
    "Заява про отримання індивідуального графіка навчання",
]


class TopTemplatesCache:
    """
    Динамічно завантажує топ-5 шаблонів з аркуша Bot_Logs.
    При помилці повертає останній вдалий кеш або статичний fallback.
    Оновлюється не частіше ніж раз на UPDATE_INTERVAL_SEC.
    """
    UPDATE_INTERVAL_SEC = 3600  # 1 година

    def __init__(self):
        self._cache:      List[str]      = list(_FALLBACK_TOP_TEMPLATES)
        self._last_ok:    List[str]      = list(_FALLBACK_TOP_TEMPLATES)
        self._updated_at: Optional[datetime.datetime] = None
        self._svc = None   # ініціалізується при першому зверненні

    def _get_svc(self):
        if self._svc is None:
            from googleapiclient.discovery import build as _build
            self._svc = _build("sheets", "v4", credentials=_get_google_creds())
        return self._svc

    def get(self) -> List[str]:
        """Повертає топ-5 шаблонів. Оновлює кеш якщо минула година."""
        now = datetime.datetime.now()
        if self._updated_at and (now - self._updated_at).seconds < self.UPDATE_INTERVAL_SEC:
            return self._cache
        try:
            fresh = self._fetch()
            if fresh:
                self._cache   = fresh
                self._last_ok = fresh
                self._updated_at = now
                logger.info(f"[TopTemplatesCache] Updated: {fresh}")
            else:
                logger.warning("[TopTemplatesCache] Empty result, keeping cache")
                self._cache = self._last_ok
                self._updated_at = now
        except Exception as exc:
            logger.warning(f"[TopTemplatesCache] Fetch failed ({exc}), using cache")
            self._cache = self._last_ok
            self._updated_at = now
        return self._cache

    def _fetch(self) -> List[str]:
        """Читає Bot_Logs і повертає топ-5 шаблонів за кількістю SUCCESS."""
        svc    = self._get_svc()
        result = svc.spreadsheets().values().get(
            spreadsheetId=Env.SPREADSHEET_ID,
            range="Bot_Logs!E2:F5000",
        ).execute()
        rows = result.get("values", [])
        counts: Dict[str, int] = {}
        for row in rows:
            if len(row) < 2:
                continue
            tmpl, status = row[0].strip(), row[1].strip()
            if status == "✅ SUCCESS" and tmpl and tmpl in TEMPLATE_CONFIG:
                counts[tmpl] = counts.get(tmpl, 0) + 1
        if not counts:
            return []
        sorted_tmpls = sorted(counts, key=lambda k: counts[k], reverse=True)
        return sorted_tmpls[:4]


def _shorten_label(tmpl: str) -> str:
    """Скорочує назву шаблону до ~20 символів для кнопки."""
    replacements = {
        "Заява про дозвіл пропустити деякі пари впродовж конкретного дня": "Пропуск пар (1 день)",
        "Заява про дозвіл пропустити пари в навчальний період":            "Пропуск (кілька днів)",
        "Заява про складання сесії за індивідуальним графіком":            "Сесія за індив. графіком",
        "Заява про отримання індивідуального графіка навчання":            "Індив. графік навчання",
        "Заява про відрахування за власним бажанням":                      "Відрахування",
        "Заява про надання академвідпустки":                               "Академвідпустка",
        "Заява про отримання виписки оцінок":                              "Виписка оцінок",
        "Заява про виготовлення студентського квитка":                     "Студентський квиток",
        "Заява про перевід на денну форму навчання":                       "Перевід на денну форму",
        "Заява про перевід на заочну форму навчання":                      "Перевід на заочну форму",
        "Заява про перевід на індивідуальну форму навчання":               "Перевід на індивід. форму",
        "Заява про перевід на іншу спеціальність":                         "Зміна спеціальності",
        "Заява про повторний курс у дистанційному форматі":                "Повторний курс дистанційно",
        "Заява про складання навчальної практики за індивідуальним графіком": "Практика за індив. графіком",
        "Заява про відпрацювання за індивідуальним графіком":              "Відпрацювання за індив. графіком",
    }
    return replacements.get(tmpl, tmpl[:22])


class GeminiBrain:
    """Взаємодія з Gemini API."""

    _MAX_RETRIES = 5

    def __init__(self):
        self._client = genai.Client(api_key=Env.GEMINI_API_KEY)
        self._model  = Env.GEMINI_AI_MODEL
        logger.info("✅ GeminiBrain ready.")

    def analyze(
        self,
        history:          List[Dict[str, str]],
        templates:        List[Dict[str, str]],
        profile:          Dict[str, Any],
        active_template:  Optional[str] = None,
        mode:             str           = BotMode.NORMAL,
    ) -> Dict[str, Any]:

        templates_str = self._build_templates_block(templates)
        today         = datetime.date.today()
        semester      = "1" if today.month >= 8 else "2"
        year_base     = today.year if today.month >= 8 else today.year - 1
        chat_ctx      = "\n".join(f"{m['role']}: {m['content']}" for m in history)

        focus = ""
        if mode == BotMode.EDITING:
            focus = "⚠️ USER IS IN EDITING_MODE. Збирай ТІЛЬКИ Profile Data."
        elif active_template:
            focus = f"URGENT: Active template = '{active_template}'. Збирай ТІЛЬКИ необхідні поля."

        prompt = f"""
Ти — ШІ-адміністратор Фахового коледжу Університету Короля Данила.
Ідентифікуй запит, консультуй за правилами коледжу, збирай дані, відповідай JSON.

### КОНТЕКСТ
- Режим: {mode}
- Дата: {today.strftime('%d.%m.%Y')} | Рік: {year_base} | Семестр: {semester}
{focus}

### ШАБЛОНИ ТА ПРАВИЛА
{templates_str}

### ДАНІ СТУДЕНТА
{json.dumps(profile, ensure_ascii=False, indent=2)}

### ПРАВИЛА ОБРОБКИ
- ЛИШЕ поля зі схеми extracted_data — не вигадуй нових.
- ЗАВЖДИ запитуй РІВНО ОДНЕ поле за повідомлення. Не став два питання підряд.
- НЕ ПРОСИ завантажувати файли, фото, документи. Нагадуй принести оригінали фізично.
- REASON — коротка конструкція в орудному відмінку (напр. "сімейними обставинами").
- Відносні дати ("вчора", "минулого тижня") → конкретна "ДД.ММ.РРРР".
- СПОЧАТКУ підтвердження даних (WAITING_FOR_CONFIRMATION), потім READY_TO_GENERATE.
- При WAITING_FOR_CONFIRMATION форматуй дані для підтвердження ОБОВ'ЯЗКОВО з нового рядка
  для кожного пункту, наприклад:
  "Підтвердьте дані:\n👤 ПІБ: Іваненко Іван\n🎓 Група: КІПЗс-25-1\n📱 Телефон: +380..."
- selected_template_name ОБОВ'ЯЗКОВО передавай у відповіді при WAITING_FOR_CONFIRMATION
  та READY_TO_GENERATE — навіть якщо шаблон вже був обраний раніше.
- Дозволені оновлення профілю: STUDENTS_PHONE, PARENTS_PHONE, GROUP.
  При зміні GROUP — SPECIALTY оновлюється автоматично, не проси його окремо.
- UNKNOWN_INTENT: якщо запит студента НЕ стосується жодного шаблону, НЕ є
  оновленням профілю, і ти не можеш дати корисну відповідь на основі правил
  коледжу — відповідай статусом UNKNOWN_INTENT. bot_reply залишай порожнім (бот
  підставить стандартну фразу). Не використовуй UNKNOWN_INTENT для уточнень у
  межах вже обраного шаблону.

### ДІАЛОГ
{chat_ctx}

Відповідай ТІЛЬКИ JSON:
{{
  "status": "CLARIFICATION_NEEDED|PROFILE_UPDATE|TEMPLATE_SELECTED|WAITING_FOR_CONFIRMATION|READY_TO_GENERATE|UNKNOWN_INTENT",
  "bot_reply": "рядок українською (порожній рядок при UNKNOWN_INTENT)",
  "selected_template_name": "рядок або null",
  "extracted_data": {{
    "STUDENTS_NAME": "", "STUDENTS_PHONE": "", "STUDENTS_EMAIL": "",
    "DATE_OF_BIRTH": "", "SPECIALTY": "", "GROUP": "",
    "PARENTS_NAME": "", "PARENTS_PHONE": "",
    "DATE_FROM": "", "DATE_TO": "", "REASON": "",
    "SPECIALTY_TO": "", "SUBJECT": "",
    "DATE_OF_SIGNING": "", "STUDY_YEAR": "", "STUDY_SEMESTER": "",
    "LESSONS_RANGE": ""
  }}
}}
"""
        for attempt in range(self._MAX_RETRIES):
            try:
                resp = self._client.models.generate_content(
                    model=self._model,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        safety_settings=[
                            types.SafetySetting(category=c, threshold="BLOCK_NONE")
                            for c in [
                                "HARM_CATEGORY_HATE_SPEECH",
                                "HARM_CATEGORY_DANGEROUS_CONTENT",
                                "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                                "HARM_CATEGORY_HARASSMENT",
                            ]
                        ],
                    ),
                )

                if not resp.text:
                    return {"status": "CLARIFICATION_NEEDED", "bot_reply": "Порожня відповідь. Спробуйте ще."}

                raw = resp.text.strip().lstrip("```json").rstrip("```").strip()
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning(f"GeminiBrain: invalid JSON (attempt {attempt+1}), retrying. Raw: {raw[:100]}")
                    if attempt < self._MAX_RETRIES - 1:
                        time.sleep(1)
                        continue
                    return {"status": "CLARIFICATION_NEEDED", "bot_reply": UI.UNKNOWN_ERROR}

            except Exception as exc:
                err = str(exc)
                if "503" in err or "overloaded" in err or "429" in err:
                    if attempt < self._MAX_RETRIES - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return {"status": "CLARIFICATION_NEEDED", "bot_reply": UI.GOOGLE_OVERLOAD}
                logger.error(f"GeminiBrain.analyze: {exc}")
                return {"status": "CLARIFICATION_NEEDED", "bot_reply": UI.UNKNOWN_ERROR}

        return {"status": "CLARIFICATION_NEEDED", "bot_reply": UI.UNKNOWN_ERROR}

    @staticmethod
    def _build_templates_block(templates: List[Dict[str, str]]) -> str:
        blocks = []
        for t in templates:
            cfg = TEMPLATE_CONFIG.get(t["name"], {})
            req = cfg.get("required_fields", [])
            vars_note = (
                f"COLLECT ONLY: {', '.join(req)} + Profile Data."
                if req
                else "Collect ONLY standard Profile Data."
            )
            blocks.append(
                f"TEMPLATE: '{t['name']}'\n"
                f"  Опис: {cfg.get('description', '')}\n"
                f"  Правила: {cfg.get('nuances', '')}\n"
                f"  {vars_note}"
            )
        return "\n\n".join(blocks)


# ════════════════════════════════════════════════════════════════════════════════
# 7.  УПРАВЛІННЯ СЕСІЯМИ
# ════════════════════════════════════════════════════════════════════════════════

def _default_session() -> Dict[str, Any]:
    return {
        SK.HISTORY:             [],
        SK.PROFILE:             {},
        SK.ACTIVE_TEMPLATE:     None,
        SK.MSG_COUNT:           0,
        SK.BLOCKED_UNTIL:       None,
        SK.MODE:                BotMode.NORMAL,
        SK.REG_STEP:            None,
        SK.MISSING_FIELDS:      [],
        SK.LAST_UNKNOWN_NOTIF:  None,   # datetime останнього сповіщення розробника
        SK.SESSION_START:         None,   # datetime початку діалогу
        SK.ANALYTICS_MSG_COUNT:   0,      # лічильник повідомлень
        SK.CLARIFICATION_COUNT:   0,      # лічильник кроків уточнення
        SK.TOTAL_STEPS:           0,      # загальна к-сть кроків
        SK.AWAITING_PHONE_UPDATE: False,  # чи чекаємо кнопку з номером
        SK.AWAITING_EDIT_FIELD:   None,   # яке поле редагується зараз
        SK.DOCS_COUNT:            0,      # к-сть згенерованих заяв
    }


class SessionStore:
    """In-memory сховище сесій із ледачою ініціалізацією."""

    def __init__(self):
        self._store: Dict[str, Dict[str, Any]] = {}

    def get(self, user_id: str) -> Dict[str, Any]:
        if user_id not in self._store:
            self._store[user_id] = _default_session()
        # Дозаповнюємо поля, що могли з'явитися пізніше
        session = self._store[user_id]
        for k, v in _default_session().items():
            session.setdefault(k, v)
        return session

    def reset_dialog(self, user_id: str) -> None:
        s = self.get(user_id)
        s[SK.HISTORY]              = []
        s[SK.ACTIVE_TEMPLATE]      = None
        s[SK.MSG_COUNT]            = 0
        s[SK.SESSION_START]          = None
        s[SK.ANALYTICS_MSG_COUNT]    = 0
        s[SK.CLARIFICATION_COUNT]    = 0
        s[SK.TOTAL_STEPS]            = 0
        s[SK.AWAITING_PHONE_UPDATE]  = False
        s[SK.AWAITING_EDIT_FIELD]    = None
        s["tmpl_msg_count"]          = 0

    def __contains__(self, item: str) -> bool:
        return item in self._store


sessions = SessionStore()


# ════════════════════════════════════════════════════════════════════════════════
# 8.  ДОПОМІЖНІ ФУНКЦІЇ
# ════════════════════════════════════════════════════════════════════════════════

def get_missing_onboarding_fields(profile: Dict[str, Any]) -> List[str]:
    """Повертає список колонок, де значення порожнє або відсутнє."""
    missing = []
    for f in ONBOARDING_FIELDS:
        val = profile.get(f["col"])
        if not val or str(val).strip() == "":
            missing.append(f["col"])
    return missing


def resolve_specialty(group: str, specialties: Dict[str, str]) -> Optional[str]:
    """КБс-25-1 → 'Будівництво та цивільна інженерія'."""
    m = re.search(r"^К(.+?)[сзідСЗІД]-", group.strip())
    if not m:
        return None
    code = m.group(1).strip()
    for key, val in specialties.items():
        if key.lower() == code.lower():
            return val
    return None


async def ask_onboarding_field(update: Update, col: str) -> None:
    info  = next((f for f in ONBOARDING_FIELDS if f["col"] == col), None)
    if not info:
        return
    if info["use_button"]:
        markup = ReplyKeyboardMarkup(
            [[KeyboardButton("📱 Поділитися номером", request_contact=True)]],
            one_time_keyboard=True,
            resize_keyboard=True,
        )
    else:
        markup = ReplyKeyboardRemove()
    await update.message.reply_text(info["prompt"], reply_markup=markup)


def reset_timeout(user_id: str, chat_id: int, context: ContextTypes.DEFAULT_TYPE) -> None:
    for job in context.job_queue.get_jobs_by_name(user_id):
        job.schedule_removal()
    context.job_queue.run_once(
        _timeout_callback,
        when=3600,
        chat_id=chat_id,
        name=user_id,
        data=user_id,
    )


async def _timeout_callback(context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = context.job.data
    chat_id = context.job.chat_id
    s = sessions.get(user_id)
    _dur = int((datetime.datetime.now() - s[SK.SESSION_START]).total_seconds()) if s.get(SK.SESSION_START) else None
    _msg = s.get(SK.ANALYTICS_MSG_COUNT)
    sheet_mgr.log_event(s.get(SK.PROFILE, {}), "Session Timeout", "🕒 DELAY", duration_sec=_dur, msg_count=_msg)
    sessions.reset_dialog(user_id)
    logger.info(f"[{user_id}] Session timeout.")
    try:
        kb_restart = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "\U0001f504 \u041f\u043e\u0447\u0430\u0442\u0438 \u0437\u043d\u043e\u0432\u0443",
                callback_data=f"{CALLBACK_DONE}restart"
            ),
        ]])
        await context.bot.send_message(
            chat_id=chat_id,
            text=UI.SESSION_EXPIRED,
            parse_mode="Markdown",
            reply_markup=kb_restart,
        )
    except Exception:
        pass


# ════════════════════════════════════════════════════════════════════════════════
# 9.  ОБРОБНИКИ РЕЄСТРАЦІЇ (FSM)
# ════════════════════════════════════════════════════════════════════════════════

async def _finish_registration(update: Update, session: Dict[str, Any]) -> None:
    session[SK.REG_STEP]       = RegStep.COMPLETED
    session[SK.MISSING_FIELDS] = []
    name = session[SK.PROFILE].get(Col.NAME, "")
    await update.message.reply_text(
        f"🎉 **Профіль готовий!** Вітаємо, {name}!\n"
        f"👤 /mydata — переглянути дані, /edit — змінити дані\n\n"
        f"📝 Яку заяву оформити?",
        reply_markup=_make_template_keyboard(),
        parse_mode="Markdown",
    )


async def handle_registration(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = str(update.effective_user.id)
    session = sessions.get(user_id)
    text    = (update.message.text or "").strip()
    step    = session.get(SK.REG_STEP)

    # ── Нова людина або сесія після рестарту бота ────────────────────────────
    if step is None:
        try:
            student = sheet_mgr.get_student_by_telegram_id(user_id)
        except Exception:
            student = None

        if student:
            # Відомий студент — відновлюємо сесію
            session[SK.PROFILE]  = student
            session[SK.REG_STEP] = RegStep.COMPLETED
            missing = get_missing_onboarding_fields(student)
            if missing:
                session[SK.REG_STEP]       = RegStep.WAITING_DATA
                session[SK.MISSING_FIELDS] = missing
                await update.message.reply_text("⚠️ **Потрібне оновлення даних.**", parse_mode="Markdown")
                await ask_onboarding_field(update, missing[0])
            else:
                name = student.get(Col.NAME, "")
                await update.message.reply_text(
                    f"👋 З поверненням, {name}!\n👤 /mydata — переглянути дані, /edit — змінити дані\n\n📝 Яку заяву оформити?",
                    parse_mode="Markdown",
                    reply_markup=_make_template_keyboard(),
                )
        else:
            # Незнайома людина — запускаємо реєстрацію
            session[SK.REG_STEP] = RegStep.WAITING_EMAIL
            await update.message.reply_text(
                "👋 **Вітаю в системі E-College UKD!**\n\n"
                "🔐 Для початку роботи введіть **корпоративну пошту** (`mail@ukd.edu.ua`):",
                parse_mode="Markdown",
                reply_markup=ReplyKeyboardRemove(),
            )
        return

    # ── Крок 1: email ────────────────────────────────────────────────────────
    if step == RegStep.WAITING_EMAIL:
        email = text.lower()
        if not email.endswith("@ukd.edu.ua"):
            await update.message.reply_text(
                "⛔ **Доступ заборонено.**\n"
                "Приймається тільки корпоративна пошта `@ukd.edu.ua`.\nВведіть коректну адресу:",
                parse_mode="Markdown",
            )
            return

        wait = await update.message.reply_text("🔎 Перевіряю в базі даних…")
        student = sheet_mgr.get_student_by_email(email)

        if not student:
            await wait.edit_text(
                "❌ **Користувача не знайдено.**\n"
                "Перевірте пошту або зверніться в каб. 300.",
                parse_mode="Markdown",
            )
            return

        # ── Перевірка конфлікту Telegram ID ─────────────────────────────────
        existing_id = str(student.get(Col.TELEGRAM_ID, "")).strip()
        if existing_id and existing_id != user_id:
            # Цей акаунт вже прив'язаний до іншого Telegram
            await wait.edit_text(
                "⛔ **Доступ заборонено.**\n\n"
                "Цей обліковий запис вже прив'язаний до іншого Telegram-акаунту.\n"
                "Якщо це ваш акаунт — зверніться до адміністрації коледжу (каб. 300).",
                parse_mode="Markdown",
            )
            # Сповіщення адміна
            name = student.get(Col.NAME, "невідомо")
            group = student.get(Col.GROUP, "—")
            ts = datetime.datetime.now().strftime("%d.%m.%Y %H:%M")
            if Env.ADMIN_ID:
                try:
                    await context.bot.send_message(
                        chat_id=int(Env.ADMIN_ID),
                        text=(
                            f"⚠️ *Спроба захоплення акаунту*\n\n"
                            f"👤 {name} | {group}\n"
                            f"📧 Пошта: `{email}`\n"
                            f"🆔 Існуючий ID: `{existing_id}`\n"
                            f"🆔 Спроба з ID: `{user_id}`\n"
                            f"🕐 {ts}"
                        ),
                        parse_mode="Markdown",
                    )
                except Exception:
                    pass
            return

        sheet_mgr.link_telegram_id(email, user_id)
        session[SK.PROFILE] = student
        await wait.delete()

        missing = get_missing_onboarding_fields(student)
        if not missing:
            await _finish_registration(update, session)
        else:
            session[SK.REG_STEP]       = RegStep.WAITING_DATA
            session[SK.MISSING_FIELDS] = missing
            name = student.get(Col.NAME, "")
            await update.message.reply_text(
                f"👋 Вітаю, {name}!\nЗаповніть відсутні дані для завершення налаштування."
            )
            await ask_onboarding_field(update, missing[0])
        return

    # ── Крок 2: збір даних ───────────────────────────────────────────────────
    if step == RegStep.WAITING_DATA:
        missing = session.get(SK.MISSING_FIELDS, [])
        if not missing:
            await _finish_registration(update, session)
            return

        col_name     = missing[0]
        value_to_save = None

        if col_name == Col.STUDENTS_PHONE:
            if update.message.contact:
                phone = update.message.contact.phone_number
                value_to_save = phone if phone.startswith("+") else f"+{phone}"
            else:
                markup = ReplyKeyboardMarkup(
                    [[KeyboardButton("📱 Поділитися номером", request_contact=True)]],
                    one_time_keyboard=True, resize_keyboard=True,
                )
                await update.message.reply_text(
                    "⛔ Потрібно натиснути кнопку **«📱 Поділитися номером»** нижче.",
                    reply_markup=markup, parse_mode="Markdown",
                )
                return
        else:
            if not text:
                await update.message.reply_text("⚠️ Надішліть текстову відповідь.")
                return
            ok, err = validate_field(col_name, text, session[SK.PROFILE])
            if not ok:
                await update.message.reply_text(err, parse_mode="Markdown")
                return
            value_to_save = text

        wait = await update.message.reply_text("💾 Записую…", reply_markup=ReplyKeyboardRemove())
        if sheet_mgr.update_field_by_telegram_id(user_id, col_name, value_to_save):
            session[SK.PROFILE][col_name] = value_to_save
            session[SK.MISSING_FIELDS].pop(0)
            await wait.delete()

            if session[SK.MISSING_FIELDS]:
                await update.message.reply_text("✅ Прийнято.")
                await ask_onboarding_field(update, session[SK.MISSING_FIELDS][0])
            else:
                await _finish_registration(update, session)
        else:
            await wait.edit_text("❌ Помилка запису. Спробуйте ще раз.")


# ════════════════════════════════════════════════════════════════════════════════
# 10.  ОБРОБНИКИ КОМАНД
# ════════════════════════════════════════════════════════════════════════════════


def _make_template_keyboard() -> InlineKeyboardMarkup:
    """Inline-клавіатура з топ-5 шаблонів (з кешу) + кнопка вільного вводу."""
    buttons = []
    row     = []
    for idx, tmpl in enumerate(top_templates_cache.get()):
        label = _shorten_label(tmpl)
        row.append(InlineKeyboardButton(label, callback_data=f"{CALLBACK_TEMPLATE_PREFIX}{idx}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(
        "✏️ Написати свій варіант",
        callback_data=CALLBACK_TEMPLATE_PREFIX + "__custom__"
    )])
    return InlineKeyboardMarkup(buttons)


def _friendly_google_error(exc: Exception) -> str:
    msg = str(exc)
    if "503" in msg or "unavailable" in msg.lower():
        return "\u26a0\ufe0f \u0421\u0435\u0440\u0432\u0456\u0441 Google \u0442\u0438\u043c\u0447\u0430\u0441\u043e\u0432\u043e \u043d\u0435\u0434\u043e\u0441\u0442\u0443\u043f\u043d\u0438\u0439.\n\n\u0411\u0443\u0434\u044c \u043b\u0430\u0441\u043a\u0430, \u043d\u0430\u0442\u0438\u0441\u043d\u0456\u0442\u044c /start \u0456 \u0441\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 \u0449\u0435 \u0440\u0430\u0437. \u042f\u043a\u0449\u043e \u043d\u0435 \u0434\u043e\u043f\u043e\u043c\u043e\u0433\u043b\u043e \u2014 \u0437\u0430\u0447\u0435\u043a\u0430\u0439\u0442\u0435 5\u201310 \u0445\u0432\u0438\u043b\u0438\u043d \u0456 \u0441\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 \u0437\u043d\u043e\u0432\u0443. \u041f\u0440\u043e\u0431\u043b\u0435\u043c\u0430 \u043d\u0430 \u0441\u0442\u043e\u0440\u043e\u043d\u0456 Google, \u043d\u0435 \u0431\u043e\u0442\u0430."
    if "401" in msg or "403" in msg or "invalid_grant" in msg.lower():
        return "\U0001f510 \u041f\u043e\u043c\u0438\u043b\u043a\u0430 \u0434\u043e\u0441\u0442\u0443\u043f\u0443 \u0434\u043e Google.\n\n\u0417\u0432\u0435\u0440\u043d\u0456\u0442\u044c\u0441\u044f \u0434\u043e \u0430\u0434\u043c\u0456\u043d\u0456\u0441\u0442\u0440\u0430\u0442\u043e\u0440\u0430 (\u043a\u0430\u0431. 300, \u0411\u0456\u043b\u043e\u0443\u0441 \u0421\u0432\u044f\u0442\u043e\u0441\u043b\u0430\u0432 \u041e\u043b\u0435\u0433\u043e\u0432\u0438\u0447)."
    if "429" in msg or "quota" in msg.lower():
        return "\u23f3 \u041f\u0435\u0440\u0435\u0432\u0438\u0449\u0435\u043d\u043e \u043b\u0456\u043c\u0456\u0442 \u0437\u0430\u043f\u0438\u0442\u0456\u0432 \u0434\u043e Google.\n\n\u0417\u0430\u0447\u0435\u043a\u0430\u0439\u0442\u0435 1\u20132 \u0445\u0432\u0438\u043b\u0438\u043d\u0438 \u0456 \u0441\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 \u0437\u043d\u043e\u0432\u0443 (/start)."
    if "timeout" in msg.lower() or "deadline" in msg.lower():
        return "\u231b Google \u043d\u0435 \u0432\u0456\u0434\u043f\u043e\u0432\u0456\u0432 \u0432\u0447\u0430\u0441\u043d\u043e.\n\n\u041d\u0430\u0442\u0438\u0441\u043d\u0456\u0442\u044c /start \u0456 \u0441\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 \u0449\u0435 \u0440\u0430\u0437."
    return "\U0001f6e0\ufe0f \u0412\u0438\u043d\u0438\u043a\u043b\u0430 \u0442\u0435\u0445\u043d\u0456\u0447\u043d\u0430 \u043d\u0435\u0441\u043f\u0440\u0430\u0432\u043d\u0456\u0441\u0442\u044c.\n\n\u041d\u0430\u0442\u0438\u0441\u043d\u0456\u0442\u044c /start \u0456 \u0441\u043f\u0440\u043e\u0431\u0443\u0439\u0442\u0435 \u0449\u0435 \u0440\u0430\u0437. \u042f\u043a\u0449\u043e \u043f\u043e\u043c\u0438\u043b\u043a\u0430 \u043f\u043e\u0432\u0442\u043e\u0440\u044e\u0454\u0442\u044c\u0441\u044f \u2014 \u0437\u0432\u0435\u0440\u043d\u0456\u0442\u044c\u0441\u044f \u0434\u043e \u0430\u0434\u043c\u0456\u043d\u0456\u0441\u0442\u0440\u0430\u0442\u043e\u0440\u0430 (\u043a\u0430\u0431. 300)."

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = str(update.effective_user.id)
    session = sessions.get(user_id)
    reset_timeout(user_id, update.effective_chat.id, context)

    try:
        student = sheet_mgr.get_student_by_telegram_id(user_id)
    except Exception as exc:
        logger.error(f"[{user_id}] cmd_start Google error: {exc}")
        await update.message.reply_text(_friendly_google_error(exc), parse_mode="Markdown")
        return

    if student:
        missing = get_missing_onboarding_fields(student)
        session[SK.PROFILE] = student
        if missing:
            session[SK.REG_STEP]       = RegStep.WAITING_DATA
            session[SK.MISSING_FIELDS] = missing
            await update.message.reply_text("⚠️ **Потрібне оновлення даних.**", parse_mode="Markdown")
            await ask_onboarding_field(update, missing[0])
        else:
            session[SK.REG_STEP] = RegStep.COMPLETED
            name = student.get(Col.NAME, "")
            await update.message.reply_text(
                f"👋 З поверненням, {name}!\n👤 /mydata — переглянути дані, /edit — змінити дані\n\n📝 Яку заяву оформити?",
                parse_mode="Markdown",
                reply_markup=_make_template_keyboard(),
            )
    else:
        session[SK.REG_STEP] = RegStep.WAITING_EMAIL
        session[SK.PROFILE]  = {}
        await update.message.reply_text(
            "👋 **Вітаю в системі E-College UKD!**\n\n"
            "🔐 Для початку роботи введіть **корпоративну пошту** (`mail@ukd.edu.ua`):",
            parse_mode="Markdown",
            reply_markup=ReplyKeyboardRemove(),
        )



async def callback_template_select(update, context) -> None:
    """Обробник натискання inline-кнопки вибору шаблону."""
    query   = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    session = sessions.get(user_id)
    data    = query.data or ""

    if not data.startswith(CALLBACK_TEMPLATE_PREFIX):
        return

    key = data[len(CALLBACK_TEMPLATE_PREFIX):]

    if key == "__custom__":
        await query.edit_message_text(
            "✏️ Напишіть, з якої причини Вам потрібна заява.\nНаприклад: *«Декілька днів не буду на парах»*, *«Переводжусь на заочку»*",
            parse_mode="Markdown",
        )
        return

    # Розв'язуємо індекс → повна назва шаблону
    try:
        tmpl = top_templates_cache.get()[int(key)]
    except (ValueError, IndexError):
        await query.edit_message_text("❌ Помилка вибору. Напишіть назву заяви текстом.")
        return

    # Обираємо шаблон — передаємо у Gemini як звичайне повідомлення
    session[SK.ACTIVE_TEMPLATE] = tmpl
    session[SK.HISTORY].append({"role": "user", "content": tmpl})
    session[SK.SESSION_START]        = datetime.datetime.now()
    session[SK.ANALYTICS_MSG_COUNT]  = 1
    # Загальна кількість кроків
    _tmpl_cfg = TEMPLATE_CONFIG.get(tmpl, {})
    session[SK.TOTAL_STEPS]          = len(_tmpl_cfg.get("required_fields", [])) + 1
    session[SK.CLARIFICATION_COUNT]  = 0

    short = _shorten_label(tmpl)
    await query.edit_message_text(f"📋 Обрано: *{short}*", parse_mode="Markdown")

    # Запускаємо Gemini щоб отримати перше уточнення
    templates = drive_mgr.get_templates()
    analysis  = brain.analyze(
        session[SK.HISTORY],
        templates,
        session[SK.PROFILE],
        active_template=tmpl,
    )
    reply_txt = analysis.get("bot_reply", "")
    status    = analysis.get("status", "")
    tmpl_from_ai = analysis.get("selected_template_name")
    if tmpl_from_ai:
        session[SK.ACTIVE_TEMPLATE] = tmpl_from_ai
    session[SK.HISTORY].append({"role": "model", "content": reply_txt})
    logger.info(f"[{user_id}] callback status={status} | tmpl={tmpl}")

    if status == "READY_TO_GENERATE":
        await _handle_generate_from_callback(query, context, user_id, session, analysis.get("extracted_data") or {})
    elif reply_txt:
        await context.bot.send_message(query.message.chat_id, reply_txt, parse_mode="Markdown")


async def _handle_generate_from_callback(query, context, user_id, session, ai_data):
    """Генерація документа після вибору шаблону через кнопку."""
    msg = await context.bot.send_message(query.message.chat_id, "⏳ Генерую документ…")
    # Reuse existing generate logic by creating a minimal wrapper
    class _FakeUpdate:
        class _FakeMsg:
            def __init__(self, chat_id, bot):
                self.chat_id = chat_id
                self._bot = bot
            async def reply_text(self, text, **kwargs):
                return await self._bot.send_message(self.chat_id, text, **kwargs)
            async def reply_document(self, doc, **kwargs):
                return await self._bot.send_document(self.chat_id, doc, **kwargs)
        def __init__(self, chat_id, bot):
            self.message = self._FakeMsg(chat_id, bot)
            self.effective_chat = type("C", (), {"id": chat_id})()
    fake_update = _FakeUpdate(query.message.chat_id, context.bot)
    await msg.delete()
    await _handle_generate(fake_update, context, user_id, session, ai_data)



async def callback_confirm(update, context) -> None:
    query   = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    session = sessions.get(user_id)
    key     = (query.data or "").replace(CALLBACK_CONFIRM, "")

    # ── Перевірка чи сесія ще активна ────────────────────────────────────────
    if not session.get(SK.ACTIVE_TEMPLATE) or not session.get(SK.HISTORY):
        kb_restart = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "\U0001f504 \u0420\u043e\u0437\u043f\u043e\u0447\u0430\u0442\u0438 \u0437\u043d\u043e\u0432\u0443",
                callback_data=f"{CALLBACK_DONE}restart"
            ),
        ]])
        await query.edit_message_text(
            "\u23f3 \u0421\u0435\u0441\u0456\u044f \u0437\u0430\u0432\u0435\u0440\u0448\u0438\u043b\u0430\u0441\u044c \u0447\u0435\u0440\u0435\u0437 \u043d\u0435\u0430\u043a\u0442\u0438\u0432\u043d\u0456\u0441\u0442\u044c.\n"
            "\u0414\u0430\u043d\u0456 \u0437\u0430\u044f\u0432\u0438 \u0431\u0443\u043b\u043e \u0432\u0442\u0440\u0430\u0447\u0435\u043d\u043e \u2014 \u043f\u043e\u0442\u0440\u0456\u0431\u043d\u043e \u043f\u043e\u0447\u0430\u0442\u0438 \u0437\u043d\u043e\u0432\u0443.",
            reply_markup=kb_restart,
        )
        return

    if key == "no":
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(
            query.message.chat_id,
            "✏️ Напишіть, що саме треба змінити (наприклад: *телефон +380...*):".replace("\n",""),
            parse_mode="Markdown",
        )
        return

    # yes — запускаємо генерацію
    await query.edit_message_reply_markup(reply_markup=None)
    try:
        await query.edit_message_text(
            (query.message.text or "") + "\n\n✅ Підтверджено",
            parse_mode="Markdown",
        )
    except Exception:
        pass

    templates = drive_mgr.get_templates()
    confirm_msg = "так, дані вірні"
    analysis  = brain.analyze(
        session[SK.HISTORY] + [{"role": "user", "content": confirm_msg}],
        templates,
        session[SK.PROFILE],
        active_template=session.get(SK.ACTIVE_TEMPLATE),
    )
    session[SK.HISTORY].append({"role": "user",  "content": confirm_msg})
    session[SK.HISTORY].append({"role": "model", "content": analysis.get("bot_reply", "")})
    if analysis.get("selected_template_name"):
        session[SK.ACTIVE_TEMPLATE] = analysis["selected_template_name"]

    class _FU:
        class _FM:
            def __init__(self, cid, bot):
                self.chat_id = cid
                self._bot = bot
            async def reply_text(self, text, **kw):
                return await self._bot.send_message(self.chat_id, text, **kw)
            async def reply_document(self, doc, **kw):
                return await self._bot.send_document(self.chat_id, doc, **kw)
        def __init__(self, cid, bot):
            self.message = self._FM(cid, bot)
            self.effective_chat = type("C", (), {"id": cid})()

    fake = _FU(query.message.chat_id, context.bot)
    await _handle_generate(fake, context, user_id, session, analysis.get("extracted_data") or {})


async def callback_done(update, context) -> None:
    query   = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    session = sessions.get(user_id)
    key     = (query.data or "").replace(CALLBACK_DONE, "")

    if key == "restart":
        sessions.reset_dialog(user_id)
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(
            query.message.chat_id,
            "📝 Яку заяву оформити?",
            reply_markup=_make_template_keyboard(),
        )
        return

    if key == "new":
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(
            query.message.chat_id,
            "📝 Яку заяву оформити?",
            reply_markup=_make_template_keyboard(),
        )
    else:
        # bye
        sessions.reset_dialog(user_id)
        kb_restart = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Розпочати знову", callback_data=f"{CALLBACK_DONE}restart"),
        ]])
        await query.edit_message_text(
            "👋 До зустрічі! Якщо знадобиться допомога — чекаю.",
            reply_markup=kb_restart,
        )



async def cmd_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = str(update.effective_user.id)
    session = sessions.get(user_id)
    session[SK.MODE]                 = BotMode.EDITING
    session[SK.AWAITING_EDIT_FIELD]  = None
    session[SK.AWAITING_PHONE_UPDATE]= False
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("📱 Мій номер телефону",    callback_data=f"{CALLBACK_EDIT_FIELD}{Col.STUDENTS_PHONE}")],
        [InlineKeyboardButton("👪 Номер представника", callback_data=f"{CALLBACK_EDIT_FIELD}{Col.PARENTS_PHONE}")],
        [InlineKeyboardButton("👤 ПІБ представника",   callback_data=f"{CALLBACK_EDIT_FIELD}{Col.PARENTS_NAME}")],
        [InlineKeyboardButton("❌ Скасувати",                    callback_data=f"{CALLBACK_EDIT_FIELD}__cancel__")],
    ])
    await update.message.reply_text(
        "✏️ *Що хочете змінити?*",
        parse_mode="Markdown",
        reply_markup=kb,
    )


async def callback_edit_field(update, context) -> None:
    query   = update.callback_query
    await query.answer()
    user_id = str(query.from_user.id)
    session = sessions.get(user_id)
    col     = (query.data or "").replace(CALLBACK_EDIT_FIELD, "")

    if col == "__cancel__":
        session[SK.MODE] = BotMode.NORMAL
        await query.edit_message_text("✅ Редагування скасовано.")
        return

    session[SK.AWAITING_EDIT_FIELD] = col

    if col == Col.STUDENTS_PHONE:
        markup = ReplyKeyboardMarkup(
            [[KeyboardButton("📱 Підтвердити номер", request_contact=True)]],
            one_time_keyboard=True, resize_keyboard=True,
        )
        await query.edit_message_text("📱 Натисніть кнопку нижче.")
        await context.bot.send_message(query.message.chat_id, "📱 Підтвердження номера:", reply_markup=markup)
    elif col == Col.PARENTS_PHONE:
        await query.edit_message_text("📱 Введіть новий номер представника (+380XXXXXXXXX):")
    elif col == Col.PARENTS_NAME:
        await query.edit_message_text("👤 Введіть ПІБ представника (три слова):")


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = str(update.effective_user.id)
    session = sessions.get(user_id)
    mode    = session.get(SK.MODE, BotMode.NORMAL)

    if mode == BotMode.EDITING:
        session[SK.MODE] = BotMode.NORMAL
        tmpl = session.get(SK.ACTIVE_TEMPLATE)
        reply = (
            f"✅ Редагування завершено.\nПовертаємось до: **{tmpl}**."
            if tmpl
            else "✅ Редагування завершено."
        )
        await update.message.reply_text(reply, parse_mode="Markdown")
    else:
        sessions.reset_dialog(user_id)
        await update.message.reply_text(
            "🔄 **Діалог очищено.** Можете починати спочатку.",
            parse_mode="Markdown",
        )

async def cmd_mydata(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = str(update.effective_user.id)
    session = sessions.get(user_id)
    profile = session.get(SK.PROFILE, {})

    # Якщо профіль порожній — дочитуємо з бази
    if not profile.get(Col.NAME):
        try:
            fresh = sheet_mgr.get_student_by_telegram_id(user_id)
            if fresh:
                profile = fresh
                session[SK.PROFILE] = fresh
        except Exception as exc:
            await update.message.reply_text(_friendly_google_error(exc), parse_mode="Markdown")
            return

    if not profile:
        await update.message.reply_text(
            "ℹ️ Профіль не знайдено. Спробуйте /start для реєстрації.",
            parse_mode="Markdown"
        )
        return

    def _val(key):
        v = profile.get(key, "")
        return str(v).strip() if v else "—"

    lines = [
        f"👤 *Ваші дані в системі:*",
        "",
        f"📋 ПІБ: {_val(Col.NAME)}",
        f"🎓 Група: {_val(Col.GROUP)}",
        f"🏫 Спеціальність: {_val(Col.SPECIALTY)}",
        f"📅 Дата народження: {_val(Col.BIRTH_DATE)}",
        f"📱 Телефон: {_val(Col.STUDENTS_PHONE)}",
        f"📧 Пошта: {_val(Col.EMAIL)}",
        "",
        f"👪 Представник: {_val(Col.PARENTS_NAME)}",
        f"📱 Телефон представника: {_val(Col.PARENTS_PHONE)}",
        "",
        "✏️ Щоб змінити дані — /edit",
    ]
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")



async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🤖 **Довідка:**\n\n"
        "/start — головне меню / нова заява\n"
        "/mydata — переглянути мої дані\n"
        "/edit — змінити групу або телефон\n"
        "/cancel — скасувати / вийти з редагування\n\n"
        "Просто напишіть, що потрібно (наприклад: *«Заява на пропуск»*) — "
        "бот підготує документ і відправить на друк.\n\n"
        "Питання по роботі боту: каб. 300, Білоус Святослав Олегович\n"
        "📧 sviatoslav.bilous@ukd.edu.ua",
        parse_mode="Markdown",
    )


# ════════════════════════════════════════════════════════════════════════════════
# 11.  ГОЛОВНИЙ ОБРОБНИК ПОВІДОМЛЕНЬ
# ════════════════════════════════════════════════════════════════════════════════

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = str(update.effective_user.id)
    chat_id = update.effective_chat.id
    session = sessions.get(user_id)
    text    = (update.message.text or "").strip()

    # ── Очікуємо підтвердження нового номера телефону кнопкою ────────────────
    if (session.get(SK.AWAITING_PHONE_UPDATE) or session.get(SK.AWAITING_EDIT_FIELD) == Col.STUDENTS_PHONE) and update.message.contact:
        phone = update.message.contact.phone_number
        new_phone = phone if phone.startswith("+") else f"+{phone}"
        # Дочитуємо профіль якщо сесія порожня
        if not session.get(SK.PROFILE, {}).get(Col.NAME):
            try:
                fresh = sheet_mgr.get_student_by_telegram_id(user_id)
                if fresh:
                    session[SK.PROFILE] = fresh
            except Exception:
                pass
        ok, err = validate_field(Col.STUDENTS_PHONE, new_phone, session.get(SK.PROFILE, {}))
        if not ok:
            await update.message.reply_text(err, reply_markup=ReplyKeyboardRemove(), parse_mode="Markdown")
            session[SK.AWAITING_PHONE_UPDATE] = False
            session[SK.AWAITING_EDIT_FIELD]   = None
            return
        # Перевірка дублікату
        old_phone = str(session[SK.PROFILE].get(Col.STUDENTS_PHONE) or "")
        if old_phone and old_phone == new_phone:
            await update.message.reply_text(
                f"ℹ️ Цей номер вже збережено: **{new_phone}**\nЗміни не потрібно.",
                reply_markup=ReplyKeyboardRemove(), parse_mode="Markdown"
            )
            session[SK.AWAITING_PHONE_UPDATE] = False
            session[SK.AWAITING_EDIT_FIELD]   = None
            session[SK.MODE]                  = BotMode.NORMAL
            return
        session[SK.AWAITING_PHONE_UPDATE] = False
        session[SK.AWAITING_EDIT_FIELD]   = None
        session[SK.MODE]                  = BotMode.NORMAL
        await update.message.reply_text("💾 Записую…", reply_markup=ReplyKeyboardRemove())
        if sheet_mgr.update_field_by_telegram_id(user_id, Col.STUDENTS_PHONE, new_phone):
            session[SK.PROFILE][Col.STUDENTS_PHONE] = new_phone
            await update.message.reply_text(f"✅ Номер змінено: **{new_phone}**", parse_mode="Markdown")
        else:
            await update.message.reply_text("❌ Помилка запису. Спробуйте ще раз.")
        return

    # ── Очікуємо нове значення поля (редагування через кнопки) ──────────────
    awaiting_field = session.get(SK.AWAITING_EDIT_FIELD)
    if awaiting_field and awaiting_field != Col.STUDENTS_PHONE and text:
        session[SK.AWAITING_EDIT_FIELD] = None
        # Якщо STUDENTS_PHONE відсутній в сесії — дочитуємо з бази для коректної перехресної перевірки
        profile_for_validation = dict(session.get(SK.PROFILE, {}))
        if awaiting_field == Col.PARENTS_PHONE and not profile_for_validation.get(Col.STUDENTS_PHONE):
            try:
                fresh = sheet_mgr.get_student_by_telegram_id(user_id)
                if fresh:
                    profile_for_validation[Col.STUDENTS_PHONE] = fresh.get(Col.STUDENTS_PHONE, "")
            except Exception:
                pass
        ok, err = validate_field(awaiting_field, text, profile_for_validation)
        if not ok:
            await update.message.reply_text(err, parse_mode="Markdown")
            session[SK.AWAITING_EDIT_FIELD] = awaiting_field
            return
        wait = await update.message.reply_text("💾 Записую…")
        # Читаємо old_value ДО оновлення
        old_value = str(session.get(SK.PROFILE, {}).get(awaiting_field) or "")
        if not old_value:
            try:
                _fresh = sheet_mgr.get_student_by_telegram_id(user_id)
                if _fresh:
                    session[SK.PROFILE] = _fresh  # оновлюємо сесію
                old_value = str((_fresh or {}).get(awaiting_field) or "(порожньо)")
            except Exception:
                old_value = "(невідомо)"
        # Перевірка дублікату
        if old_value and old_value != "(порожньо)" and old_value == text.strip():
            await update.message.reply_text(
                f"ℹ️ Ця інформація вже збережена: **{text}**\nЗміни не потрібно.",
                parse_mode="Markdown"
            )
            session[SK.MODE] = BotMode.NORMAL
            return
        if sheet_mgr.update_field_by_telegram_id(user_id, awaiting_field, text):
            session[SK.PROFILE][awaiting_field] = text
            session[SK.MODE] = BotMode.NORMAL
            labels = {Col.PARENTS_PHONE: "номер представника", Col.PARENTS_NAME: "ПІБ представника"}
            label = labels.get(awaiting_field, awaiting_field)
            if awaiting_field == Col.PARENTS_NAME:
                other_hint = "\n📱 Якщо змінився і номер представника — натисніть /edit ще раз."
            elif awaiting_field == Col.PARENTS_PHONE:
                other_hint = "\n👤 Якщо змінився і ПІБ представника — натисніть /edit ще раз."
            else:
                other_hint = ""
            await wait.edit_text(
                f"✅ Змінено {label}: **{text}**{other_hint}",
                parse_mode="Markdown"
            )
            await dev_notifier.notify_data_change(
                bot=context.bot, user_id=user_id, profile=session[SK.PROFILE],
                field=awaiting_field, old_value=old_value, new_value=text,
            )
        else:
            await wait.edit_text("❌ Помилка запису. Спробуйте ще раз.")
            session[SK.AWAITING_EDIT_FIELD] = awaiting_field
        return

    # ── Переадресація до реєстрації ──────────────────────────────────────────
    if session.get(SK.REG_STEP) != RegStep.COMPLETED:
        await handle_registration(update, context)
        return

    # ── Нетекстові повідомлення ───────────────────────────────────────────────
    if not text and not update.message.contact:
        if any([
            update.message.voice, update.message.video, update.message.audio,
            update.message.document, update.message.photo, update.message.sticker,
            update.message.location, update.message.animation, update.message.video_note,
        ]):
            await update.message.reply_text(UI.NON_TEXT, parse_mode="Markdown")
        return

    # ── Антиспам ──────────────────────────────────────────────────────────────
    blocked_until = session.get(SK.BLOCKED_UNTIL)
    if blocked_until:
        if datetime.datetime.now() < blocked_until:
            return
        session[SK.BLOCKED_UNTIL] = None
        session[SK.MSG_COUNT]     = 0

    mode = session.get(SK.MODE, BotMode.NORMAL)
    # ── Антиспам: повідомлення без шаблону (ліміт 10) ────────────────────
    if mode == BotMode.NORMAL and not session.get(SK.ACTIVE_TEMPLATE):
        session[SK.MSG_COUNT] += 1
        if session[SK.MSG_COUNT] > 10:
            session[SK.BLOCKED_UNTIL] = datetime.datetime.now() + timedelta(hours=1)
            session[SK.HISTORY]       = []
            session[SK.MSG_COUNT]     = 0
            sheet_mgr.log_event(session.get(SK.PROFILE, {}), "SPAM FILTER", "🚫 BLOCKED")
            logger.warning(f"[{user_id}] SPAM BLOCK.")
            await update.message.reply_text(UI.SPAM_BLOCK, parse_mode="Markdown")
            return
        # Перевірка к-сті заяв за сесію (ліміт 5)
        if session.get(SK.DOCS_COUNT, 0) >= 5:
            session[SK.BLOCKED_UNTIL] = datetime.datetime.now() + timedelta(hours=1)
            session[SK.DOCS_COUNT]    = 0
            sheet_mgr.log_event(session.get(SK.PROFILE, {}), "SPAM FILTER", "🚫 DOCS LIMIT")
            logger.warning(f"[{user_id}] DOCS LIMIT BLOCK.")
            await update.message.reply_text(UI.SPAM_BLOCK, parse_mode="Markdown")
            return

    # ── Антиспам: повідомлення всередині шаблону (ліміт 15) ─────────────
    if mode == BotMode.NORMAL and session.get(SK.ACTIVE_TEMPLATE):
        session[SK.ANALYTICS_MSG_COUNT] = session.get(SK.ANALYTICS_MSG_COUNT, 0)
        _tmpl_msg = session.get("tmpl_msg_count", 0) + 1
        session["tmpl_msg_count"] = _tmpl_msg
        if _tmpl_msg > 15:
            sessions.reset_dialog(user_id)
            sheet_mgr.log_event(session.get(SK.PROFILE, {}), "SPAM FILTER", "🚫 TMPL MSG LIMIT")
            logger.warning(f"[{user_id}] TMPL MSG LIMIT BLOCK.")
            await update.message.reply_text(
                "⚠️ Схоже, оформлення заяви затяглось. Діалог скинуто.\n"
                "Натисніть /start щоб почати знову.",
                parse_mode="Markdown",
            )
            return

    reset_timeout(user_id, chat_id, context)

    # ── Трекінг сесії для аналітики ───────────────────────────────────────────
    if session.get(SK.SESSION_START) is None:
        session[SK.SESSION_START] = datetime.datetime.now()
    session[SK.ANALYTICS_MSG_COUNT] = session.get(SK.ANALYTICS_MSG_COUNT, 0) + 1

    # ── AI-обробка ────────────────────────────────────────────────────────────
    session[SK.HISTORY].append({"role": "user", "content": text})
    await context.bot.send_chat_action(chat_id, action="typing")

    templates = drive_mgr.get_templates()
    analysis  = brain.analyze(
        session[SK.HISTORY],
        templates,
        session[SK.PROFILE],
        session[SK.ACTIVE_TEMPLATE],
        mode=mode,
    )

    status    = analysis.get("status", "")
    reply_txt = analysis.get("bot_reply", "")
    data      = analysis.get("extracted_data") or {}

    logger.info(f"[{user_id}] status={status} | tmpl={analysis.get('selected_template_name')}")
    session[SK.HISTORY].append({"role": "model", "content": reply_txt})

    # ── 1. Оновлення профілю ──────────────────────────────────────────────────
    if status == "PROFILE_UPDATE":
        await _handle_profile_update(update, user_id, session, data, reply_txt)
        return

    # ── 2. Діалог / уточнення ─────────────────────────────────────────────────
    if status in ("CLARIFICATION_NEEDED", "TEMPLATE_SELECTED", "WAITING_FOR_CONFIRMATION"):
        # Зберігаємо шаблон при будь-якому статусі де він відомий
        tmpl_from_ai = analysis.get("selected_template_name")
        if tmpl_from_ai:
            session[SK.ACTIVE_TEMPLATE] = tmpl_from_ai
            # TOTAL_STEPS встановлюємо при першій появі шаблону (один раз)
            if not session.get(SK.TOTAL_STEPS):
                _tmpl_cfg = TEMPLATE_CONFIG.get(tmpl_from_ai, {})
                session[SK.TOTAL_STEPS] = len(_tmpl_cfg.get("required_fields", [])) + 1

        if status == "TEMPLATE_SELECTED":
            session[SK.MSG_COUNT] = 0
            # Скидаємо CLARIFICATION_COUNT тільки якщо діалог ще не починався
            if session.get(SK.CLARIFICATION_COUNT, 0) == 0:
                session[SK.CLARIFICATION_COUNT] = 0
            session["tmpl_msg_count"] = 0
            if not session.get(SK.TOTAL_STEPS):
                _tmpl_cfg = TEMPLATE_CONFIG.get(session.get(SK.ACTIVE_TEMPLATE) or tmpl_from_ai or "", {})
                session[SK.TOTAL_STEPS] = len(_tmpl_cfg.get("required_fields", [])) + 1

        if status == "CLARIFICATION_NEEDED":
            session[SK.CLARIFICATION_COUNT] = session.get(SK.CLARIFICATION_COUNT, 0) + 1
            step  = session[SK.CLARIFICATION_COUNT]
            total = session.get(SK.TOTAL_STEPS, 0)
            if total > 1 and step > 0:
                prefix = f"\U0001f4cd *\u041a\u0440\u043e\u043a {step} \u0437 {total}*\n"
            elif step > 1:
                prefix = f"\U0001f4cd *\u041a\u0440\u043e\u043a {step}*\n"
            else:
                prefix = ""
            if reply_txt:
                # Якщо це повідомлення про помилку — додаємо кнопку перезапуску
                is_error = reply_txt in (UI.UNKNOWN_ERROR, UI.GOOGLE_OVERLOAD)
                kb = InlineKeyboardMarkup([[
                    InlineKeyboardButton("\U0001f504 \u0421\u043f\u0440\u043e\u0431\u0443\u0432\u0430\u0442\u0438 \u0437\u043d\u043e\u0432\u0443", callback_data=f"{CALLBACK_DONE}restart"),
                ]]) if is_error else None
                await update.message.reply_text(
                    f"{prefix}{reply_txt}",
                    parse_mode="Markdown",
                    reply_markup=kb,
                )

        elif status == "WAITING_FOR_CONFIRMATION":
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Все вірно", callback_data=f"{CALLBACK_CONFIRM}yes"),
                InlineKeyboardButton("✏️ Змінити", callback_data=f"{CALLBACK_CONFIRM}no"),
            ]])
            step  = session.get(SK.CLARIFICATION_COUNT, 0)
            total = session.get(SK.TOTAL_STEPS, 0)
            if total > 1:
                header = f"\U0001f4cb *\u041a\u0440\u043e\u043a {step + 1} \u0437 {total} \u2014 \u041f\u0435\u0440\u0435\u0432\u0456\u0440\u0442\u0435 \u0434\u0430\u043d\u0456:*\n\n"
            elif step:
                header = f"\U0001f4cb *\u041a\u0440\u043e\u043a {step + 1} \u2014 \u041f\u0435\u0440\u0435\u0432\u0456\u0440\u0442\u0435 \u0434\u0430\u043d\u0456:*\n\n"
            else:
                header = "\U0001f4cb *\u041f\u0435\u0440\u0435\u0432\u0456\u0440\u0442\u0435 \u0434\u0430\u043d\u0456:*\n\n"
            if reply_txt:
                await update.message.reply_text(
                    f"{header}{reply_txt}",
                    parse_mode="Markdown",
                    reply_markup=kb,
                )
        else:
            if reply_txt:
                await update.message.reply_text(reply_txt, parse_mode="Markdown")
        return

    # ── 3. Генерація документа ────────────────────────────────────────────────
    if status == "READY_TO_GENERATE":
        await _handle_generate(update, context, user_id, session, data)
        return

    # ── 4. Невідомий запит — сповістити розробника ────────────────────────────
    if status == "UNKNOWN_INTENT":
        await update.message.reply_text(UI.UNKNOWN_INTENT, parse_mode="Markdown")
        await dev_notifier.notify(
            bot     = context.bot,
            user_id = user_id,
            profile = session[SK.PROFILE],
            message = text,
            history = session[SK.HISTORY],
        )
        return

    # ── Fallback ──────────────────────────────────────────────────────────────
    if reply_txt:
        await update.message.reply_text(reply_txt, parse_mode="Markdown")


async def _handle_profile_update(
    update: Update,
    user_id: str,
    session: Dict[str, Any],
    data: Dict[str, Any],
    ai_reply: str,
) -> None:
    if not data:
        return
    col, val = next(iter(data.items()))

    if col not in Col.EDITABLE:
        await update.message.reply_text(
            f"⚠️ Поле `{col}` не можна редагувати через бота.\n"
            "Зверніться до адміністрації коледжу.",
            parse_mode="Markdown",
        )
        return

    # Для зміни власного телефону — вимагаємо кнопку (щоб номер був прив'язаний до Telegram)
    if col == Col.STUDENTS_PHONE:
        markup = ReplyKeyboardMarkup(
            [[KeyboardButton("📱 Підтвердити новий номер", request_contact=True)]],
            one_time_keyboard=True, resize_keyboard=True,
        )
        session[SK.AWAITING_PHONE_UPDATE] = True
        await update.message.reply_text(
            "📱 Для зміни номера натисніть кнопку нижче — це гарантує що номер правильний.",
            reply_markup=markup,
        )
        return

    # Валідація значення перед записом (для решти полів)
    ok, err = validate_field(col, str(val), session.get(SK.PROFILE, {}))
    if not ok:
        await update.message.reply_text(err, parse_mode="Markdown")
        return

    extra = ""
    if col == Col.GROUP:
        spec = resolve_specialty(str(val), specialties)
        if spec:
            session[SK.PROFILE][Col.SPECIALTY] = spec
            extra = f"\n🎓 Спеціальність: **{spec}**"
        else:
            extra = "\n⚠️ Не вдалося визначити спеціальність. Перевірте формат групи."

    if sheet_mgr.update_field_by_telegram_id(user_id, col, val):
        session[SK.PROFILE][col] = val
        ua = {Col.GROUP: "групу", Col.STUDENTS_PHONE: "телефон", Col.PARENTS_PHONE: "телефон представника"}
        await update.message.reply_text(
            f"✅ Змінено {ua.get(col, col)}: **{val}**{extra}", parse_mode="Markdown"
        )
        if ai_reply:
            await update.message.reply_text(ai_reply, parse_mode="Markdown")
    else:
        await update.message.reply_text("❌ Помилка запису в базу.", parse_mode="Markdown")


async def _handle_generate(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    user_id: str,
    session: Dict[str, Any],
    ai_data: Dict[str, Any],
) -> None:
    chat_id   = update.effective_chat.id
    tmpl_name = session.get(SK.ACTIVE_TEMPLATE) or ai_data.get("selected_template_name")

    if not tmpl_name:
        await update.message.reply_text("⚠️ Шаблон не визначено. Уточніть тип заяви.")
        return

    templates = drive_mgr.get_templates()
    tmpl_obj  = next((t for t in templates if t["name"] == tmpl_name), None)
    if not tmpl_obj:
        await update.message.reply_text("❌ Файл шаблону не знайдено на Google Drive.")
        return

    status_msg = await update.message.reply_text("⏳ Генерую документ…")

    try:
        # Фільтруємо ai_data — дозволяємо тільки поля з required_fields шаблону
        # та стандартні поля дат/причин. Захист від підміни STUDENTS_NAME тощо.
        _allowed = set(TEMPLATE_CONFIG.get(tmpl_name, {}).get("required_fields", []))
        _allowed |= {"DATE_FROM", "DATE_TO", "LESSONS_RANGE", "REASON",
                     "SUBJECT", "SPECIALTY", "SPECIALTY_TO"}
        _safe_ai_data = {k: v for k, v in ai_data.items() if k in _allowed}
        if set(ai_data.keys()) - _allowed:
            logger.warning(f"[{user_id}] Filtered out ai_data keys: {set(ai_data.keys()) - _allowed}")

        # Валідація дат перед генерацією
        _date_errors = []
        for _date_key in ("DATE_FROM", "DATE_TO"):
            _dval = str(_safe_ai_data.get(_date_key, "")).strip()
            if _dval:
                if not Regex.DATE.match(_dval):
                    _date_errors.append(f"{_date_key}: «{_dval}» — очікується формат ДД.ММ.РРРР")
                else:
                    try:
                        _d, _m, _y = map(int, _dval.split("."))
                        datetime.date(_y, _m, _d)
                    except ValueError:
                        _date_errors.append(f"{_date_key}: «{_dval}» — такої дати не існує")
        if _date_errors:
            await status_msg.edit_text(
                "⛔ Виявлено некоректні дати:\n" + "\n".join(f"• {e}" for e in _date_errors) +
                "\n\nНапишіть дату у форматі ДД.ММ.РРРР (наприклад: 25.03.2026).",
                parse_mode="Markdown",
            )
            return

        full_data = {**session[SK.PROFILE], **_safe_ai_data}
        await context.bot.send_chat_action(chat_id, action="upload_document")
        pdf_bytes = drive_mgr.create_pdf(tmpl_obj["id"], tmpl_name, full_data)

        pdf_file  = io.BytesIO(pdf_bytes)
        clean     = str(full_data.get(Col.NAME, "Doc")).replace(" ", "_")
        filename  = f"Заява_{clean}.pdf"
        pdf_file.name = filename

        await status_msg.edit_text("📤 Відправляю на пошту…")

        email_body   = f"Студент: {full_data.get(Col.NAME)}\nТип: {tmpl_name}"
        # TARGET_PRINT_EMAIL може містити кілька адрес через кому
        print_emails = [e.strip() for e in (Env.TARGET_PRINT_EMAIL or "").split(",") if e.strip()]
        email_success = False
        for _email in print_emails:
            pdf_file.seek(0)
            if email_mgr.send(_email, f"ДРУК: {filename}", email_body, pdf_file, filename):
                email_success = True

        log_status = "✅ SUCCESS" if email_success else "❌ EMAIL FAILED"
        _dur = int((datetime.datetime.now() - session.get(SK.SESSION_START, datetime.datetime.now())).total_seconds()) if session.get(SK.SESSION_START) else None
        _msg = session.get(SK.ANALYTICS_MSG_COUNT)
        sheet_mgr.log_event(full_data, tmpl_name, log_status, duration_sec=_dur, msg_count=_msg)

        final = "✅ Готово! Заяву відправлено на друк." if email_success else "⚠️ Заяву згенеровано, але не вдалося відправити на друк."
        await status_msg.edit_text(final)

        pdf_file.seek(0)
        await update.message.reply_document(pdf_file, filename=filename, caption="Ваша копія 📄")

        session[SK.DOCS_COUNT] = session.get(SK.DOCS_COUNT, 0) + 1
        sessions.reset_dialog(user_id)
        logger.info(f"[{user_id}] Document generated: {tmpl_name} (total this session: {session.get(SK.DOCS_COUNT, 1)})")

        # Кнопки після генерації
        kb_done = InlineKeyboardMarkup([[
            InlineKeyboardButton("📝 Ще одна заява", callback_data=f"{CALLBACK_DONE}new"),
            InlineKeyboardButton("👋 На цьому все", callback_data=f"{CALLBACK_DONE}bye"),
        ]])
        await update.message.reply_text(
            "Що робимо далі?",
            reply_markup=kb_done,
        )

    except Exception as exc:
        logger.error(f"[{user_id}] generate error: {exc}")
        sheet_mgr.log_event(session.get(SK.PROFILE, {}), tmpl_name, f"🔥 ERROR: {exc}")
        kb_retry = InlineKeyboardMarkup([[
            InlineKeyboardButton("\U0001f504 \u0421\u043f\u0440\u043e\u0431\u0443\u0432\u0430\u0442\u0438 \u0437\u043d\u043e\u0432\u0443", callback_data=f"{CALLBACK_DONE}restart"),
        ]])
        await status_msg.edit_text(
            _friendly_google_error(exc),
            parse_mode="Markdown",
            reply_markup=kb_retry,
        )


# ════════════════════════════════════════════════════════════════════════════════
# 12.  ІНІЦІАЛІЗАЦІЯ ТА ЗАПУСК
# ════════════════════════════════════════════════════════════════════════════════

def _get_google_creds() -> Credentials:
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", GOOGLE_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow  = InstalledAppFlow.from_client_secrets_file(Env.CLIENT_SECRET_FILE, GOOGLE_SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as fh:
            fh.write(creds.to_json())
    return creds


# Глобальні сервіси (ініціалізуються один раз при старті)
_creds      = _get_google_creds()
drive_mgr   = DriveManager(_creds)
sheet_mgr   = SheetManager(_creds, Env.SPREADSHEET_ID)
email_mgr   = EmailManager(_creds)
brain       = GeminiBrain()
top_templates_cache = TopTemplatesCache()
dev_notifier = DevNotifier(email_mgr)

# Завантаження спеціальностей із Config-аркуша (один раз при старті)
specialties: Dict[str, str] = sheet_mgr.load_specialties()
logger.info(f"✅ Loaded {len(specialties)} specialties from Config sheet.")


async def _post_init(application: Application) -> None:
    commands = [
        BotCommand("start",  "🏠 Головна / нова заява"),
        BotCommand("edit",   "✏️ Редагувати дані"),
        BotCommand("cancel", "❌ Скасувати / Назад"),
        BotCommand("mydata", "👤 Мої дані"),
        BotCommand("help",   "ℹ️ Допомога"),
    ]
    await application.bot.set_my_commands(commands)

    if Env.MODE == "DEV" and Env.ADMIN_ID:
        try:
            await application.bot.send_message(
                chat_id=Env.ADMIN_ID,
                text="👨‍💻 **[DEV]** Бот запущено в режимі розробки.",
                parse_mode="Markdown",
            )
        except Exception as exc:
            logger.warning(f"Admin notify failed: {exc}")

    logger.info("✅ Bot started and ready.")


def main() -> None:
    logger.info("🤖 Starting eCollege Bot…")
    app = ApplicationBuilder().token(Env.TELEGRAM_TOKEN).post_init(_post_init).build()

    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CallbackQueryHandler(callback_template_select, pattern=f"^{CALLBACK_TEMPLATE_PREFIX}"))
    app.add_handler(CallbackQueryHandler(callback_edit_field,      pattern=f"^{CALLBACK_EDIT_FIELD}"))
    app.add_handler(CallbackQueryHandler(callback_confirm,         pattern=f"^{CALLBACK_CONFIRM}"))
    app.add_handler(CallbackQueryHandler(callback_done,            pattern=f"^{CALLBACK_DONE}"))
    app.add_handler(CommandHandler("edit",   cmd_edit))
    app.add_handler(CommandHandler("mydata", cmd_mydata))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("help",   cmd_help))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_message))

    app.run_polling()


if __name__ == "__main__":
    main()