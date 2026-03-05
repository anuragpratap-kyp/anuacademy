import json
import os
import random
import sqlite3
import csv
import re
import smtplib
import base64
import urllib.error
import urllib.parse
import urllib.request
from io import StringIO
from datetime import datetime
from pathlib import Path
from email.message import EmailMessage
from flask import Flask, Response, abort, jsonify, redirect, render_template, request, url_for

try:
    import psycopg2
except ImportError:
    psycopg2 = None

app = Flask(__name__)
DEFAULT_DB_PATH = Path(__file__).with_name("quiz_history.db")
DB_PATH = Path(os.environ.get("DB_PATH", str(DEFAULT_DB_PATH)))
DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    DATABASE_URL = (os.environ.get("RENDER_DATABASE_URL") or os.environ.get("POSTGRES_URL") or "").strip()
QUESTIONS_PATH = Path(__file__).with_name("questions.json")
GALLERY_DIR = Path(__file__).with_name("static") / "gallery"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
ACTIVE_SEMESTER = 4
TOTAL_SEMESTERS = 6
QUIZ_QUESTION_LIMIT = 25
QUIZ_DURATION_MINUTES = 25
SUBJECT_TEST_QUESTION_LIMIT = int(os.environ.get("SUBJECT_TEST_QUESTION_LIMIT") or 20)
SUBJECT_TEST_DURATION_MINUTES = int(os.environ.get("SUBJECT_TEST_DURATION_MINUTES") or 20)
SUBJECT_TEST_OPEN_TIME = (os.environ.get("SUBJECT_TEST_OPEN_TIME") or "").strip()  # HH:MM, e.g. 10:00
SUBJECT_TEST_CLOSE_TIME = (os.environ.get("SUBJECT_TEST_CLOSE_TIME") or "").strip()  # HH:MM, e.g. 12:00

COURSES = [
    {"slug": "ba", "label": "B.A"},
    {"slug": "bsc", "label": "B.Sc"},
    {"slug": "bcom", "label": "B.Com"},
]

COURSE_LABELS = {course["slug"]: course["label"] for course in COURSES}

SOCIAL_LINKS = {
    "instagram": (os.environ.get("SOCIAL_INSTAGRAM_URL") or "https://www.instagram.com/").strip(),
    "youtube": (os.environ.get("SOCIAL_YOUTUBE_URL") or "https://www.youtube.com/").strip(),
    "linkedin": (os.environ.get("SOCIAL_LINKEDIN_URL") or "https://www.linkedin.com/").strip(),
}
NOTICE_HOST_KEY = (os.environ.get("NOTICE_HOST_KEY") or "").strip()
SMTP_HOST = (os.environ.get("SMTP_HOST") or "").strip()
SMTP_PORT = int(os.environ.get("SMTP_PORT") or 587)
SMTP_USERNAME = (os.environ.get("SMTP_USERNAME") or "").strip()
SMTP_PASSWORD = (os.environ.get("SMTP_PASSWORD") or "").strip()
SMTP_USE_TLS = (os.environ.get("SMTP_USE_TLS", "1") or "1").strip() not in {"0", "false", "False"}
SMTP_FROM_EMAIL = (os.environ.get("SMTP_FROM_EMAIL") or SMTP_USERNAME).strip()
EMAIL_PROVIDER = (os.environ.get("EMAIL_PROVIDER") or "").strip().lower()
RESEND_API_KEY = (os.environ.get("RESEND_API_KEY") or "").strip()
RESEND_API_URL = (os.environ.get("RESEND_API_URL") or "https://api.resend.com/emails").strip()
GMAIL_CLIENT_ID = (os.environ.get("GMAIL_CLIENT_ID") or "").strip()
GMAIL_CLIENT_SECRET = (os.environ.get("GMAIL_CLIENT_SECRET") or "").strip()
GMAIL_REFRESH_TOKEN = (os.environ.get("GMAIL_REFRESH_TOKEN") or "").strip()
GMAIL_SENDER = (os.environ.get("GMAIL_SENDER") or "").strip()
GMAIL_TOKEN_URL = (os.environ.get("GMAIL_TOKEN_URL") or "https://oauth2.googleapis.com/token").strip()
GMAIL_SEND_URL = (os.environ.get("GMAIL_SEND_URL") or "https://gmail.googleapis.com/gmail/v1/users/me/messages/send").strip()

COURSE_SUBJECTS = {
    "ba": [
        {"name": "History", "description": "Ancient, Medieval and Modern topics"},
        {"name": "Political Science", "description": "Indian polity and political theories"},
        {"name": "Economics", "description": "Micro, macro and development concepts"},
    ],
    "bsc": [
        {"name": "Chemistry", "description": "Quantum Mechanics and Analytical Techniques"},
        {"name": "Zoology", "description": "Gene Technology, Immunology and Computational Biology"},
        {"name": "Botany", "description": "Economic Botany,Ethomedicine and Phytochemistry"},
        {"name": "Co-Curricular", "description": "Skill development and practical knowledge"},
    ],
    "bcom": [
        {"name": "Accountancy", "description": "Financial and corporate accounting"},
        {"name": "Business Studies", "description": "Management, organization and strategy"},
        {"name": "Economics", "description": "Business economics and market analysis"},
    ],
}

COURSE_ACTIVE_SEMESTERS = {
    "ba": [],
    "bsc": [ACTIVE_SEMESTER],
    "bcom": [],
}

# Load questions from JSON
with QUESTIONS_PATH.open(encoding="utf-8-sig") as f:
    all_questions = json.load(f)


def load_questions():
    with QUESTIONS_PATH.open(encoding="utf-8-sig") as f:
        return json.load(f)


def get_gallery_images():
    if not GALLERY_DIR.exists():
        return []

    images = []
    for file_path in sorted(GALLERY_DIR.iterdir(), key=lambda p: p.name.lower()):
        if file_path.is_file() and file_path.suffix.lower() in IMAGE_EXTENSIONS:
            images.append(f"gallery/{file_path.name}")
    return images


SYLLABUS_UNITS = {
    ("Chemistry", 4): [
        "Unit I - Atomic Structure",
        "Unit II - Elementary Quantum Mechanics",
        "Unit III - Molecular Spectroscopy",
        "Unit IV - UV-Visible Spectroscopy",
        "Unit V - Infrared Spectroscopy",
        "Unit VI - 1H-NMR Spectroscopy (PMR)",
        "Unit VII - Introduction to Mass Spectrometry",
        "Unit VIII - Separation Techniques",
    ],
    ("Botany", 4): [
        "Unit I - Origin and domestication of cultivated plants",
        "Unit II - Botany of oils, fibers, timber yielding plants and dyes",
        "Unit III - Commercial production of flowers, vegetables and fruits",
        "Unit IV - IPR and Traditional Knowledge",
        "Unit V - Ethnobotany",
        "Unit VI - Medicinal aspects",
        "Unit VII - Pharmacognosy",
        "Unit VIII - Herbal Preparations and Phytochemistry",
    ],
    ("Zoology", 4): [
        "Unit I - Principles of Gene Manipulation",
        "Unit II - Applications of Genetic Engineering",
        "Unit III - DNA Diagnostics",
        "Unit IV - Immune System and its Components",
        "Unit V - Biostatistics I",
        "Unit VI - Biostatistics II",
        "Unit VII - Basics of Computers",
        "Unit VIII - Bioinformatics",
    ]
}


def get_subject_semester_questions(subject, semester):
    all_questions = load_questions()
    return [
        q for q in all_questions
        if q.get("subject") == subject and str(q.get("semester")) == str(semester)
        and not bool(q.get("test_only"))
    ]


def get_subject_semester_test_questions(subject, semester):
    all_questions = load_questions()
    return [
        q for q in all_questions
        if q.get("subject") == subject
        and str(q.get("semester")) == str(semester)
        and bool(q.get("test_only"))
    ]


def get_course_semester_test_questions(course_slug, semester):
    clean_course_slug = normalize_course_slug(course_slug)
    course_subject_names = {item["name"] for item in get_course_subjects(clean_course_slug)}
    all_questions = load_questions()
    return [
        q for q in all_questions
        if q.get("subject") in course_subject_names
        and str(q.get("semester")) == str(semester)
        and bool(q.get("test_only"))
    ]


def get_test_units_for_course_semester(course_slug, semester):
    questions = attach_units(get_course_semester_test_questions(course_slug, semester))
    units = []
    for q in questions:
        unit_name = q.get("unit", "").strip() or "General"
        if unit_name not in units:
            units.append(unit_name)
    return units


def get_default_test_target():
    for course in COURSES:
        clean_course_slug = normalize_course_slug(course["slug"])
        for sem in get_active_semesters(clean_course_slug):
            units = get_test_units_for_course_semester(clean_course_slug, sem)
            if units:
                return (clean_course_slug, sem, units[0])
    return None


def get_default_test_target_details():
    target = get_default_test_target()
    if not target:
        return None

    course_slug, semester, unit = target
    unit_questions = [
        q for q in attach_units(get_course_semester_test_questions(course_slug, semester))
        if (q.get("unit") or "").strip() == unit
    ]
    subject_names = sorted({(q.get("subject") or "").strip() for q in unit_questions if (q.get("subject") or "").strip()})
    subject_label = ", ".join(subject_names) if subject_names else "Subject Test"

    return {
        "course_slug": course_slug,
        "semester": semester,
        "unit": unit,
        "subject_label": subject_label,
    }


def attach_units(questions):
    """Attach unit name to each question.
    If question data doesn't contain `unit`, auto-group by 5 questions per unit.
    """
    if not questions:
        return []

    has_unit = any(str(q.get("unit", "")).strip() for q in questions)
    enriched = []

    if has_unit:
        for q in questions:
            copy_q = q.copy()
            copy_q["unit"] = str(copy_q.get("unit", "General")).strip() or "General"
            enriched.append(copy_q)
        return enriched

    for i, q in enumerate(questions):
        copy_q = q.copy()
        copy_q["unit"] = f"Unit {(i // 5) + 1}"
        enriched.append(copy_q)

    return enriched


def get_units_for_subject(subject, semester):
    syllabus_units = SYLLABUS_UNITS.get((subject, semester))
    if syllabus_units:
        return syllabus_units

    questions = attach_units(get_subject_semester_questions(subject, semester))
    units = []
    for q in questions:
        unit_name = q["unit"]
        if unit_name not in units:
            units.append(unit_name)
    return units


def normalize_course_slug(course_slug):
    clean_slug = (course_slug or "bsc").strip().lower()
    if clean_slug not in COURSE_LABELS:
        return "bsc"
    return clean_slug


def get_course_subjects(course_slug):
    return COURSE_SUBJECTS.get(normalize_course_slug(course_slug), [])


def get_active_semesters(course_slug):
    return COURSE_ACTIVE_SEMESTERS.get(normalize_course_slug(course_slug), [])


@app.context_processor
def inject_global_template_context():
    return {"social_links": SOCIAL_LINKS}


def normalize_database_url(url):
    if url.startswith("postgres://"):
        return url.replace("postgres://", "postgresql://", 1)
    return url


def using_postgres():
    return bool(DATABASE_URL)


def get_cursor_columns(cursor):
    return [desc[0] for desc in (cursor.description or [])]


def row_to_dict(row, columns):
    if row is None:
        return None

    if isinstance(row, sqlite3.Row):
        return dict(row)

    return {columns[i]: row[i] for i in range(len(columns))}


def query_with_placeholders(sql):
    if using_postgres():
        return sql.replace("?", "%s")
    return sql


def normalize_answer_text(value):
    # Normalize whitespace and case to avoid false mismatches from formatting differences.
    return " ".join(str(value or "").split()).strip().casefold()


def is_valid_email(email):
    clean_email = (email or "").strip()
    if not clean_email:
        return False
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", clean_email))


def parse_hhmm(value):
    clean_value = (value or "").strip()
    if not clean_value:
        return None
    try:
        return datetime.strptime(clean_value, "%H:%M").time()
    except ValueError:
        return None


def get_subject_test_window_status(now=None):
    open_time_obj = parse_hhmm(SUBJECT_TEST_OPEN_TIME)
    close_time_obj = parse_hhmm(SUBJECT_TEST_CLOSE_TIME)
    now_dt = now or datetime.now()
    now_time = now_dt.time()

    if not open_time_obj or not close_time_obj:
        return {
            "enabled": False,
            "is_open": True,
            "open_label": SUBJECT_TEST_OPEN_TIME or "-",
            "close_label": SUBJECT_TEST_CLOSE_TIME or "-",
            "now_label": now_dt.strftime("%H:%M"),
        }

    if open_time_obj <= close_time_obj:
        is_open = open_time_obj <= now_time <= close_time_obj
    else:
        # Supports overnight windows like 22:00 to 02:00.
        is_open = now_time >= open_time_obj or now_time <= close_time_obj

    return {
        "enabled": True,
        "is_open": is_open,
        "open_label": open_time_obj.strftime("%H:%M"),
        "close_label": close_time_obj.strftime("%H:%M"),
        "now_label": now_dt.strftime("%H:%M"),
    }


def is_host_authorized():
    if not NOTICE_HOST_KEY:
        return False
    provided_key = (
        request.args.get("host_key")
        or request.form.get("host_key")
        or request.headers.get("X-Host-Key")
        or ""
    ).strip()
    return provided_key == NOTICE_HOST_KEY


def send_test_score_email(to_email, student_name, subject, semester, score, total, percentage, result_status):
    target = (to_email or "").strip()
    if not target:
        return (False, "missing_email")
    if not is_valid_email(target):
        return (False, "invalid_email")

    subject_line = f"Your Test Score - {subject} (Sem {semester})"
    body_text = (
        f"Hello {student_name},\n\n"
        f"Your test result is ready.\n"
        f"Subject: {subject}\n"
        f"Semester: {semester}\n"
        f"Score: {score}/{total}\n"
        f"Percentage: {percentage}%\n"
        f"Result: {result_status}\n\n"
        "Thank you."
    )

    use_gmail_api = EMAIL_PROVIDER == "gmail_api"
    if use_gmail_api:
        gmail_ready = all([GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET, GMAIL_REFRESH_TOKEN, GMAIL_SENDER])
        if not gmail_ready:
            missing_fields = []
            if not GMAIL_CLIENT_ID:
                missing_fields.append("GMAIL_CLIENT_ID")
            if not GMAIL_CLIENT_SECRET:
                missing_fields.append("GMAIL_CLIENT_SECRET")
            if not GMAIL_REFRESH_TOKEN:
                missing_fields.append("GMAIL_REFRESH_TOKEN")
            if not GMAIL_SENDER:
                missing_fields.append("GMAIL_SENDER")
            app.logger.warning(
                "Gmail API not configured. Missing fields: %s",
                ", ".join(missing_fields) if missing_fields else "unknown",
            )
            return (False, "gmail_api_not_configured")

        token_payload = urllib.parse.urlencode(
            {
                "client_id": GMAIL_CLIENT_ID,
                "client_secret": GMAIL_CLIENT_SECRET,
                "refresh_token": GMAIL_REFRESH_TOKEN,
                "grant_type": "refresh_token",
            }
        ).encode("utf-8")
        token_req = urllib.request.Request(
            GMAIL_TOKEN_URL,
            data=token_payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )

        try:
            app.logger.info("Gmail token request url=%s sender=%s", GMAIL_TOKEN_URL, GMAIL_SENDER)
            with urllib.request.urlopen(token_req, timeout=15) as token_response:
                token_data = json.loads(token_response.read().decode("utf-8"))
            access_token = (token_data.get("access_token") or "").strip()
            if not access_token:
                app.logger.error("Gmail token response missing access_token")
                return (False, "gmail_api_failed")

            raw_message = (
                f"From: {GMAIL_SENDER}\r\n"
                f"To: {target}\r\n"
                f"Subject: {subject_line}\r\n"
                "Content-Type: text/plain; charset=utf-8\r\n"
                "\r\n"
                f"{body_text}"
            )
            encoded_message = base64.urlsafe_b64encode(raw_message.encode("utf-8")).decode("ascii")
            send_payload = json.dumps({"raw": encoded_message}).encode("utf-8")
            send_req = urllib.request.Request(
                GMAIL_SEND_URL,
                data=send_payload,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )

            app.logger.info("Gmail send attempt url=%s from=%s to=%s", GMAIL_SEND_URL, GMAIL_SENDER, target)
            with urllib.request.urlopen(send_req, timeout=15) as send_response:
                status_code = getattr(send_response, "status", None) or send_response.getcode()
            if 200 <= int(status_code) < 300:
                return (True, "sent")
            app.logger.error("Gmail send failed with non-success status code=%s", status_code)
            return (False, "gmail_api_failed")
        except urllib.error.HTTPError as exc:
            error_body = ""
            try:
                error_body = exc.read().decode("utf-8", errors="ignore")
            except Exception:
                error_body = ""
            app.logger.exception(
                "Gmail API HTTP error status=%s reason=%s body=%s",
                exc.code,
                exc.reason,
                error_body[:500],
            )
            return (False, "gmail_api_failed")
        except Exception:
            app.logger.exception("Gmail API send failed sender=%s to=%s", GMAIL_SENDER, target)
            return (False, "gmail_api_failed")

    use_resend = EMAIL_PROVIDER == "resend" or bool(RESEND_API_KEY)
    if use_resend:
        if not RESEND_API_KEY:
            app.logger.warning("Resend not configured. Missing field: RESEND_API_KEY")
            return (False, "email_api_not_configured")

        from_email = SMTP_FROM_EMAIL or SMTP_USERNAME
        if not from_email:
            app.logger.warning("Resend not configured. Missing sender email in SMTP_FROM_EMAIL/SMTP_USERNAME")
            return (False, "email_api_not_configured")

        payload = json.dumps(
            {
                "from": from_email,
                "to": [target],
                "subject": subject_line,
                "text": body_text,
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            RESEND_API_URL,
            data=payload,
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        try:
            app.logger.info("Resend send attempt url=%s from=%s to=%s", RESEND_API_URL, from_email, target)
            with urllib.request.urlopen(req, timeout=15) as response:
                status_code = getattr(response, "status", None) or response.getcode()
            if 200 <= int(status_code) < 300:
                return (True, "sent")
            app.logger.error("Resend send failed with non-success status code=%s", status_code)
            return (False, "email_api_failed")
        except urllib.error.HTTPError as exc:
            error_body = ""
            try:
                error_body = exc.read().decode("utf-8", errors="ignore")
            except Exception:
                error_body = ""
            app.logger.exception(
                "Resend HTTP error status=%s reason=%s body=%s",
                exc.code,
                exc.reason,
                error_body[:500],
            )
            return (False, "email_api_failed")
        except Exception:
            app.logger.exception("Resend send failed url=%s to=%s", RESEND_API_URL, target)
            return (False, "email_api_failed")

    smtp_ready = all([SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD, SMTP_FROM_EMAIL])
    if not smtp_ready:
        missing_fields = []
        if not SMTP_HOST:
            missing_fields.append("SMTP_HOST")
        if not SMTP_PORT:
            missing_fields.append("SMTP_PORT")
        if not SMTP_USERNAME:
            missing_fields.append("SMTP_USERNAME")
        if not SMTP_PASSWORD:
            missing_fields.append("SMTP_PASSWORD")
        if not SMTP_FROM_EMAIL:
            missing_fields.append("SMTP_FROM_EMAIL")
        app.logger.warning(
            "SMTP not configured. Missing fields: %s",
            ", ".join(missing_fields) if missing_fields else "unknown",
        )
        return (False, "smtp_not_configured")

    msg = EmailMessage()
    msg["Subject"] = subject_line
    msg["From"] = SMTP_FROM_EMAIL
    msg["To"] = target
    msg.set_content(body_text)

    try:
        app.logger.info(
            "SMTP send attempt host=%s port=%s tls=%s from=%s to=%s username=%s",
            SMTP_HOST,
            SMTP_PORT,
            SMTP_USE_TLS,
            SMTP_FROM_EMAIL,
            target,
            SMTP_USERNAME,
        )
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
            if SMTP_USE_TLS:
                server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
        return (True, "sent")
    except Exception:
        app.logger.exception(
            "SMTP send failed host=%s port=%s tls=%s from=%s to=%s username=%s",
            SMTP_HOST,
            SMTP_PORT,
            SMTP_USE_TLS,
            SMTP_FROM_EMAIL,
            target,
            SMTP_USERNAME,
        )
        return (False, "send_failed")


def fetchone_dict(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(query_with_placeholders(sql), params)
    row = cur.fetchone()
    columns = get_cursor_columns(cur)
    cur.close()
    return row_to_dict(row, columns)


def fetchall_dicts(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(query_with_placeholders(sql), params)
    rows = cur.fetchall()
    columns = get_cursor_columns(cur)
    cur.close()
    return [row_to_dict(row, columns) for row in rows]


def execute_sql(conn, sql, params=()):
    cur = conn.cursor()
    cur.execute(query_with_placeholders(sql), params)
    return cur


def get_db_connection():
    if using_postgres():
        if psycopg2 is None:
            raise RuntimeError(
                "DATABASE_URL is set but psycopg2 is not installed. Add psycopg2-binary to requirements."
            )
        return psycopg2.connect(normalize_database_url(DATABASE_URL))

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db_connection()
    try:
        if using_postgres():
            execute_sql(
                conn,
                """
                CREATE TABLE IF NOT EXISTS notices (
                    id SERIAL PRIMARY KEY,
                    message TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1
                )
                """,
            ).close()
            execute_sql(
                conn,
                """
                CREATE TABLE IF NOT EXISTS students (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    mobile TEXT NOT NULL,
                    UNIQUE(name, mobile)
                )
                """,
            ).close()
            execute_sql(
                conn,
                """
                CREATE TABLE IF NOT EXISTS attempts (
                    id SERIAL PRIMARY KEY,
                    attempted_at TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    semester INTEGER NOT NULL,
                    unit TEXT NOT NULL,
                    score INTEGER NOT NULL,
                    total INTEGER NOT NULL,
                    percentage REAL NOT NULL,
                    result_status TEXT NOT NULL
                )
                """,
            ).close()
            execute_sql(
                conn,
                """
                CREATE TABLE IF NOT EXISTS test_entries (
                    id SERIAL PRIMARY KEY,
                    attempted_at TEXT NOT NULL,
                    name TEXT NOT NULL,
                    student_identifier TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    class_name TEXT NOT NULL,
                    course_slug TEXT NOT NULL,
                    semester INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    total INTEGER NOT NULL,
                    percentage REAL NOT NULL,
                    result_status TEXT NOT NULL
                )
                """,
            ).close()

            execute_sql(conn, "ALTER TABLE attempts ADD COLUMN IF NOT EXISTS student_id INTEGER").close()
            execute_sql(conn, "ALTER TABLE attempts ADD COLUMN IF NOT EXISTS student_name TEXT").close()
            execute_sql(conn, "ALTER TABLE attempts ADD COLUMN IF NOT EXISTS student_mobile TEXT").close()

            execute_sql(
                conn,
                """
                UPDATE attempts
                SET student_mobile = students.mobile
                FROM students
                WHERE attempts.student_id = students.id
                  AND (attempts.student_mobile IS NULL OR attempts.student_mobile = '')
                """,
            ).close()
            execute_sql(
                conn,
                """
                UPDATE attempts
                SET student_name = students.name
                FROM students
                WHERE attempts.student_id = students.id
                  AND students.name IS NOT NULL
                  AND students.name != ''
                  AND (
                    attempts.student_name IS NULL
                    OR attempts.student_name = ''
                    OR LOWER(TRIM(attempts.student_name)) IN ('backend test user', 'test user', 'backend user')
                  )
                """,
            ).close()
        else:
            execute_sql(
                conn,
                """
                CREATE TABLE IF NOT EXISTS notices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1
                )
                """,
            ).close()
            execute_sql(
                conn,
                """
                CREATE TABLE IF NOT EXISTS students (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    mobile TEXT NOT NULL,
                    UNIQUE(name, mobile)
                )
                """,
            ).close()
            execute_sql(
                conn,
                """
                CREATE TABLE IF NOT EXISTS attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    attempted_at TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    semester INTEGER NOT NULL,
                    unit TEXT NOT NULL,
                    score INTEGER NOT NULL,
                    total INTEGER NOT NULL,
                    percentage REAL NOT NULL,
                    result_status TEXT NOT NULL
                )
                """,
            ).close()
            execute_sql(
                conn,
                """
                CREATE TABLE IF NOT EXISTS test_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    attempted_at TEXT NOT NULL,
                    name TEXT NOT NULL,
                    student_identifier TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    class_name TEXT NOT NULL,
                    course_slug TEXT NOT NULL,
                    semester INTEGER NOT NULL,
                    score INTEGER NOT NULL,
                    total INTEGER NOT NULL,
                    percentage REAL NOT NULL,
                    result_status TEXT NOT NULL
                )
                """,
            ).close()

            attempt_cols_rows = fetchall_dicts(conn, "PRAGMA table_info(attempts)")
            attempt_cols = {row["name"] for row in attempt_cols_rows}
            if "student_id" not in attempt_cols:
                execute_sql(conn, "ALTER TABLE attempts ADD COLUMN student_id INTEGER").close()
            if "student_name" not in attempt_cols:
                execute_sql(conn, "ALTER TABLE attempts ADD COLUMN student_name TEXT").close()
            if "student_mobile" not in attempt_cols:
                execute_sql(conn, "ALTER TABLE attempts ADD COLUMN student_mobile TEXT").close()

            execute_sql(
                conn,
                """
                UPDATE attempts
                SET student_mobile = (
                    SELECT students.mobile
                    FROM students
                    WHERE students.id = attempts.student_id
                )
                WHERE (student_mobile IS NULL OR student_mobile = '')
                  AND student_id IS NOT NULL
                """,
            ).close()
            execute_sql(
                conn,
                """
                UPDATE attempts
                SET student_name = (
                    SELECT students.name
                    FROM students
                    WHERE students.id = attempts.student_id
                )
                WHERE student_id IS NOT NULL
                  AND (
                    student_name IS NULL
                    OR student_name = ''
                    OR LOWER(TRIM(student_name)) IN ('backend test user', 'test user', 'backend user')
                  )
                  AND (
                    SELECT students.name
                    FROM students
                    WHERE students.id = attempts.student_id
                  ) IS NOT NULL
                """,
            ).close()

        conn.commit()
    finally:
        conn.close()


def get_or_create_student(name, mobile):
    clean_name = (name or "").strip()
    clean_mobile = (mobile or "").strip()
    if not clean_name or not clean_mobile:
        return None

    conn = get_db_connection()
    existing = fetchone_dict(
        conn,
        "SELECT id FROM students WHERE name = ? AND mobile = ?",
        (clean_name, clean_mobile),
    )

    if existing:
        student_id = existing["id"]
    else:
        execute_sql(
            conn,
            "INSERT INTO students (name, mobile) VALUES (?, ?)",
            (clean_name, clean_mobile),
        ).close()
        conn.commit()
        student_id = fetchone_dict(
            conn,
            "SELECT id FROM students WHERE name = ? AND mobile = ?",
            (clean_name, clean_mobile),
        )["id"]

    conn.close()
    return student_id


def save_attempt(
    subject,
    semester,
    unit,
    score,
    total,
    percentage,
    result_status,
    student_name="",
    student_mobile="",
):
    student_id = get_or_create_student(student_name, student_mobile)
    conn = get_db_connection()
    execute_sql(
        conn,
        """
        INSERT INTO attempts (
            attempted_at, subject, semester, unit, score, total, percentage, result_status,
            student_id, student_name, student_mobile
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            subject,
            semester,
            unit,
            score,
            total,
            percentage,
            result_status,
            student_id,
            (student_name or "").strip(),
            (student_mobile or "").strip(),
        ),
    ).close()
    conn.commit()
    conn.close()
    return student_id


def save_test_entry(
    name,
    student_identifier,
    subject,
    class_name,
    course_slug,
    semester,
    score,
    total,
    percentage,
    result_status,
):
    conn = get_db_connection()
    execute_sql(
        conn,
        """
        INSERT INTO test_entries (
            attempted_at, name, student_identifier, subject, class_name,
            course_slug, semester, score, total, percentage, result_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            (name or "").strip(),
            (student_identifier or "").strip(),
            (subject or "").strip(),
            (class_name or "").strip(),
            (course_slug or "").strip(),
            int(semester),
            int(score),
            int(total),
            float(percentage),
            (result_status or "").strip(),
        ),
    ).close()
    conn.commit()
    conn.close()


def get_leaderboard_rows(conn, subject_filter="all", limit=10):
    clean_subject_filter = (subject_filter or "all").strip()
    leaderboard_filters = ["attempts.student_id IS NOT NULL"]
    leaderboard_params = []
    if clean_subject_filter != "all":
        leaderboard_filters.append("subject = ?")
        leaderboard_params.append(clean_subject_filter)

    limit_clause = ""
    if limit is not None:
        limit_clause = f"\n        LIMIT {int(limit)}"

    return fetchall_dicts(
        conn,
        f"""
        WITH normalized_attempts AS (
            SELECT
                attempts.student_id,
                COALESCE(
                    NULLIF(
                        CASE
                            WHEN LOWER(TRIM(COALESCE(attempts.student_name, ''))) IN ('backend test user', 'test user', 'backend user') THEN ''
                            ELSE TRIM(COALESCE(attempts.student_name, ''))
                        END,
                        ''
                    ),
                    NULLIF(TRIM(COALESCE(students.name, '')), ''),
                    'Unknown'
                ) AS student_name,
                attempts.percentage
            FROM attempts
            LEFT JOIN students ON students.id = attempts.student_id
            WHERE {" AND ".join(leaderboard_filters)}
        )
        SELECT
            student_id,
            student_name,
            MAX(percentage) AS top_percentage,
            AVG(percentage) AS avg_percentage,
            COUNT(*) AS attempts
        FROM normalized_attempts
        WHERE student_name != 'Unknown'
        GROUP BY student_id, student_name
        ORDER BY top_percentage DESC, avg_percentage DESC, attempts DESC, student_name ASC
        {limit_clause}
        """,
        tuple(leaderboard_params),
    )


def get_student_rank(student_id, subject_filter="all"):
    if not student_id:
        return (None, 0)

    conn = get_db_connection()
    try:
        ranked_rows = get_leaderboard_rows(conn, subject_filter=subject_filter, limit=None)
    finally:
        conn.close()

    total_students = len(ranked_rows)
    for index, row in enumerate(ranked_rows, start=1):
        if row["student_id"] == student_id:
            return (index, total_students)

    return (None, total_students)


def get_percentile(rank, total_students):
    if not rank or not total_students:
        return None
    percentile = ((total_students - rank + 1) / total_students) * 100
    return round(percentile, 2)


def get_dashboard_data(subject_filter="all"):
    conn = get_db_connection()
    clean_subject_filter = (subject_filter or "all").strip()
    analytics_filters = []
    analytics_params = []

    if clean_subject_filter != "all":
        analytics_filters.append("subject = ?")
        analytics_params.append(clean_subject_filter)

    analytics_where = "WHERE " + " AND ".join(analytics_filters) if analytics_filters else ""

    total_attempts = fetchone_dict(
        conn,
        f"SELECT COUNT(*) AS count FROM attempts {analytics_where}",
        tuple(analytics_params),
    )["count"]
    avg_percentage = fetchone_dict(
        conn,
        f"SELECT AVG(percentage) AS value FROM attempts {analytics_where}",
        tuple(analytics_params),
    )["value"] or 0
    best_percentage = fetchone_dict(
        conn,
        f"SELECT MAX(percentage) AS value FROM attempts {analytics_where}",
        tuple(analytics_params),
    )["value"] or 0
    pass_count = fetchone_dict(
        conn,
        f"SELECT COUNT(*) AS count FROM attempts {analytics_where}{' AND ' if analytics_where else ' WHERE '}result_status = 'Pass'",
        tuple(analytics_params),
    )["count"] or 0
    pass_rate = round((pass_count / total_attempts) * 100, 2) if total_attempts else 0

    best_subject_row = fetchone_dict(
        conn,
        """
        SELECT subject, AVG(percentage) AS avg_score
        FROM attempts
        WHERE subject IS NOT NULL AND subject != ''
        GROUP BY subject
        ORDER BY avg_score DESC
        LIMIT 1
        """,
    )

    weak_subject_row = fetchone_dict(
        conn,
        """
        SELECT subject, AVG(percentage) AS avg_score
        FROM attempts
        WHERE subject IS NOT NULL AND subject != ''
        GROUP BY subject
        ORDER BY avg_score ASC
        LIMIT 1
        """,
    )

    subject_stats = fetchall_dicts(
        conn,
        """
        SELECT
            subject,
            COUNT(*) AS attempts,
            AVG(percentage) AS avg_percentage,
            SUM(CASE WHEN result_status = 'Pass' THEN 1 ELSE 0 END) AS passed
        FROM attempts
        WHERE subject IS NOT NULL AND subject != ''
        GROUP BY subject
        ORDER BY attempts DESC, subject ASC
        """,
    )

    recent_attempts = fetchall_dicts(
        conn,
        f"""
        SELECT
            attempts.attempted_at,
            attempts.subject,
            attempts.semester,
            attempts.unit,
            attempts.score,
            attempts.total,
            attempts.percentage,
            attempts.result_status,
            COALESCE(
                NULLIF(
                    CASE
                        WHEN LOWER(TRIM(COALESCE(attempts.student_name, ''))) IN ('backend test user', 'test user', 'backend user') THEN ''
                        ELSE TRIM(COALESCE(attempts.student_name, ''))
                    END,
                    ''
                ),
                NULLIF(TRIM(COALESCE(students.name, '')), ''),
                'Unknown'
            ) AS student_name
        FROM attempts
        LEFT JOIN students ON students.id = attempts.student_id
        {analytics_where}
        ORDER BY attempts.id DESC
        LIMIT 10
        """,
        tuple(analytics_params),
    )

    subject_options_rows = fetchall_dicts(
        conn,
        """
        SELECT DISTINCT subject
        FROM attempts
        WHERE subject IS NOT NULL AND subject != ''
        ORDER BY subject ASC
        """,
    )
    subject_options = [row["subject"] for row in subject_options_rows]
    if clean_subject_filter not in subject_options:
        clean_subject_filter = "all"

    top_students = get_leaderboard_rows(conn, subject_filter=clean_subject_filter, limit=10)
    top_three_students = top_students[:3]

    trend_rows = fetchall_dicts(
        conn,
        f"""
        SELECT
            substr(attempted_at, 1, 10) AS attempt_day,
            COUNT(*) AS attempts,
            AVG(percentage) AS avg_percentage
        FROM attempts
        {analytics_where}
        GROUP BY attempt_day
        ORDER BY attempt_day ASC
        """,
        tuple(analytics_params),
    )
    trend_rows = trend_rows[-12:]

    distribution_row = fetchone_dict(
        conn,
        f"""
        SELECT
            SUM(CASE WHEN percentage < 33 THEN 1 ELSE 0 END) AS fail_band,
            SUM(CASE WHEN percentage >= 33 AND percentage < 60 THEN 1 ELSE 0 END) AS low_band,
            SUM(CASE WHEN percentage >= 60 AND percentage < 80 THEN 1 ELSE 0 END) AS good_band,
            SUM(CASE WHEN percentage >= 80 THEN 1 ELSE 0 END) AS excellent_band
        FROM attempts
        {analytics_where}
        """,
        tuple(analytics_params),
    ) or {}

    if clean_subject_filter == "all":
        subject_chart_rows = subject_stats
    else:
        subject_chart_rows = fetchall_dicts(
            conn,
            """
            SELECT
                subject,
                COUNT(*) AS attempts,
                AVG(percentage) AS avg_percentage
            FROM attempts
            WHERE subject = ?
            GROUP BY subject
            ORDER BY subject ASC
            """,
            (clean_subject_filter,),
        )

    conn.close()

    return {
        "total_attempts": total_attempts,
        "avg_percentage": round(avg_percentage, 2),
        "best_percentage": round(best_percentage, 2),
        "pass_rate": pass_rate,
        "best_subject": best_subject_row["subject"] if best_subject_row else "-",
        "weak_subject": weak_subject_row["subject"] if weak_subject_row else "-",
        "subject_stats": subject_stats,
        "recent_attempts": recent_attempts,
        "top_students": top_students,
        "top_three_students": top_three_students,
        "trend_labels": [row["attempt_day"] for row in trend_rows],
        "trend_attempt_counts": [int(row["attempts"] or 0) for row in trend_rows],
        "trend_avg_scores": [round(float(row["avg_percentage"] or 0), 2) for row in trend_rows],
        "distribution_labels": ["<33%", "33-59%", "60-79%", "80%+"],
        "distribution_values": [
            int(distribution_row.get("fail_band") or 0),
            int(distribution_row.get("low_band") or 0),
            int(distribution_row.get("good_band") or 0),
            int(distribution_row.get("excellent_band") or 0),
        ],
        "subject_labels": [row["subject"] for row in subject_chart_rows],
        "subject_avg_values": [round(float(row["avg_percentage"] or 0), 2) for row in subject_chart_rows],
        "subject_options": subject_options,
        "selected_subject": clean_subject_filter,
    }


def get_attempts_for_export(subject_filter="all"):
    clean_subject_filter = (subject_filter or "all").strip()
    conn = get_db_connection()
    params = []
    where_clause = ""
    if clean_subject_filter != "all":
        where_clause = "WHERE subject = ?"
        params.append(clean_subject_filter)

    rows = fetchall_dicts(
        conn,
        f"""
        SELECT
            attempts.attempted_at,
            attempts.subject,
            attempts.semester,
            attempts.unit,
            COALESCE(
                NULLIF(
                    CASE
                        WHEN LOWER(TRIM(COALESCE(attempts.student_name, ''))) IN ('backend test user', 'test user', 'backend user') THEN ''
                        ELSE TRIM(COALESCE(attempts.student_name, ''))
                    END,
                    ''
                ),
                NULLIF(TRIM(COALESCE(students.name, '')), ''),
                'Unknown'
            ) AS student_name,
            attempts.score,
            attempts.total,
            attempts.percentage,
            attempts.result_status
        FROM attempts
        LEFT JOIN students ON students.id = attempts.student_id
        {where_clause}
        ORDER BY attempts.id DESC
        """,
        tuple(params),
    )
    conn.close()
    return rows


def get_test_entries(limit=200):
    conn = get_db_connection()
    rows = fetchall_dicts(
        conn,
        """
        SELECT
            attempted_at,
            name,
            student_identifier,
            subject,
            class_name,
            course_slug,
            semester,
            score,
            total,
            percentage,
            result_status
        FROM test_entries
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    conn.close()
    return rows


def get_test_entries_for_export():
    conn = get_db_connection()
    rows = fetchall_dicts(
        conn,
        """
        SELECT
            attempted_at,
            name,
            student_identifier,
            subject,
            class_name,
            course_slug,
            semester,
            score,
            total,
            percentage,
            result_status
        FROM test_entries
        ORDER BY id DESC
        """,
    )
    conn.close()
    return rows


def add_notice(message):
    clean_message = " ".join((message or "").split()).strip()
    if not clean_message:
        return False

    conn = get_db_connection()
    execute_sql(
        conn,
        "INSERT INTO notices (message, created_at, is_active) VALUES (?, ?, 1)",
        (clean_message, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    ).close()
    conn.commit()
    conn.close()
    return True


def get_active_notices(limit=5):
    conn = get_db_connection()
    rows = fetchall_dicts(
        conn,
        """
        SELECT id, message, created_at
        FROM notices
        WHERE is_active = 1
        ORDER BY id DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    conn.close()
    return rows


def deactivate_all_notices():
    conn = get_db_connection()
    execute_sql(conn, "UPDATE notices SET is_active = 0 WHERE is_active = 1").close()
    conn.commit()
    conn.close()


try:
    init_db()
except Exception:
    app.logger.exception("Startup DB init failed")


# ===== ROUTES =====
@app.route("/")
def home():
    notices = get_active_notices(limit=5)
    default_test = get_default_test_target_details()
    return render_template("index.html", notices=notices, default_test=default_test)


@app.route("/notices", methods=["POST"])
def create_notice():
    return redirect(url_for("host_notices"))


@app.route("/host/notices", methods=["GET", "POST"])
def host_notices():
    status = ""
    if request.method == "POST":
        host_key = (request.form.get("host_key") or "").strip()
        notice_message = request.form.get("notice_message", "")
        action = (request.form.get("action") or "add").strip()

        if not NOTICE_HOST_KEY:
            status = "host_key_missing"
        elif host_key != NOTICE_HOST_KEY:
            status = "invalid_key"
        elif action == "clear":
            deactivate_all_notices()
            status = "cleared"
        elif add_notice(notice_message):
            status = "added"
        else:
            status = "empty"

    notices = get_active_notices(limit=10)
    return render_template("host_notices.html", notices=notices, status=status)


@app.route("/courses")
def courses():
    course_cards = []
    test_course_slug = "bsc"
    for course in COURSES:
        slug = course["slug"]
        active_semesters = get_active_semesters(slug)
        if active_semesters and test_course_slug == "bsc":
            test_course_slug = slug
        course_cards.append(
            {
                "slug": slug,
                "label": course["label"],
                "active_semesters": active_semesters,
            }
        )
    return render_template("courses.html", courses=course_cards, test_course_slug=test_course_slug)


@app.route("/semesters")
def semesters():
    return redirect(url_for("courses"))


@app.route("/semesters/<course_slug>")
def semesters_by_course(course_slug):
    clean_course_slug = normalize_course_slug(course_slug)
    return render_template(
        "semesters.html",
        course_slug=clean_course_slug,
        course_label=COURSE_LABELS[clean_course_slug],
        active_semester=ACTIVE_SEMESTER,
        active_semesters=get_active_semesters(clean_course_slug),
        total_semesters=TOTAL_SEMESTERS,
    )


@app.route("/subjects")
@app.route("/subjects/<course_slug>/<int:semester>")
def subjects(course_slug="bsc", semester=ACTIVE_SEMESTER):
    clean_course_slug = normalize_course_slug(course_slug)
    if semester not in get_active_semesters(clean_course_slug):
        return redirect(url_for("semesters_by_course", course_slug=clean_course_slug))

    return render_template(
        "subjects.html",
        semester=semester,
        course_slug=clean_course_slug,
        course_label=COURSE_LABELS[clean_course_slug],
        subjects=get_course_subjects(clean_course_slug),
    )


@app.route("/units/<course_slug>/<subject>/<int:semester>")
@app.route("/units/<subject>/<int:semester>")
def units(subject, semester, course_slug="bsc"):
    clean_course_slug = normalize_course_slug(course_slug)
    unit_list = get_units_for_subject(subject, semester)
    return render_template(
        "units.html",
        subject=subject,
        semester=semester,
        units=unit_list,
        course_slug=clean_course_slug,
        course_label=COURSE_LABELS[clean_course_slug],
    )


@app.route("/quiz/<course_slug>/<subject>/<int:semester>")
@app.route("/quiz/<subject>/<int:semester>")
def quiz_redirect(subject, semester, course_slug="bsc"):
    clean_course_slug = normalize_course_slug(course_slug)
    # Keep old URL working by redirecting to unit selection
    return redirect(
        url_for(
            "units",
            course_slug=clean_course_slug,
            subject=subject,
            semester=semester,
        )
    )


@app.route("/test/<course_slug>")
def test_semesters(course_slug):
    clean_course_slug = normalize_course_slug(course_slug)
    return render_template(
        "test_semesters.html",
        course_slug=clean_course_slug,
        course_label=COURSE_LABELS[clean_course_slug],
        active_semesters=get_active_semesters(clean_course_slug),
        total_semesters=TOTAL_SEMESTERS,
    )


@app.route("/test")
@app.route("/test/start")
def test_start():
    target = get_default_test_target()
    if not target:
        return redirect(url_for("courses"))
    course_slug, semester, unit = target
    return redirect(url_for("subject_test", course_slug=course_slug, semester=semester, unit=unit))


@app.route("/test/<course_slug>/<int:semester>")
def test_units_by_semester(course_slug, semester):
    clean_course_slug = normalize_course_slug(course_slug)
    if semester not in get_active_semesters(clean_course_slug):
        return redirect(url_for("test_semesters", course_slug=clean_course_slug))

    units = get_test_units_for_course_semester(clean_course_slug, semester)
    return render_template(
        "test_units.html",
        course_slug=clean_course_slug,
        course_label=COURSE_LABELS[clean_course_slug],
        semester=semester,
        units=units,
    )


@app.route("/test/<course_slug>/<int:semester>/<unit>", methods=["GET", "POST"])
def subject_test(course_slug, semester, unit):
    clean_course_slug = normalize_course_slug(course_slug)
    if semester not in get_active_semesters(clean_course_slug):
        return redirect(url_for("test_semesters", course_slug=clean_course_slug))

    all_semester_test_questions = [
        q for q in attach_units(get_course_semester_test_questions(clean_course_slug, semester))
        if q.get("unit", "").strip() == unit
    ]
    subject_names = sorted({(q.get("subject") or "").strip() for q in all_semester_test_questions if (q.get("subject") or "").strip()})
    subject_label = ", ".join(subject_names) if subject_names else "Subject Test"

    test_window = get_subject_test_window_status()
    if test_window["enabled"] and not test_window["is_open"]:
        return render_template(
            "test_locked.html",
            unit=unit,
            subject_label=subject_label,
            semester=semester,
            course_slug=clean_course_slug,
            course_label=COURSE_LABELS[clean_course_slug],
            test_window=test_window,
        )

    selected_indices = list(range(len(all_semester_test_questions)))

    if request.method == "POST":
        test_window = get_subject_test_window_status()
        if test_window["enabled"] and not test_window["is_open"]:
            return render_template(
                "test_locked.html",
                unit=unit,
                subject_label=subject_label,
                semester=semester,
                course_slug=clean_course_slug,
                course_label=COURSE_LABELS[clean_course_slug],
                test_window=test_window,
            )

        raw_order = (request.form.get("question_order") or "").strip()
        parsed_indices = []
        if raw_order:
            seen = set()
            for token in raw_order.split(","):
                token = token.strip()
                if not token.isdigit():
                    continue
                idx = int(token)
                if idx in seen:
                    continue
                if 0 <= idx < len(all_semester_test_questions):
                    parsed_indices.append(idx)
                    seen.add(idx)
        if not parsed_indices:
            return render_template(
                "test.html",
                questions=[],
                unit=unit,
                subject_label=subject_label,
                semester=semester,
                course_slug=clean_course_slug,
                course_label=COURSE_LABELS[clean_course_slug],
                test_duration_minutes=SUBJECT_TEST_DURATION_MINUTES,
                test_window=test_window,
                question_order="",
                error_message="Test session mismatch detected. Please restart this test.",
            )
        selected_indices = parsed_indices
    else:
        random.shuffle(selected_indices)

    selected_indices = selected_indices[:SUBJECT_TEST_QUESTION_LIMIT]
    questions = [all_semester_test_questions[i] for i in selected_indices]
    question_order = ",".join(str(i) for i in selected_indices)

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        student_identifier = (request.form.get("student_identifier") or "").strip()
        class_name = (request.form.get("class_name") or "").strip()
        subject_entry = (request.form.get("subject_entry") or subject_label).strip()
        email = (request.form.get("email") or "").strip()

        if not name:
            return render_template(
                "test.html",
                questions=questions,
                unit=unit,
                subject_label=subject_label,
                semester=semester,
                course_slug=clean_course_slug,
                course_label=COURSE_LABELS[clean_course_slug],
                test_duration_minutes=SUBJECT_TEST_DURATION_MINUTES,
                test_window=test_window,
                question_order=question_order,
                error_message="Please enter your name.",
            )
        if not student_identifier:
            return render_template(
                "test.html",
                questions=questions,
                unit=unit,
                subject_label=subject_label,
                semester=semester,
                course_slug=clean_course_slug,
                course_label=COURSE_LABELS[clean_course_slug],
                test_duration_minutes=SUBJECT_TEST_DURATION_MINUTES,
                test_window=test_window,
                question_order=question_order,
                error_message="Please enter your student ID.",
            )
        if not class_name:
            return render_template(
                "test.html",
                questions=questions,
                unit=unit,
                subject_label=subject_label,
                semester=semester,
                course_slug=clean_course_slug,
                course_label=COURSE_LABELS[clean_course_slug],
                test_duration_minutes=SUBJECT_TEST_DURATION_MINUTES,
                test_window=test_window,
                question_order=question_order,
                error_message="Please enter your class.",
            )
        if normalize_answer_text(subject_entry) != normalize_answer_text(subject_label):
            return render_template(
                "test.html",
                questions=questions,
                unit=unit,
                subject_label=subject_label,
                semester=semester,
                course_slug=clean_course_slug,
                course_label=COURSE_LABELS[clean_course_slug],
                test_duration_minutes=SUBJECT_TEST_DURATION_MINUTES,
                test_window=test_window,
                question_order=question_order,
                error_message="Subject mismatch detected. Please restart this test.",
            )
        if not email:
            return render_template(
                "test.html",
                questions=questions,
                unit=unit,
                subject_label=subject_label,
                semester=semester,
                course_slug=clean_course_slug,
                course_label=COURSE_LABELS[clean_course_slug],
                test_duration_minutes=SUBJECT_TEST_DURATION_MINUTES,
                test_window=test_window,
                question_order=question_order,
                error_message="Please enter your email.",
            )
        if not is_valid_email(email):
            return render_template(
                "test.html",
                questions=questions,
                unit=unit,
                subject_label=subject_label,
                semester=semester,
                course_slug=clean_course_slug,
                course_label=COURSE_LABELS[clean_course_slug],
                test_duration_minutes=SUBJECT_TEST_DURATION_MINUTES,
                test_window=test_window,
                question_order=question_order,
                error_message="Please enter a valid email address.",
            )

        score = 0
        total = len(questions)
        for i, q in enumerate(questions):
            submitted = request.form.get(f"q{i}")
            expected = q.get("answer")
            if normalize_answer_text(submitted) == normalize_answer_text(expected):
                score += 1

        percentage = round((score / total) * 100, 2) if total > 0 else 0
        result_status = "Pass" if percentage >= 33 else "Fail"
        save_test_entry(
            name=name,
            student_identifier=student_identifier,
            subject=subject_entry,
            class_name=class_name,
            course_slug=clean_course_slug,
            semester=semester,
            score=score,
            total=total,
            percentage=percentage,
            result_status=result_status,
        )
        email_sent, email_status = send_test_score_email(
            to_email=email,
            student_name=name,
            subject=subject_entry,
            semester=semester,
            score=score,
            total=total,
            percentage=percentage,
            result_status=result_status,
        )

        return render_template(
            "result.html",
            score=score,
            total=total,
            percentage=percentage,
            result_status=result_status,
            subject=subject_label,
            semester=semester,
            unit=unit,
            course_slug=clean_course_slug,
            course_label=COURSE_LABELS[clean_course_slug],
            student_name=name,
            subject_rank=None,
            subject_total_students=None,
            overall_rank=None,
            overall_total_students=None,
            subject_percentile=None,
            overall_percentile=None,
            show_leaderboard=False,
            is_test_result=True,
            email=email,
            email_sent=email_sent,
            email_status=email_status,
            test_start_url=url_for("test_start"),
        )

    return render_template(
        "test.html",
        questions=questions,
        unit=unit,
        subject_label=subject_label,
        semester=semester,
        course_slug=clean_course_slug,
        course_label=COURSE_LABELS[clean_course_slug],
        test_duration_minutes=SUBJECT_TEST_DURATION_MINUTES,
        test_window=test_window,
        question_order=question_order,
        error_message="",
    )


@app.route("/quiz/<course_slug>/<subject>/<int:semester>/<unit>", methods=["GET", "POST"])
@app.route("/quiz/<subject>/<int:semester>/<unit>", methods=["GET", "POST"])
def quiz(subject, semester, unit, course_slug="bsc"):
    clean_course_slug = normalize_course_slug(course_slug)
    unit_questions = [
        q for q in attach_units(get_subject_semester_questions(subject, semester))
        if q["unit"] == unit
    ]
    selected_indices = list(range(len(unit_questions)))

    if request.method == "POST":
        raw_order = (request.form.get("question_order") or "").strip()
        parsed_indices = []
        if raw_order:
            seen = set()
            for token in raw_order.split(","):
                token = token.strip()
                if not token.isdigit():
                    continue
                idx = int(token)
                if idx in seen:
                    continue
                if 0 <= idx < len(unit_questions):
                    parsed_indices.append(idx)
                    seen.add(idx)
        if not parsed_indices:
            return render_template(
                "quiz.html",
                questions=questions,
                subject=subject,
                semester=semester,
                unit=unit,
                course_slug=clean_course_slug,
                course_label=COURSE_LABELS[clean_course_slug],
                test_duration_minutes=QUIZ_DURATION_MINUTES,
                question_order=question_order,
                error_message="Quiz session mismatch detected. Please restart this test.",
            )
        selected_indices = parsed_indices
    else:
        random.shuffle(selected_indices)

    selected_indices = selected_indices[:QUIZ_QUESTION_LIMIT]
    questions = [unit_questions[i] for i in selected_indices]
    question_order = ",".join(str(i) for i in selected_indices)

    if request.method == "POST":
        student_name = request.form.get("student_name", "").strip()
        student_mobile = request.form.get("student_mobile", "").strip()
        if not student_name:
            return render_template(
                "quiz.html",
                questions=questions,
                subject=subject,
                semester=semester,
                unit=unit,
                course_slug=clean_course_slug,
                course_label=COURSE_LABELS[clean_course_slug],
                test_duration_minutes=QUIZ_DURATION_MINUTES,
                question_order=question_order,
                error_message="Please enter your name.",
            )
        if not (student_mobile.isdigit() and len(student_mobile) == 10):
            return render_template(
                "quiz.html",
                questions=questions,
                subject=subject,
                semester=semester,
                unit=unit,
                course_slug=clean_course_slug,
                course_label=COURSE_LABELS[clean_course_slug],
                test_duration_minutes=QUIZ_DURATION_MINUTES,
                question_order=question_order,
                error_message="Mobile number must be exactly 10 digits.",
            )

        score = 0
        total = len(questions)

        for i, q in enumerate(questions):
            submitted = request.form.get(f"q{i}")
            expected = q.get("answer")
            if normalize_answer_text(submitted) == normalize_answer_text(expected):
                score += 1

        percentage = round((score / total) * 100, 2) if total > 0 else 0
        result_status = "Pass" if percentage >= 33 else "Fail"
        student_id = save_attempt(
            subject,
            semester,
            unit,
            score,
            total,
            percentage,
            result_status,
            student_name=student_name,
            student_mobile=student_mobile,
        )
        subject_rank, subject_total_students = get_student_rank(student_id, subject)
        overall_rank, overall_total_students = get_student_rank(student_id, "all")
        subject_percentile = get_percentile(subject_rank, subject_total_students)
        overall_percentile = get_percentile(overall_rank, overall_total_students)

        return render_template(
            "result.html",
            score=score,
            total=total,
            percentage=percentage,
            result_status=result_status,
            subject=subject,
            semester=semester,
            unit=unit,
            course_slug=clean_course_slug,
            course_label=COURSE_LABELS[clean_course_slug],
            student_name=student_name,
            subject_rank=subject_rank,
            subject_total_students=subject_total_students,
            overall_rank=overall_rank,
            overall_total_students=overall_total_students,
            subject_percentile=subject_percentile,
            overall_percentile=overall_percentile,
        )

    return render_template(
        "quiz.html",
        questions=questions,
        subject=subject,
        semester=semester,
        unit=unit,
        course_slug=clean_course_slug,
        course_label=COURSE_LABELS[clean_course_slug],
        test_duration_minutes=QUIZ_DURATION_MINUTES,
        question_order=question_order,
        error_message="",
    )


@app.route("/result")
def result():
    return redirect(url_for("subjects"))


@app.route("/dashboard")
def dashboard():
    selected_subject = request.args.get("subject", "all")
    stats = get_dashboard_data(selected_subject)
    host_key = (request.args.get("host_key") or "").strip()
    can_view_test_entries = is_host_authorized()
    return render_template(
        "dashboard.html",
        can_view_test_entries=can_view_test_entries,
        host_key=host_key if can_view_test_entries else "",
        **stats,
    )


@app.route("/dashboard/export.csv")
def dashboard_export_csv():
    selected_subject = request.args.get("subject", "all")
    rows = get_attempts_for_export(selected_subject)

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(["Attempted At", "Subject", "Semester", "Unit", "Student Name", "Score", "Total", "Percentage", "Result"])
    for row in rows:
        writer.writerow(
            [
                row.get("attempted_at") or "",
                row.get("subject") or "",
                row.get("semester") or "",
                row.get("unit") or "",
                row.get("student_name") or "",
                row.get("score") or "",
                row.get("total") or "",
                row.get("percentage") or "",
                row.get("result_status") or "",
            ]
        )

    filename = f"dashboard_report_{(selected_subject or 'all').replace(' ', '_').lower()}.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/test-entries")
def test_entries():
    if not is_host_authorized():
        abort(403)
    rows = get_test_entries(limit=300)
    return render_template("test_entries.html", rows=rows)


@app.route("/test-entries/export.csv")
def test_entries_export_csv():
    if not is_host_authorized():
        abort(403)
    rows = get_test_entries_for_export()

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "Attempted At",
        "Name",
        "Student ID",
        "Subject",
        "Class",
        "Course",
        "Semester",
        "Score",
        "Total",
        "Percentage",
        "Result",
    ])
    for row in rows:
        writer.writerow(
            [
                row.get("attempted_at") or "",
                row.get("name") or "",
                row.get("student_identifier") or "",
                row.get("subject") or "",
                row.get("class_name") or "",
                row.get("course_slug") or "",
                row.get("semester") or "",
                row.get("score") or "",
                row.get("total") or "",
                row.get("percentage") or "",
                row.get("result_status") or "",
            ]
        )

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=test_entries_report.csv"},
    )


@app.route("/debug-db")
def debug_db():
    conn = get_db_connection()
    try:
        attempts_count = fetchone_dict(conn, "SELECT COUNT(*) AS count FROM attempts")["count"]
        students_count = fetchone_dict(conn, "SELECT COUNT(*) AS count FROM students")["count"]
    finally:
        conn.close()

    active_db = "postgres" if using_postgres() else "sqlite"
    db_target = normalize_database_url(DATABASE_URL) if using_postgres() else str(DB_PATH)

    return jsonify(
        {
            "active_db": active_db,
            "db_target": db_target,
            "counts": {
                "students": int(students_count or 0),
                "attempts": int(attempts_count or 0),
            },
            "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    )


@app.route("/about")
def about():
    gallery_images = get_gallery_images()
    return render_template("about.html", gallery_images=gallery_images)


@app.route("/contact")
def contact():
    return render_template("contact.html")


# ===== RUN APP =====
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
