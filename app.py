import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from flask import Flask, redirect, render_template, request, url_for

try:
    import psycopg2
except ImportError:
    psycopg2 = None

app = Flask(__name__)
DEFAULT_DB_PATH = Path(__file__).with_name("quiz_history.db")
DB_PATH = Path(os.environ.get("DB_PATH", str(DEFAULT_DB_PATH)))
DATABASE_URL = (os.environ.get("DATABASE_URL") or "").strip()
QUESTIONS_PATH = Path(__file__).with_name("questions.json")
GALLERY_DIR = Path(__file__).with_name("static") / "gallery"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif"}

# Load questions from JSON
with QUESTIONS_PATH.open(encoding="utf-8") as f:
    all_questions = json.load(f)


def load_questions():
    with QUESTIONS_PATH.open(encoding="utf-8") as f:
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
    ]


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

            execute_sql(conn, "ALTER TABLE attempts ADD COLUMN IF NOT EXISTS student_id INTEGER").close()
            execute_sql(conn, "ALTER TABLE attempts ADD COLUMN IF NOT EXISTS student_name TEXT").close()
        else:
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

            attempt_cols_rows = fetchall_dicts(conn, "PRAGMA table_info(attempts)")
            attempt_cols = {row["name"] for row in attempt_cols_rows}
            if "student_id" not in attempt_cols:
                execute_sql(conn, "ALTER TABLE attempts ADD COLUMN student_id INTEGER").close()
            if "student_name" not in attempt_cols:
                execute_sql(conn, "ALTER TABLE attempts ADD COLUMN student_name TEXT").close()

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
            student_id, student_name
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        ),
    ).close()
    conn.commit()
    conn.close()
    return student_id


def get_leaderboard_rows(conn, subject_filter="all", limit=10):
    clean_subject_filter = (subject_filter or "all").strip()
    leaderboard_filters = [
        "student_name IS NOT NULL",
        "student_name != ''",
        "student_id IS NOT NULL",
    ]
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
        SELECT
            student_id,
            COALESCE(NULLIF(student_name, ''), 'Unknown') AS student_name,
            MAX(percentage) AS top_percentage,
            AVG(percentage) AS avg_percentage,
            COUNT(*) AS attempts
        FROM attempts
        WHERE {" AND ".join(leaderboard_filters)}
        GROUP BY student_name, student_id
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

    total_attempts = fetchone_dict(conn, "SELECT COUNT(*) AS count FROM attempts")["count"]
    avg_percentage = fetchone_dict(conn, "SELECT AVG(percentage) AS value FROM attempts")["value"] or 0
    best_percentage = fetchone_dict(conn, "SELECT MAX(percentage) AS value FROM attempts")["value"] or 0
    pass_count = fetchone_dict(
        conn,
        "SELECT COUNT(*) AS count FROM attempts WHERE result_status = 'Pass'",
    )["count"]
    pass_rate = round((pass_count / total_attempts) * 100, 2) if total_attempts else 0

    best_subject_row = fetchone_dict(
        conn,
        """
        SELECT subject, AVG(percentage) AS avg_score
        FROM attempts
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
        GROUP BY subject
        ORDER BY attempts DESC, subject ASC
        """,
    )

    recent_attempts = fetchall_dicts(
        conn,
        """
        SELECT attempted_at, subject, semester, unit, score, total, percentage, result_status,
               student_name
        FROM attempts
        ORDER BY id DESC
        LIMIT 10
        """,
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
        "subject_options": subject_options,
        "selected_subject": clean_subject_filter,
    }


init_db()


# ===== ROUTES =====
@app.route("/")
def home():
    return render_template("index.html")


@app.route("/semesters")
def semesters():
    return render_template("semesters.html")


@app.route("/subjects")
def subjects():
    # For now only semester 4 is active
    return render_template("subjects.html", semester=4)


@app.route("/units/<subject>/<int:semester>")
def units(subject, semester):
    unit_list = get_units_for_subject(subject, semester)
    return render_template("units.html", subject=subject, semester=semester, units=unit_list)


@app.route("/quiz/<subject>/<int:semester>")
def quiz_redirect(subject, semester):
    # Keep old URL working by redirecting to unit selection
    return redirect(url_for("units", subject=subject, semester=semester))


@app.route("/quiz/<subject>/<int:semester>/<unit>", methods=["GET", "POST"])
def quiz(subject, semester, unit):
    questions = [
        q for q in attach_units(get_subject_semester_questions(subject, semester))
        if q["unit"] == unit
    ]

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
                error_message="Please enter your name.",
            )
        if not (student_mobile.isdigit() and len(student_mobile) == 10):
            return render_template(
                "quiz.html",
                questions=questions,
                subject=subject,
                semester=semester,
                unit=unit,
                error_message="Mobile number must be exactly 10 digits.",
            )

        score = 0
        total = len(questions)

        for i, q in enumerate(questions):
            if request.form.get(f"q{i}") == q["answer"]:
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
        error_message="",
    )


@app.route("/result")
def result():
    return redirect(url_for("subjects"))


@app.route("/dashboard")
def dashboard():
    selected_subject = request.args.get("subject", "all")
    stats = get_dashboard_data(selected_subject)
    return render_template("dashboard.html", **stats)


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

