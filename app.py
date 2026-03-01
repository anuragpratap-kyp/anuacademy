import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from flask import Flask, redirect, render_template, request, url_for

app = Flask(__name__)
DB_PATH = Path(__file__).with_name("quiz_history.db")
QUESTIONS_PATH = Path(__file__).with_name("questions.json")

# Load questions from JSON
with QUESTIONS_PATH.open(encoding="utf-8") as f:
    all_questions = json.load(f)


def load_questions():
    with QUESTIONS_PATH.open(encoding="utf-8") as f:
        return json.load(f)


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


def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db_connection()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS students (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            mobile TEXT NOT NULL,
            UNIQUE(name, mobile)
        )
        """
    )
    conn.execute(
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
        """
    )

    attempt_cols = {row["name"] for row in conn.execute("PRAGMA table_info(attempts)").fetchall()}
    if "student_id" not in attempt_cols:
        conn.execute("ALTER TABLE attempts ADD COLUMN student_id INTEGER")
    if "student_name" not in attempt_cols:
        conn.execute("ALTER TABLE attempts ADD COLUMN student_name TEXT")
    if "student_mobile" not in attempt_cols:
        conn.execute("ALTER TABLE attempts ADD COLUMN student_mobile TEXT")

    conn.commit()
    conn.close()


def get_or_create_student(name, mobile):
    clean_name = (name or "").strip()
    clean_mobile = (mobile or "").strip()
    if not clean_name or not clean_mobile:
        return None

    conn = get_db_connection()
    existing = conn.execute(
        "SELECT id FROM students WHERE name = ? AND mobile = ?",
        (clean_name, clean_mobile),
    ).fetchone()

    if existing:
        student_id = existing["id"]
    else:
        conn.execute(
            "INSERT INTO students (name, mobile) VALUES (?, ?)",
            (clean_name, clean_mobile),
        )
        conn.commit()
        student_id = conn.execute(
            "SELECT id FROM students WHERE name = ? AND mobile = ?",
            (clean_name, clean_mobile),
        ).fetchone()["id"]

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
    conn.execute(
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
    )
    conn.commit()
    conn.close()


def get_dashboard_data():
    conn = get_db_connection()

    total_attempts = conn.execute("SELECT COUNT(*) FROM attempts").fetchone()[0]
    avg_percentage = conn.execute("SELECT AVG(percentage) FROM attempts").fetchone()[0] or 0
    best_percentage = conn.execute("SELECT MAX(percentage) FROM attempts").fetchone()[0] or 0
    pass_count = conn.execute(
        "SELECT COUNT(*) FROM attempts WHERE result_status = 'Pass'"
    ).fetchone()[0]
    pass_rate = round((pass_count / total_attempts) * 100, 2) if total_attempts else 0

    best_subject_row = conn.execute(
        """
        SELECT subject, AVG(percentage) AS avg_score
        FROM attempts
        GROUP BY subject
        ORDER BY avg_score DESC
        LIMIT 1
        """
    ).fetchone()

    weak_subject_row = conn.execute(
        """
        SELECT subject, AVG(percentage) AS avg_score
        FROM attempts
        GROUP BY subject
        ORDER BY avg_score ASC
        LIMIT 1
        """
    ).fetchone()

    subject_stats = conn.execute(
        """
        SELECT
            subject,
            COUNT(*) AS attempts,
            AVG(percentage) AS avg_percentage,
            SUM(CASE WHEN result_status = 'Pass' THEN 1 ELSE 0 END) AS passed
        FROM attempts
        GROUP BY subject
        ORDER BY attempts DESC, subject ASC
        """
    ).fetchall()

    recent_attempts = conn.execute(
        """
        SELECT attempted_at, subject, semester, unit, score, total, percentage, result_status,
               student_name, student_mobile
        FROM attempts
        ORDER BY id DESC
        LIMIT 10
        """
    ).fetchall()

    top_students = conn.execute(
        """
        SELECT
            COALESCE(NULLIF(student_name, ''), 'Unknown') AS student_name,
            student_mobile,
            MAX(percentage) AS top_percentage,
            COUNT(*) AS attempts
        FROM attempts
        WHERE student_name IS NOT NULL AND student_name != ''
          AND student_mobile IS NOT NULL AND student_mobile != ''
        GROUP BY student_name, student_mobile
        ORDER BY top_percentage DESC, attempts DESC, student_name ASC
        LIMIT 5
        """
    ).fetchall()

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

        score = 0
        total = len(questions)

        for i, q in enumerate(questions):
            if request.form.get(f"q{i}") == q["answer"]:
                score += 1

        percentage = round((score / total) * 100, 2) if total > 0 else 0
        result_status = "Pass" if percentage >= 33 else "Fail"
        save_attempt(
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
        )

    return render_template(
        "quiz.html",
        questions=questions,
        subject=subject,
        semester=semester,
        unit=unit,
    )


@app.route("/result")
def result():
    return render_template("result.html", score=0, total=0, percentage=0, result_status="Fail")


@app.route("/dashboard")
def dashboard():
    stats = get_dashboard_data()
    return render_template("dashboard.html", **stats)


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/contact")
def contact():
    return render_template("contact.html")


# ===== RUN APP =====
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
