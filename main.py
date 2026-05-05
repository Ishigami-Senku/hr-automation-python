"""
main.py — ETL-процесс для объединения данных из двух SQLite-баз.

Источник 1 (source_1.db): проекты сотрудников
Источник 2 (source_2.db): HR-информация сотрудников
Результат (warehouse.db): объединённая таблица с расчётом общей компенсации
"""

import sqlite3
import os


# 1. ФУНКЦИИ ДЛЯ РАБОТЫ С БАЗАМИ ДАННЫХ

def create_connection(db_path: str) -> sqlite3.Connection:
    """Создаёт подключение к SQLite-базе данных."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # доступ к полям по именам
    return conn


def drop_if_exists(db_path: str) -> None:
    """Удаляет базу данных, если она уже существует (для повторных запусков)."""
    if os.path.exists(db_path):
        os.remove(db_path)


# 2. ГЕНЕРАЦИЯ БАЗ ДАННЫХ ДЛЯ ТЕСТА

def init_source_1(conn: sqlite3.Connection) -> None:
    """
    Создаёт таблицу projects в source_1.db и заполняет её данными.
    Формат ФИО: «Фамилия Имя» (стандартный русский формат).
    """
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS projects (
            fio          TEXT NOT NULL,
            role         TEXT NOT NULL,
            project_name TEXT NOT NULL,
            stack        TEXT NOT NULL
        )
    """)

    data = [
        ("Иванов Иван",       "Backend Developer",   "ProjectAlpha", "Python, FastAPI, PostgreSQL"),
        ("Петрова Мария",     "Frontend Developer",  "ProjectBeta",  "React, TypeScript, Tailwind"),
        ("Сидоров Алексей",   "DevOps Engineer",     "ProjectGamma", "Docker, Kubernetes, AWS"),
        ("Кузнецова Елена",   "Data Scientist",      "ProjectDelta", "Python, Pandas, Scikit-learn"),
        ("Морозов Дмитрий",   "QA Lead",             "ProjectEpsilon", "Selenium, Pytest, JIRA"),
    ]

    cur.executemany(
        "INSERT INTO projects (fio, role, project_name, stack) VALUES (?, ?, ?, ?)",
        data,
    )
    conn.commit()
    print(f"[source_1] Вставлено {len(data)} записей в projects.")


def init_source_2(conn: sqlite3.Connection) -> None:
    """
    Создаёт таблицу hr_info в source_2.db и заполняет её данными.
    """
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hr_info (
            fio      TEXT NOT NULL,
            skills   TEXT NOT NULL,
            salary   REAL NOT NULL,
            bonus    REAL NOT NULL
        )
    """)

    data = [
        ("иван иванов",     "Python, SQL",           150000, 25000),
        ("Петрова М.",      "JavaScript, CSS",       130000, 15000),
        ("СИДОРОВ А.В.",    "Docker, Terraform",     170000, 30000),
        ("Кузнецова Е.",    "Python, ML, NLP",       160000, 20000),
        ("Морозов Д.С.",    "Selenium, Java",        140000, 18000),
    ]

    cur.executemany(
        "INSERT INTO hr_info (fio, skills, salary, bonus) VALUES (?, ?, ?, ?)",
        data,
    )
    conn.commit()
    print(f"[source_2] Вставлено {len(data)} записей в hr_info.")

# 3. НОРМАЛИЗАЦИЯ ФИО

def normalize_fio(raw_fio: str) -> str:
    # Чистим всё лишнее
    cleaned = raw_fio.lower().replace('.', ' ').replace(',', ' ').strip()
    tokens = cleaned.split()
    if not tokens: return ""

    # Находим фамилию (самое длинное слово)
    surname = max(tokens, key=len)

    # Находим все остальные слова (имена/отчества/инициалы)
    others = [t for t in tokens if t != surname]

    # Берем только первую букву самого первого встречного слова (имени)
    # Если имя есть, берем его первую букву, если нет - ничего
    first_initial = others[0][0] if others else ""

    return f"{surname}_{first_initial}"

# 4. ETL (Extract → Transform → Load)

def extract_source_1(conn: sqlite3.Connection) -> list[tuple[str, str, str]]:
    """Извлекает все записи из source_1.db (fio, role, project_name, stack)."""
    cur = conn.cursor()
    cur.execute("SELECT fio, role, project_name, stack FROM projects")
    rows = cur.fetchall()
    return [(row["fio"], row["role"], row["project_name"], row["stack"]) for row in rows]


def extract_source_2(conn: sqlite3.Connection) -> list[tuple[str, str, float, float]]:
    """Извлекает все записи из source_2.db (fio, skills, salary, bonus)."""
    cur = conn.cursor()
    cur.execute("SELECT fio, skills, salary, bonus FROM hr_info")
    rows = cur.fetchall()
    return [
        (row["fio"], row["skills"], row["salary"], row["bonus"])
        for row in rows
    ]


def transform_normalize(records: list[tuple], normalize_index: int = 0) -> list[tuple]:
    result = []
    for record in records:
        # Берем именно ФИО по индексу
        fio_value = str(record[normalize_index])
        key = normalize_fio(fio_value)
        result.append((key, *record))
    return result


def load_warehouse(warehouse_conn: sqlite3.Connection, norm_p1: list, norm_p2: list) -> None:
    """Сопоставляет источники по ключу и загружает данные в warehouse.db."""
    cur = warehouse_conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            fio TEXT NOT NULL, role TEXT, project_name TEXT, stack TEXT,
            skills TEXT, salary REAL NOT NULL, bonus REAL NOT NULL,
            total_compensation REAL NOT NULL
        )
    """)

    # Индексируем данные
    hr_map = {entry[0]: entry for entry in norm_p2}

    inserted = 0
    not_found = []

    for entry in norm_p1:
        key, raw_fio, role, project_name, stack = entry

        if key in hr_map:
            hr_entry = hr_map[key]
            # hr_entry имеет вид: (key, fio, skills, salary, bonus)
            skills, salary, bonus = hr_entry[2], hr_entry[3], hr_entry[4]

            cur.execute("""
                INSERT INTO employees 
                (fio, role, project_name, stack, skills, salary, bonus, total_compensation)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (raw_fio, role, project_name, stack, skills, salary, bonus, round(salary + bonus, 2)))
            inserted += 1
        else:
            not_found.append(raw_fio)

    warehouse_conn.commit()
    print(f"[warehouse] Вставлено записей: {inserted}")
    if not_found:
        print(f"[warehouse] Не найдены совпадения для: {not_found}")

# 5. ВЫВОД РЕЗУЛЬТАТА

def display_warehouse(conn: sqlite3.Connection) -> None:
    """Выводит содержимое таблицы employees в читаемом виде."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM employees ORDER BY fio")
    rows = cur.fetchall()

    if not rows:
        print("\n[warehouse] Таблица пуста.")
        return

    # Определяем ширину колонок
    headers = [
        "ФИО",
        "Роль",
        "Проект",
        "Стек",
        "Навыки",
        "Зарплата",
        "Бонус",
        "Общая компенсация",
    ]

    col_widths = [len(h) for h in headers]
    data_rows = []
    for row in rows:
        data_rows.append([
            str(row["fio"]),
            str(row["role"] or ""),
            str(row["project_name"] or ""),
            str(row["stack"] or ""),
            str(row["skills"] or ""),
            f"{row['salary']:,.0f}",
            f"{row['bonus']:,.0f}",
            f"{row['total_compensation']:,.0f}",
        ])
        for i, val in enumerate(data_rows[-1]):
            col_widths[i] = max(col_widths[i], len(val))

    # Формируем разделитель
    sep = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"

    print("\n" + sep)
    header_line = "| " + " | ".join(
        h.ljust(col_widths[i]) for i, h in enumerate(headers)
    ) + " |"
    print(header_line)
    print(sep)

    for dr in data_rows:
        row_line = "| " + " | ".join(
            val.ljust(col_widths[i]) for i, val in enumerate(dr)
        ) + " |"
        print(row_line)

    print(sep)
    print(f"\nВсего записей: {len(rows)}\n")


# 6. ОСНОВНАЯ ФУНКЦИЯ

def main() -> None:
    """Точка входа: полный ETL-процесс."""
    print("ETL-процесс: Объединение данных из двух источников")

    # 1. Очистка старых данных
    drop_if_exists("source_1.db")
    drop_if_exists("source_2.db")
    drop_if_exists("warehouse.db")

    # 2. Создание источников
    print("\n--- Генерация источников данных---")
    conn1 = create_connection("source_1.db")
    init_source_1(conn1)
    conn2 = create_connection("source_2.db")
    init_source_2(conn2)

    # 3. Извлечение (Extract)
    raw_p1 = extract_source_1(conn1)
    raw_p2 = extract_source_2(conn2)

    # 4. Трансформация (Transform)
    print("\n---Трансформация (нормализация ФИО)---")
    norm_p1 = transform_normalize(raw_p1, normalize_index=0)
    norm_p2 = transform_normalize(raw_p2, normalize_index=0)

    # Принты для проверки
    for entry in norm_p1:
        print(f" Source 1: '{entry[1]}' -> ключ={entry[0]}")
    for entry in norm_p2:
        print(f" Source 2: '{entry[1]}' -> ключ={entry[0]}")

    # 5. Загрузка (Load)
    print("\n---Загрузка в warehouse---")
    conn_wh = create_connection("warehouse.db")
    load_warehouse(conn_wh, norm_p1, norm_p2)

    # 6. Итог
    display_warehouse(conn_wh)

    # Закрытие
    conn1.close()
    conn2.close()
    conn_wh.close()
    print("Готово.")


if __name__ == "__main__":
    main()