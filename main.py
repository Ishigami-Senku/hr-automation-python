"""
main.py — ETL-процесс для объединения данных из двух SQLite-баз.
Источник 1 (source_1.db): проекты сотрудников
Источник 2 (source_2.db): HR-информация сотрудников
Результат (warehouse.db): объединённая таблица с расчётом общей компенсации

Реализует:
- Словарь исправлений (lookup table) для известных опечаток
- Удаление лишних символов (точки, запятые, пробелы)
- Обработку перестановок слов ("Иван Иванов" = "Иванов Иван")
- Поиск по похожим именам (сравнение префиксов)
- Хэш-таблицу для O(1) поиска
"""
import sqlite3
import os



# 1. СЛОВАРЬ ИСПРАВЛЕНИЙ (Lookup Table)

FIO_CORRECTIONS = {
    "иван иванов": "Иванов Иван",
    "петрова м": "Петрова Мария",
    "петрова м.": "Петрова Мария",
    "сидоров а в": "Сидоров Алексей",
    "сидоров а.в.": "Сидоров Алексей",
    "сидоров а в ": "Сидоров Алексей",
    "кузнецова е": "Кузнецова Елена",
    "кузнецова е.": "Кузнецова Елена",
    "морозов д с": "Морозов Дмитрий",
    "морозов д.с.": "Морозов Дмитрий",
}



# 2. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ

def create_connection(db_path):
    """Создаёт подключение к SQLite-базе данных."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def drop_if_exists(db_path):
    """Удаляет базу данных, если она существует."""
    if os.path.exists(db_path):
        os.remove(db_path)



# 3. ГЕНЕРАЦИЯ БАЗ ДАННЫХ

def init_source_1(conn):
    """Создаёт таблицу projects."""
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS projects (
        fio TEXT NOT NULL,
        role TEXT NOT NULL,
        project_name TEXT NOT NULL,
        stack TEXT NOT NULL
    )
    """)
    data = [
        ("Иванов Иван", "Backend Developer", "ProjectAlpha", "Python, FastAPI, PostgreSQL"),
        ("Петрова Мария", "Frontend Developer", "ProjectBeta", "React, TypeScript, Tailwind"),
        ("Сидоров Алексей", "DevOps Engineer", "ProjectGamma", "Docker, Kubernetes, AWS"),
        ("Кузнецова Елена", "Data Scientist", "ProjectDelta", "Python, Pandas, Scikit-learn"),
        ("Морозов Дмитрий", "QA Lead", "ProjectEpsilon", "Selenium, Pytest, JIRA"),
    ]
    cur.executemany(
        "INSERT INTO projects (fio, role, project_name, stack) VALUES (?, ?, ?, ?)",
        data,
    )
    conn.commit()
    print(f"[source_1] Вставлено {len(data)} записей в projects.")


def init_source_2(conn):
    """Создаёт таблицу hr_info."""
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS hr_info (
        fio TEXT NOT NULL,
        skills TEXT NOT NULL,
        salary REAL NOT NULL,
        bonus REAL NOT NULL
    )
    """)
    data = [
        ("иван иванов", "Python, SQL", 150000, 25000),
        ("Петрова М.", "JavaScript, CSS", 130000, 15000),
        ("СИДОРОВ А.В.", "Docker, Terraform", 170000, 30000),
        ("Кузнецова Е.", "Python, ML, NLP", 160000, 20000),
        ("Морозов Д.С.", "Selenium, Java", 140000, 18000),
    ]
    cur.executemany(
        "INSERT INTO hr_info (fio, skills, salary, bonus) VALUES (?, ?, ?, ?)",
        data,
    )
    conn.commit()
    print(f"[source_2] Вставлено {len(data)} записей в hr_info.")



# 4. НОРМАЛИЗАЦИЯ ФИО (улучшенная версия)

def remove_extra_chars(text):
    """
    Удаляет лишние символы: точки, запятые, лишние пробелы.
    Решает проблему: "СИДОРОВ А.В." -> "сидоров а в"
    """
    cleaned = text.lower()
    cleaned = cleaned.replace('.', ' ')
    cleaned = cleaned.replace(',', ' ')
    cleaned = cleaned.replace('-', ' ')
    cleaned = ' '.join(cleaned.split())  # убираем лишние пробелы
    return cleaned.strip()


def get_canonical_fio(raw_fio):
    """
    Получает каноническое ФИО по словарю исправлений.
    Если не найдено — возвращает исходное значение.
    """
    cleaned = remove_extra_chars(raw_fio)
    return FIO_CORRECTIONS.get(cleaned, raw_fio)


def normalize_fio(raw_fio):
    """
    Нормализует ФИО для поиска.
    Алгоритм:
    1. Проверяем словарь исправлений
    2. Удаляем лишние символы
    3. Приводим к нижнему регистру
    4. Находим фамилию (самое длинное слово)
    5. Находим первую букву имени
    6. Формируем ключ: "фамилия_первая_буква_имени"

    Решает проблемы:
    - Перестановка слов: "Иван Иванов" = "Иванов Иван"
    - Разный регистр: "ИВАНОВ" = "иванов"
    - Неполное имя: "Петрова М." = "Петрова Мария"
    - Лишние символы: "СИДОРОВ А.В." = "сидоров а в"
    """
    # Сначала проверяем словарь исправлений
    canonical = get_canonical_fio(raw_fio)

    # Нормализуем
    cleaned = remove_extra_chars(canonical)
    tokens = cleaned.split()

    if not tokens:
        return ""

    # Находим фамилию (самое длинное слово)
    surname = max(tokens, key=len)

    # Находим все остальные слова
    others = [t for t in tokens if t != surname]

    # Берем первую букву имени
    first_initial = others[0][0] if others else ""

    return f"{surname}_{first_initial}"


def find_similar_fio(raw_fio, candidates):
    """
    Простой алгоритм поиска по похожим именам.
    Сравнивает первые 3 буквы фамилии и первую букву имени.
    """
    cleaned = remove_extra_chars(raw_fio)
    tokens = cleaned.split()
    if not tokens:
        return None

    surname = max(tokens, key=len)
    others = [t for t in tokens if t != surname]
    first_initial = others[0][0] if others else ""

    # Ищем совпадение по первым 3 буквам фамилии и первой букве имени
    for candidate in candidates:
        cand_cleaned = remove_extra_chars(candidate)
        cand_tokens = cand_cleaned.split()
        if not cand_tokens:
            continue

        cand_surname = max(cand_tokens, key=len)
        cand_others = [t for t in cand_tokens if t != cand_surname]
        cand_initial = cand_others[0][0] if cand_others else ""

        # Сравниваем первые 3 буквы фамилии и первую букву имени
        if surname[:3] == cand_surname[:3] and first_initial == cand_initial:
            return candidate

    return None



# 5. ETL (Extract → Transform → Load)

def extract_source_1(conn):
    """Извлекает данные из source_1."""
    cur = conn.cursor()
    cur.execute("SELECT fio, role, project_name, stack FROM projects")
    rows = cur.fetchall()
    return [(row["fio"], row["role"], row["project_name"], row["stack"]) for row in rows]


def extract_source_2(conn):
    """Извлекает данные из source_2."""
    cur = conn.cursor()
    cur.execute("SELECT fio, skills, salary, bonus FROM hr_info")
    rows = cur.fetchall()
    return [(row["fio"], row["skills"], row["salary"], row["bonus"]) for row in rows]


def transform_normalize(records, normalize_index=0):
    """Нормализует ФИО и добавляет ключ."""
    result = []
    for record in records:
        fio_value = str(record[normalize_index])
        key = normalize_fio(fio_value)
        result.append((key, *record))
    return result


def load_warehouse(warehouse_conn, norm_p1, norm_p2):
    """
    Сопоставляет источники по ключу.
    Использует хэш-таблицу для O(1) поиска.
    Если точное совпадение не найдено — пытается найти похожее.
    """
    cur = warehouse_conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS employees (
        fio TEXT NOT NULL, role TEXT, project_name TEXT, stack TEXT,
        skills TEXT, salary REAL NOT NULL, bonus REAL NOT NULL,
        total_compensation REAL NOT NULL
    )
    """)

    # Хэш-таблица для быстрого поиска (O(1))
    hr_map = {entry[0]: entry for entry in norm_p2}

    # Список всех ФИО из source_2 для поиска по похожести
    all_hr_fios = [entry[1] for entry in norm_p2]

    inserted = 0
    not_found = []

    for entry in norm_p1:
        key, raw_fio, role, project_name, stack = entry

        # 1. Точный поиск по нормализованному ключу
        if key in hr_map:
            hr_entry = hr_map[key]
            skills, salary, bonus = hr_entry[2], hr_entry[3], hr_entry[4]

            cur.execute("""
                INSERT INTO employees
                (fio, role, project_name, stack, skills, salary, bonus, total_compensation)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (raw_fio, role, project_name, stack, skills, salary, bonus, round(salary + bonus, 2)))
            inserted += 1
        else:
            # 2. Поиск по похожим именам (алгоритм близости)
            similar = find_similar_fio(raw_fio, all_hr_fios)
            if similar:
                # Находим запись по похожему ФИО
                for hr_entry in norm_p2:
                    if hr_entry[1] == similar:
                        skills, salary, bonus = hr_entry[2], hr_entry[3], hr_entry[4]
                        cur.execute("""
                            INSERT INTO employees
                            (fio, role, project_name, stack, skills, salary, bonus, total_compensation)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (raw_fio, role, project_name, stack, skills, salary, bonus, round(salary + bonus, 2)))
                        inserted += 1
                        print(f"  [похожее] {raw_fio} -> {similar}")
                        break
            else:
                not_found.append(raw_fio)

    warehouse_conn.commit()
    print(f"[warehouse] Вставлено записей: {inserted}")
    if not_found:
        print(f"[warehouse] Не найдены совпадения для: {not_found}")



# 6. ВЫВОД РЕЗУЛЬТАТА

def display_warehouse(conn):
    """Выводит содержимое таблицы employees."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM employees ORDER BY fio")
    rows = cur.fetchall()

    if not rows:
        print("\n[warehouse] Таблица пуста.")
        return

    headers = ["ФИО", "Роль", "Проект", "Стек", "Навыки", "Зарплата", "Бонус", "Всего"]
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

    sep = "+-" + "-+-".join("-" * w for w in col_widths) + "-+"
    print("\n" + sep)
    print("| " + " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + " |")
    print(sep)
    for dr in data_rows:
        print("| " + " | ".join(val.ljust(col_widths[i]) for i, val in enumerate(dr)) + " |")
    print(sep)
    print(f"\nВсего записей: {len(rows)}\n")



# 7. ОСНОВНАЯ ФУНКЦИЯ

def main():
    """Точка входа: полный ETL-процесс."""
    print("=" * 60)
    print("ETL-процесс с улучшенной нормализацией ФИО")
    print("=" * 60)

    # Очистка старых данных
    drop_if_exists("source_1.db")
    drop_if_exists("source_2.db")
    drop_if_exists("warehouse.db")

    # Создание источников
    print("\n1. Генерация источников данных...")
    conn1 = create_connection("source_1.db")
    init_source_1(conn1)
    conn2 = create_connection("source_2.db")
    init_source_2(conn2)

    # Извлечение
    print("\n2. Извлечение данных (Extract)...")
    raw_p1 = extract_source_1(conn1)
    raw_p2 = extract_source_2(conn2)

    # Трансформация
    print("\n3. Трансформация (нормализация ФИО)...")
    norm_p1 = transform_normalize(raw_p1, normalize_index=0)
    norm_p2 = transform_normalize(raw_p2, normalize_index=0)

    print("\nРезультат нормализации:")
    for entry in norm_p1:
        print(f"  Source 1: '{entry[1]}' -> ключ={entry[0]}")
    for entry in norm_p2:
        print(f"  Source 2: '{entry[1]}' -> ключ={entry[0]}")

    # Загрузка
    print("\n4. Загрузка в warehouse (с поиском по похожести)...")
    conn_wh = create_connection("warehouse.db")
    load_warehouse(conn_wh, norm_p1, norm_p2)

    # Итог
    print("\n5. Итоговый отчёт:")
    display_warehouse(conn_wh)

    # Закрытие
    conn1.close()
    conn2.close()
    conn_wh.close()
    print("=" * 60)
    print("Готово!")
    print("=" * 60)


if __name__ == "__main__":
    main()