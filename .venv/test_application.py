from application import create_table_departments,create_table_hired_employees,create_table_jobs
import sqlite3

def test_create_table_departments():
    # Check if the table exists
    conn = sqlite3.connect('data/data.db')
    c = conn.cursor()
    create_table_departments()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='departments'")
    result = c.fetchone()
    conn.close()
    assert result is None

def test_create_hired_employees():
    # Check if the table exists
    conn = sqlite3.connect('data/data.db')
    c = conn.cursor()
    create_table_hired_employees()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='hired_employees'")
    result = c.fetchone()
    conn.close()
    assert result is None

def test_create_table_jobs():
    # Check if the table exists
    conn = sqlite3.connect('data/data.db')
    c = conn.cursor()
    create_table_jobs()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='jobs'")
    result = c.fetchone()
    conn.close()
    assert result is None

if __name__ == '__main__':
    test_create_table_departments()
    test_create_hired_employees()
    test_create_table_jobs()
