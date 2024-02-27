from application import create_table_departments 
import requests
import sqlite3
import os
import pytest


def test_create_table_departments():
    # Check if the table exists
    conn = sqlite3.connect('data\data.db')
    c = conn.cursor()
    create_table_departments()
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='departments'")
    result = c.fetchone()
    conn.close()
    assert result is not None

# Test inserting batch transactions
def test_insert_batch():
    url = 'http://127.0.0.1:5000/insert_batch_departments'
    data = {
        'transactions': [
            {'date': '2024-02-25', 'value': 100},
            {'date': '2024-02-26', 'value': 150}
        ]
    }
    response = requests.post(url, json=data)
    assert response.status_code == 200
    assert response.json()['message'] == 'Batch transactions inserted successfully'

if __name__ == '__main__':
    test_create_table_departments()
    #test_upload_csv()
    #test_insert_batch()