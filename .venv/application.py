from flask import Flask, request, jsonify
import sqlite3
import csv
from io import TextIOWrapper

app = Flask(__name__)

# Function to create SQLite database table if not exists
def create_table():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS csv_data
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT,age INTEGER, city TEXT)''')
    conn.commit()
    conn.close()

# Function to insert CSV data into the database
def insert_csv_data(content):
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    for row in content:
        c.execute('INSERT INTO csv_data (name, age, city) VALUES (?, ?, ?)', row)
    conn.commit()
    conn.close()

@app.route('/upload', methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'})

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'})

    if file:
        # Read the CSV file and insert each row into the database
        csv_file = TextIOWrapper(file.stream, encoding='utf-8')
        csv_reader = csv.reader(csv_file)
        rows = list(csv_reader)
        insert_csv_data(rows)
        return jsonify({'message': 'File uploaded successfully'})

if __name__ == '__main__':
    create_table()
    app.run(debug=True) # You can set debug=False for production