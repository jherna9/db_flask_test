from flask import Flask, request, jsonify
import sqlite3
import csv
from cleaning_csv import clean_departments

app = Flask(__name__)

# Function to create database table if not exists
#departments
def create_table_departments():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS departments
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, department STRING)''')
    conn.commit()
    conn.close()

#hired_employees
def create_table_hired_employees():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS hired_employees
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, name STRING, datetime STRING, department_id INTEGER, job_id INTEGER)''')
    conn.commit()
    conn.close()


#jobs
def create_table_jobs():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS jobs
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, job STRING)''')
    conn.commit()
    conn.close()

#departments
def insert_batch_departments(transactions):
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.executemany('INSERT INTO departments (STRING) VALUES (?)', transactions)
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
        # Save the file
        file.save(file.filename)
        # Read the CSV file and insert data into the database
        with open(file.filename, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # Skip the header row
            transactions = [(row[0], int(row[1])) for row in csv_reader]
            insert_batch_departments(transactions)
        return jsonify({'message': 'File uploaded successfully'})


if __name__ == '__main__':
    clean_departments()
    create_table_departments()
    create_table_hired_employees()
    create_table_jobs()
    app.run(debug=True) 
