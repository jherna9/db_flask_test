from flask import Flask, request, jsonify
import sqlite3
import csv
import pandas as pd
from io import TextIOWrapper

app = Flask(__name__)

# departments
def clean_departments():
    data= pd.read_csv("data/departments.csv",header=None,skiprows=0,names=['id','departments'])
    data1 = data[['departments']]
    data1.to_csv('data/departments_out.csv',index=False)

def create_table_departments():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS departments
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, department STRING)''')
    conn.commit()
    conn.close()

# Function to insert CSV data into the database
def insert_departments(content):
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    for row in content:
        c.execute('INSERT INTO departments (department) VALUES (?)', row)
    conn.commit()
    conn.close()

@app.route('/upload_departments',endpoint='func1', methods=['POST'])
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
        insert_departments(rows)
        return jsonify({'message': 'File uploaded successfully'})

#hired_employees
def clean_departments():
    data= pd.read_csv("data/hired_employees.csv",header=None,skiprows=0,names=['id','name','datetime','department_id','job_id'])
    data2 = data[['name','datetime','department_id','job_id']]
    data2.to_csv('data/hired_employees.csv',index=False)

def create_hired_employees():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS hired_employees
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, name STRING, datetime STRING, department_id INTEGER, job_id INTEGER)''')
    conn.commit()
    conn.close()

# Function to insert CSV data into the database
def insert_hired_employees(content):
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    for row in content:
        c.executemany('INSERT INTO hired_employees (STRING,STRING,STRING,INTEGER,INTEGER) VALUES (?,?,?,?,?)', row)
    conn.commit()
    conn.close()

@app.route('/upload_hired_employees',endpoint='func2', methods=['POST'])
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
        insert_hired_employees(rows)
        return jsonify({'message': 'File uploaded successfully'})

if __name__ == '__main__':
    clean_departments()
    create_table_departments()
    create_hired_employees()
    app.run(debug=True) # You can set debug=False for production