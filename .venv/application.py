from flask import Flask, request, jsonify
import sqlite3
import csv
import pandas as pd
from io import TextIOWrapper

app = Flask(__name__)

# departments
def create_table_departments():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS departments
                 (id INT NOT NULL PRIMARY KEY, department STRING)''')
    conn.commit()
    conn.close()

# Function to insert CSV data into the database
def insert_departments(content):
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    for row in content:
        c.execute('INSERT INTO departments (id,department) VALUES (?,?)', row)
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
    



# hired_employees
def create_table_hired_employees():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS hired_employees
                 (id INT NOT NULL PRIMARY KEY, name STRING, datetime STRING, department_id INTEGER, job_id INTEGER)''')
    conn.commit()
    conn.close()

# Function to insert CSV data into the database
def insert_hired_employees(content):
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    for row in content:
        c.execute('INSERT INTO hired_employees (id,name,datetime,department_id,job_id) VALUES (?,?,?,?,?)', row)
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
    



# jobs
def create_table_jobs():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS jobs
                 (id INT NOT NULL PRIMARY KEY, job STRING)''')
    conn.commit()
    conn.close()

# Function to insert CSV data into the database
def insert_departments(content):
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    for row in content:
        c.execute('INSERT INTO jobs (id,job) VALUES (?,?)', row)
    conn.commit()
    conn.close()

@app.route('/upload_job',endpoint='func3', methods=['POST'])
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
    

#query 1
def execute_query(query):
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute(query)
    data = c.fetchall()
    conn.close()
    return data   

@app.route('/get_data1', methods=['GET'])
def get_data():
    query = """
        SELECT
            department,
            job,
            SUM(CASE WHEN hire_date BETWEEN '2021-01-01' AND '2021-03-31' THEN 1 ELSE 0 END) AS Q1,
            SUM(CASE WHEN hire_date BETWEEN '2021-04-01' AND '2021-06-30' THEN 1 ELSE 0 END) AS Q2,
            SUM(CASE WHEN hire_date BETWEEN '2021-07-01' AND '2021-09-30' THEN 1 ELSE 0 END) AS Q3,
            SUM(CASE WHEN hire_date BETWEEN '2021-10-01' AND '2021-12-31' THEN 1 ELSE 0 END) AS Q4
        FROM
            employees
        WHERE
            hire_date BETWEEN '2021-01-01' AND '2021-12-31'
        GROUP BY
            department, job
        ORDER BY
            department, job
    """
    result = execute_query(query)
    response = {
        'data': result
    }
    return jsonify(response)





#query 2
def execute_query(query):
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute(query)
    data = c.fetchall()
    conn.close()
    return data   

@app.route('/get_data2', methods=['GET'])
def get_data():
    query = """
            SELECT 
                d.id AS department_id,
                d.department AS department_name,
                COUNT(employees.id) AS num_employees_hired
            FROM 
                departments d
            JOIN 
                hired_employees employees ON d.id = employees.department_id
            WHERE 
                strftime('%Y', employees.datetime) = '2021'
            GROUP BY 
                d.id, d.department
            HAVING 
                COUNT(employees.id) > (
                    SELECT 
                        AVG(num_employees)
                    FROM 
                        (
                            SELECT 
                                COUNT(id) AS num_employees
                            FROM 
                                hired_employees
                            WHERE 
                                strftime('%Y', datetime) = '2021'
                            GROUP BY 
                                department_id
                        ) AS avg_employees
                )
        ORDER BY 
            num_employees_hired DESC;
    """
    result = execute_query(query)
    response = {
        'data': result
    }
    return jsonify(response)

if __name__ == '__main__':
    create_table_departments()
    create_table_hired_employees()
    create_table_jobs()
    app.run(debug=True) # You can set debug=False for production