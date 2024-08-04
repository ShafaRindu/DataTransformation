import pandas as pd
import os
import sqlite3

def extract():
    # Directory containing the raw CSV files
    raw_data_dir = 'raw_data'
    # Read all CSV files into a list of dataframes
    csv_files = [os.path.join(raw_data_dir, file) for file in os.listdir(raw_data_dir) if file.endswith('.csv')]
    dataframes = [pd.read_csv(csv_file) for csv_file in csv_files]
    return pd.concat(dataframes, ignore_index=True)

def transform(data):
    # Assuming columns 'attendance' and 'total_classes' exist for calculating attendance percentage
    data['attendance_pct'] = data['attendance'] / data['total_classes'] * 100
    return data[['semester_id', 'week_id', 'course_name', 'attendance_pct']]

def load(data):
    conn = sqlite3.connect('data_warehouse.db')
    data.to_sql('staging_attendance', conn, if_exists='replace', index=False)
    conn.execute('''CREATE TABLE IF NOT EXISTS dim_semester (semester_id INTEGER PRIMARY KEY)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS dim_week (week_id INTEGER, semester_id INTEGER, PRIMARY KEY (week_id, semester_id))''')
    conn.execute('''CREATE TABLE IF NOT EXISTS dim_course (course_name TEXT PRIMARY KEY)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS fact_attendance (
                    semester_id INTEGER,
                    week_id INTEGER,
                    course_name TEXT,
                    attendance_pct FLOAT,
                    PRIMARY KEY (semester_id, week_id, course_name),
                    FOREIGN KEY (semester_id) REFERENCES dim_semester(semester_id),
                    FOREIGN KEY (week_id, semester_id) REFERENCES dim_week(week_id, semester_id),
                    FOREIGN KEY (course_name) REFERENCES dim_course(course_name))''')
    data[['semester_id']].drop_duplicates().to_sql('dim_semester', conn, if_exists='replace', index=False)
    data[['week_id', 'semester_id']].drop_duplicates().to_sql('dim_week', conn, if_exists='replace', index=False)
    data[['course_name']].drop_duplicates().to_sql('dim_course', conn, if_exists='replace', index=False)
    data.to_sql('fact_attendance', conn, if_exists='replace', index=False)
    conn.close()

if __name__ == '__main__':
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
