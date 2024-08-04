import pandas as pd
import os

#extract data
# Directory containing the raw CSV files
raw_data_dir = 'path_to_raw_data'

# Read all CSV files into a list of dataframes
csv_files = [os.path.join(raw_data_dir, file) for file in os.listdir(raw_data_dir) if file.endswith('.csv')]
dataframes = [pd.read_csv(csv_file) for csv_file in csv_files]

#Transform Data
# Concatenate all dataframes into a single dataframe
data = pd.concat(dataframes, ignore_index=True)

# Data cleaning and transformation
data['attendance_pct'] = data['attendance'] / data['total_classes'] * 100

# Normalizing data to fit into schema
dim_semester = data[['semester_id']].drop_duplicates().reset_index(drop=True)
dim_week = data[['week_id', 'semester_id']].drop_duplicates().reset_index(drop=True)
dim_course = data[['course_name']].drop_duplicates().reset_index(drop=True)
fact_attendance = data[['semester_id', 'week_id', 'course_name', 'attendance_pct']]

#Load Data
import sqlite3

# Connect to SQLite database
conn = sqlite3.connect('data_warehouse.db')

# Load data into staging table
data.to_sql('staging_attendance', conn, if_exists='replace', index=False)

# Load data into dimension and fact tables
dim_semester.to_sql('dim_semester', conn, if_exists='replace', index=False)
dim_week.to_sql('dim_week', conn, if_exists='replace', index=False)
dim_course.to_sql('dim_course', conn, if_exists='replace', index=False)
fact_attendance.to_sql('fact_attendance', conn, if_exists='replace', index=False)

