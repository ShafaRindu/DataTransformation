# DataTransformation

To run the ETL process, follow these steps:

1. **Set Up Your Environment:**
   - Make sure you have Python installed.
   - Install the necessary Python libraries. You'll need `pandas` and `sqlite3`.

2. **Download the CSV Files:**
   - Place the CSV files from the provided Google Drive link into a directory named `raw_data`.

3.  **Run the Script:**
   - Open a terminal or command prompt.
   - Navigate to the directory where your `etl_pipeline.py` script is located.
   - Run the script using Python:


### Explanation of the Script:

1. **Extract:**
   - The `extract` function reads all CSV files from the `raw_data` directory and combines them into a single DataFrame.

2. **Transform:**
   - The `transform` function calculates the attendance percentage and selects the required columns.

3. **Load:**
   - The `load` function creates the necessary tables in a SQLite database and populates them with the transformed data.

