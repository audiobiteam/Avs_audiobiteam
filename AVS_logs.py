import os
import pyodbc
import re
import sys
from start import ADSPLog

def create_combined_table(cursor):
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'AVS_logs')
        BEGIN
            CREATE TABLE AVS_logs (
                TestName NVARCHAR(255),
                SplitName NVARCHAR(255),
                TestResult NVARCHAR(50),
                Start_Time NVARCHAR(50),
                End_Time NVARCHAR(50),
                Duration NVARCHAR(50),
                ErrorString NVARCHAR(MAX),
                BuildID NVARCHAR(50),
                Logfile NVARCHAR(255),
                TC_Name NVARCHAR(510),
                SuiteName NVARCHAR(255),
                DataType NVARCHAR(50)
            )
        END
    """)
def extract_start_test_info(file_path, build_id):
    test_info = []
    test_names = set()  # To keep track of unique test names
    duplicate_test_names = set()  # To keep track of duplicate test names
    try:
        log = ADSPLog(file_path)
        log.BuildTestInfos()

        startTime = log.startTime
        endTime = log.endTime

        for split_obj in log.splits.values():
            Split_Name = split_obj.splitName
            for test_obj in split_obj.tests:
                test_name = test_obj.name
                test_result = test_obj.status
                error_string = test_obj.errorString if hasattr(test_obj, 'errorString') else None
                duration = test_obj.duration if hasattr(test_obj, 'duration') else None
                if test_name in test_names:
                    duplicate_test_names.add(test_name)
                else:
                    test_names.add(test_name)
                tc_name = test_name + '_' + Split_Name + ' ' + os.path.splitext(os.path.basename(file_path))[0] if test_name in duplicate_test_names else test_name + '_' + os.path.splitext(os.path.basename(file_path))[0]
                test_info.append((test_name, Split_Name, test_result, startTime, endTime, duration, error_string,build_id, os.path.splitext(os.path.basename(file_path))[0], tc_name, None, 'Start'))
    except Exception as e:
        print("Error processing file:", file_path)
        print("Error:", str(e))
    return test_info

def extract_cases_info(Txt_path):
    data = set()
    for tst_root, tst_dirs, _ in os.walk(Txt_path):
        for subfolder in tst_dirs:
            list_folder = os.path.join(tst_root, subfolder, "list")
            if os.path.exists(list_folder):
                print("Found 'list' folder in", list_folder)
                for list_root, _, list_files in os.walk(list_folder):
                    for filename in list_files:
                        if filename.endswith('_test_list.txt'):  # Check if any file ends with '_test_list.txt'
                            log_name1 = filename.replace('_test_list.txt', '_tests')  # Extract log_name from filename
                            break  # Stop searching once a matching file is found
                    for filename in list_files:
                        suite_name = os.path.splitext(filename)[0]  # Extracting suite name from file name
                        suite_path = os.path.join(list_root, filename)
                        print("Suite path:", suite_path)
                        with open(suite_path, 'r') as suite_file:
                            case_names = set()
                            for line in suite_file:
                                match_test_case = re.match(r'^\s*NEW_TEST_SET\s*(\w*)\s', line)
                                if match_test_case:
                                    case_name = match_test_case.group(1)
                                    case_names.add(case_name)
                            for case_name in case_names:
                                combined_name = case_name + '_' + log_name1  # Concatenate caseName with LogName
                                cursor.execute("INSERT INTO AVS_logs (TestName, SplitName, TestResult, Start_Time, End_Time, Duration, ErrorString, BuildID, Logfile, TC_Name, SuiteName, DataType) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (None, None, None, None, None, None, None, None, None, combined_name, suite_name, 'Cases'))
                                # Check if there's a match in Start table and update SuiteName if found
                                cursor.execute("UPDATE AVS_logs SET SuiteName = ? WHERE TC_Name = ? AND DataType = ?", (suite_name, combined_name, 'Start'))
        # Set SuiteName to "Uncategorized" where it's NULL
        cursor.execute("UPDATE AVS_logs SET SuiteName = 'Uncategorized_'+ Logfile WHERE SuiteName IS NULL")
        # Set SuiteName to "Uncategorized" where it's NULL
        cursor.execute("DELETE FROM AVS_logs WHERE DataType = 'Cases'")


def process_log_files(Log_path):
    test_data = []
    for file_name in os.listdir(Log_path):
        if file_name.endswith('.log'):
            file_path = os.path.join(Log_path, file_name)
            print("Log_path:",file_path)
            build_id = os.path.basename(Log_path)
            test_data.extend(extract_start_test_info(file_path, build_id))
    return test_data

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py Log_path Txt_path")
        sys.exit(1)

    Log_path = sys.argv[1]
    Txt_path = sys.argv[2]

    conn = pyodbc.connect(r'DRIVER={SQL Server};SERVER=KOPPGOVA\EXPRESS2022;DATABASE=AVS;Trusted_Connection=yes;')
    cursor = conn.cursor()
    create_combined_table(cursor)

    start_test_data = process_log_files(Log_path)
    for data in start_test_data:
        cursor.execute("INSERT INTO AVS_logs (TestName, SplitName, TestResult, Start_Time, End_Time, Duration, ErrorString, BuildID, Logfile, TC_Name, SuiteName, DataType) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", data)

    extract_cases_info(Txt_path)

    conn.commit()
    conn.close()
