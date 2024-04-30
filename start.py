import os
import re
import pyodbc
import ntpath
import sys
from datetime import datetime


class TestInfo:
    def __init__(self, Name, SplitName):
        self.splitName = SplitName
        self.status = 'unknown'
        self.duration = 0
        self.errorString = ''
        self.name = Name
        self.nameDeduped = False
        self.diagStartLine = 0
        self.diagEndLine = 0
        self.profilerErrors = []


class TestSplit:
    def __init__(self, SplitName):
        if str(SplitName)[:13] == 'DUMP_splitTM_':
            SplitName = SplitName[13:]
        self.splitName = SplitName
        self.tests = []
        self.diagStartLine = 0
        self.diagEndLine = 0

    def AddTest(self, TestName):
        for t in self.tests:
            if t.name == TestName:
                return None
        ti = TestInfo(TestName, self.splitName)
        self.tests.append(ti)
        return ti

    def GetTestByName(self, Name):
        for t in self.tests:
            if t.name == Name:
                return t
        ti = self.AddTest(Name)
        assert (len(self.tests) <= 3)
        return ti

    def MarkAllTests(self, status, error_message):
        for t in self.tests:
            t.status = status
            t.errorString = error_message

    def Dump(self, stringIO):
        stringIO.write(self.splitName + '\n')
        for t in self.tests:
            stringIO.write("  " + t.name + '\n')


class ADSPLog:
    def __init__(self, FilePath):
        if not os.path.isfile(FilePath):
            msg = "Input file [" + FilePath + "] doesn't exist"
            raise Exception(msg)

        self.filePath = FilePath
        self.splits = {}
        self.startTime = None
        self.endTime = None

    def ParseTime(self, line):
        # Extract start time
        start_match = re.match(r'Test split start time: (\w{3}) (\w{3}) (\d+) (\d+:\d+:\d+) PDT (\d{4})', line)
        if start_match:
            self.startTime = f"{start_match.group(1)} {start_match.group(2)} {start_match.group(3)} {start_match.group(5)} {start_match.group(4)}"

        # Extract end time
        end_match = re.match(r'Test split end time: (\w{3}) (\w{3}) (\d+) (\d+:\d+:\d+) PDT (\d{4})', line)
        if end_match:
            self.endTime = f"{end_match.group(1)} {end_match.group(2)} {end_match.group(3)} {end_match.group(5)} {end_match.group(4)}"

    def ParseSplits(self):
        currentTc = None
        curSplit = None
        duration_test_name = None
        with open(self.filePath, 'r') as f:
            for idx, l in enumerate(f):
                params = re.match(r'Contents of test_run.log in ... ([\w\.]*)', l)
                if params:
                    if currentTc:
                        curSplit.diagEndLine = idx
                        currentTc.diagEndLine = idx
                        currentTc = None

                    curSplitName = params.group(1)
                    curSplit = TestSplit(curSplitName)
                    curSplit.diagStartLine = idx
                    self.splits[curSplit.splitName] = curSplit
                    continue

                params = re.match(r'^DUMP_.+:.+0x[0-9]+\s*$', l)
                if params:
                    if currentTc:
                        curSplit.diagEndLine = idx
                        currentTc.diagEndLine = idx
                        currentTc = None
                    continue

                params = re.match(r'.*TEST LAUNCHER SUCCESS Test (\w*),', l)
                if params:
                    testName = params.group(1)
                    tc = curSplit.AddTest(testName)
                    if tc:
                        tc.diagStartLine = curSplit.diagStartLine
                        currentTc = tc
                    continue

                params = re.match(r'.*ERROR: Profiling test (\w+) FAILED: (.+)', l)
                if params:
                    testName = params.group(1)
                    testObj = curSplit.GetTestByName(testName)
                    testObj.profilerErrors.append(params.group(2))
                    continue

                params = re.match(r'.*TEST SET (\w+),', l)
                if params:
                    duration_test_name = params.group(1)
                    continue

                params = re.match(r'.*TestFwk TimeCard: Duration=(\d*)', l)
                if params and duration_test_name:
                    duration = int(params.group(1))
                    testObj = curSplit.GetTestByName(duration_test_name)
                    testObj.duration = (duration * 1000)
                    duration_test_name = None

                self.ParseTime(l)

    def BuildTestInfos(self):
        if len(self.splits) == 0:
            self.ParseSplits()

        with open(self.filePath, 'r') as f:
            for l in f:
                params = re.match(r'^DUMP_splitTM_([\w\.]*):', l)
                if params:
                    nextLine = next(f)
                    splitName = params.group(1)
                    curSplit = self.splits.get(splitName, None)
                    if curSplit is None:
                        if re.search(r': LSF JOB NOT YET STARTED', l):
                            curSplit = TestSplit(splitName)
                            test_list = nextLine[28:-2].split(' ')
                            for test in test_list:
                                curSplit.AddTest(test)
                            curSplit.MarkAllTests('not_applicable', 'LSF JOB NOT YET STARTED')
                            self.splits[splitName] = curSplit
                        continue

                    if nextLine[:28] != 'Tests in the current split: ':
                        curSplit.MarkAllTests('Blocked', 'Possible hang in main.c after test done...')
                        continue
                    line0 = nextLine[28:]

                    i = line0.find('Tests Passed: ')
                    if i != -1:
                        j = line0.find('Tests Failed: ')
                        k = line0.find('Failed Test Numbers :')
                        if j != -1:
                            testNames = line0[:i].split()
                            for n in testNames:
                                testObj = curSplit.GetTestByName(n)
                                testObj.status = 'Blocked'
                                testObj.errorString = l
                        else:
                            failedTestNames = line0[k + 21:].split()
                            for t in curSplit.tests:
                                if t.name in failedTestNames:
                                    t.status = 'failed'
                                else:
                                    t.status = 'passed'
                    else:
                        testPasses = []
                        testFails = []
                        i = line0.find('Successful Test Numbers: ')
                        j = line0.find('Failed Test Numbers : ')
                        h = line0.find('Tests Failed: ')
                        assert (i != -1)

                        if j == -1 and h == -1:
                            testPasses = line0[i + 24:].split()

                            passfail_num = re.search(r'0x([0-9])', l)
                            split_status = 'passed'
                            split_errorString = ''
                            if passfail_num:
                                if passfail_num.group(1) != '0':
                                    split_errorString = 'Memory Leaks occured in %s test cases within this test split' % passfail_num.group(
                                        1)
                                    split_status = 'failed'

                            for n in testPasses:
                                testObj = curSplit.GetTestByName(n)
                                testObj.errorString = split_errorString
                                testObj.status = split_status

                        elif j != -1 and h == -1:
                            testPasses = line0[i + 24:j].split()
                            testFails = line0[j + 21:].split()
                            for t in curSplit.tests:
                                if t.name in testPasses:
                                    t.status = 'passed'
                                elif t.name in testFails:
                                    t.status = 'failed'
                                else:
                                    t.status = 'Blocked'
                                    t.errorString = l

                        elif j == -1 and h != -1:
                            testNames = line0[:i].split()
                            testPasses = line0[i + 24:h].split()
                            for n in testNames:
                                testObj = curSplit.GetTestByName(n)
                                if n in testPasses:
                                    testObj.status = 'passed'
                                else:
                                    testObj.status = 'Blocked'
                                    testObj.errorString = l

                        else:
                            assert (False)

    def DedupTestNames(self):
        testNames = {}
        for split in self.splits.values():
            for ti in split.tests:
                if ti.name.lower() in testNames:
                    ti.name = ti.name + '-' + ti.splitName
                    ti.nameDeduped = True
                testNames[ti.name.lower()] = 0

    def DumpSplitMap(self, stringIO):
        for key in sorted(self.splits):
            self.splits[key].Dump(stringIO)

    @staticmethod
    def RemoveNonAsciiChars(string_to_clean, subs=' '):
        return re.sub(r'[^\x00-\x7f]', subs, string_to_clean)


def store_data_into_sql_server(log, build_id):
    conn = pyodbc.connect(r'DRIVER={SQL Server};SERVER=KOPPGOVA\EXPRESS2022;DATABASE=AVS;Trusted_Connection=yes;')
    cursor = conn.cursor()

    create_tests_table(cursor)  # Create the Tests table if it doesn't exist

    report = log.filePath + ".report"
    try:
        log.BuildTestInfos()
        log.DedupTestNames()
        AddTestInfos(report, log, cursor, build_id)
    except Exception as e:
        print("ERROR: " + str(e))

    cursor.close()
    conn.close()


def AddTestInfos(report, ADSPLog, cursor, build_id):
    for split_obj in ADSPLog.splits.values():
        test_suite_name = split_obj.splitName
        root_suite_name = ntpath.basename(ADSPLog.filePath)
        for test_obj in split_obj.tests:
            test_name = test_obj.name
            status = test_obj.status
            duration = test_obj.duration
            error_string = test_obj.errorString

            cursor.execute(
                "SELECT COUNT(*) FROM AVS_Tests WHERE TestName = ? AND TestSplit = ? AND Logfile = ? AND BuildID = ?",
                (test_name, test_suite_name, root_suite_name, build_id))
            existing_count = cursor.fetchone()[0]

            if existing_count > 0:
                cursor.execute(
                    "UPDATE AVS_Tests SET TestResult = ?, Duration = ?, ErrorString = ? WHERE TestName = ? AND TestSplit = ? AND Logfile = ? AND BuildID = ?",
                    (status, duration, error_string, test_name, test_suite_name, root_suite_name, build_id))
            else:
                cursor.execute(
                    "INSERT INTO AVS_Tests (TestName, TestSplit, Logfile, TestResult, Duration, ErrorString, StartTime, EndTime, BuildID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (test_name, test_suite_name, root_suite_name, status, duration, error_string, ADSPLog.startTime,
                     ADSPLog.endTime, build_id))

    cursor.commit()


def create_tests_table(cursor):
    cursor.execute("""
        IF OBJECT_ID('dbo.AVS_Tests', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.AVS_Tests (
                TestID INT IDENTITY(1,1) PRIMARY KEY,
                TestName NVARCHAR(255) NOT NULL,
                TestSplit NVARCHAR(255) NOT NULL,
                Logfile NVARCHAR(255) NOT NULL,
                TestResult NVARCHAR(50) NOT NULL,
                Duration INT,
                ErrorString NVARCHAR(MAX),
                StartTime NVARCHAR(50),
                EndTime NVARCHAR(50),
                BuildID INT NOT NULL
            )
        END
    """)
    cursor.commit()

def get_build_id_from_folder_path(folder_path):
    # Extract the build ID from the folder name
    folder_name = os.path.basename(folder_path)
    match = re.search(r'\d+', folder_name)  # Assuming the build ID is the first sequence of digits in the folder name
    if match:
        return int(match.group())
    else:
        return None

def process_log_files(folder_path, build_id):
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.log'):
            file_path = os.path.join(folder_path, file_name)
            print("Processing log file:", file_path)
            main(file_path, build_id)


def main(file_path, build_id):
    try:
        log = ADSPLog(file_path)
        store_data_into_sql_server(log, build_id)
    except Exception as e:
        print("ERROR: " + str(e))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py folder_path")
        sys.exit(1)

    folder_path = sys.argv[1]
    build_id = get_build_id_from_folder_path(folder_path)
    process_log_files(folder_path, build_id)
