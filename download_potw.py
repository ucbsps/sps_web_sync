from datetime import date, datetime, timedelta
from io import FileIO
from os import environ
from urllib.parse import urlparse, parse_qs

import pymysql
from apiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build

from sps_secrets import MARIADB_USER, MARIADB_PASSWORD, MARIADB_DB, MARIADB_HOST
from db_util import load_set_id
from gd_util import get_creds

PROBLEM_SPREADSHEET_ID = '15BHEtsYFtnzQ38YtF0zi7amT76kcBmG7PD14NXkEWyY'
PROBLEM_RANGE_NAME = 'Form Responses 1!A1:G200'
SCOREBOARD_SPREADSHEET_ID = '1U9ESoCQAkihbgGNWkPrid5LhgJL04jXTf8zv93St8Wk'
SCOREBOARD_RANGE_NAME = 'Sheet1!A1:G200'

WEB_DIR = environ['HOME'] + '/public_html/'

def get_url_id_param(url):
    params = parse_qs(urlparse(url).query)
    if 'id' in params:
        ids = params['id']
        if len(ids) > 0:
            return ids[0]
    elif 'usp' in params:
        return url.split('/').pop(-2)
    return None

def download_gd_file(drive_service, file_id, filename_base):

    metadata = drive_service.files().get(fileId=file_id).execute()
    original_filename = metadata['name']
    request = drive_service.files().get_media(fileId=file_id)

    filename = filename_base + '.' + original_filename.split('.')[-1]
    file_handle = FileIO(WEB_DIR + filename, 'wb')

    downloader = MediaIoBaseDownload(file_handle, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    return filename

creds = get_creds('token.pickle')

sheets_service = build('sheets', 'v4', credentials=creds)
drive_service = build('drive', 'v3', credentials=creds)

try:
    db_conn = pymysql.connect(user=MARIADB_USER, password=MARIADB_PASSWORD, database=MARIADB_DB,
                              host=MARIADB_HOST, port=3306, autocommit=True)
except pymysql.Error as e:
    print('Error connecting to database: {}'.format(e))

    exit()

cur = db_conn.cursor()

sheet = sheets_service.spreadsheets()
result = sheet.values().get(spreadsheetId=PROBLEM_SPREADSHEET_ID, range=PROBLEM_RANGE_NAME).execute()
values = result.get('values', [])

if not values:
    print('No data found.')
else:
    with db_conn.cursor() as cur:
        for row in values:
            if row[0] == 'Timestamp':
                # header row
                continue

            start_date = datetime.strptime(row[1], '%m/%d/%Y').date()
            end_date = datetime.strptime(row[2], '%m/%d/%Y').date()

            problem = None
            if len(row) > 3:
                problem = row[3]
            problem_file_id = None
            if len(row) > 4:
                if row[4] != '':
                    problem_file_id = get_url_id_param(row[4])

            solution = None
            if len(row) > 5:
                solution = row[5]
            solution_file_id = None
            if len(row) > 6:
                if row[6] != '':
                    solution_file_id = get_url_id_param(row[6])

            if problem_file_id != None:
                problem_filename = '/static/potw_' + start_date.isoformat() + '_problem'
                problem_filename = download_gd_file(drive_service, problem_file_id, problem_filename)
            else:
                problem_filename = None

            if solution_file_id != None:
                solution_filename = '/static/potw_' + start_date.isoformat() + '_solution'
                solution_filename = download_gd_file(drive_service, solution_file_id, solution_filename)
            else:
                solution_filename = None

            try:
                cur.execute('INSERT INTO web2020_potw' +
                            ' (start_date, end_date, problem, linked_problem,' +
                            ' solution, linked_solution)' +
                            ' VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE' +
                            ' end_date=VALUES(end_date), problem=VALUES(problem),' +
                            ' linked_problem=VALUES(linked_problem), solution=VALUES(solution),' +
                            ' linked_solution=VALUES(linked_solution)',
                            (start_date, end_date, problem, problem_filename,
                             solution, solution_filename,))
            except pymysql.Error as e:
                print('DB Error: {}'.format(e))

    db_conn.commit()

### Download scoreboard

sheet = sheets_service.spreadsheets()
result = sheet.values().get(spreadsheetId=SCOREBOARD_SPREADSHEET_ID,
                            range=SCOREBOARD_RANGE_NAME).execute()
values = result.get('values', [])

if not values:
    print('No data found.')
else:
    with db_conn.cursor() as cur:
        try:
            cur.execute('DELETE FROM web2020_potw_scoreboard')
        except pymysql.Error as e:
            print('DB Error: {}'.format(e))

        for row in values:
            if len(row) < 3:
                # empty row
                continue

            if row[0] == 'Email':
                # header row
                continue

            email = row[0]
            name = row[1]
            solved = row[2]

            if email is None or len(email) == 0:
                continue
            if name is None or len(name) == 0:
                continue
            if solved is None or len(solved) == 0:
                continue

            try:
                cur.execute('INSERT INTO web2020_potw_scoreboard' +
                        ' (email, name, solved)' +
                        ' VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE' +
                        ' email=VALUES(email), name=VALUES(name),' +
                        ' solved=VALUES(solved)',
                        (email, name, solved))
            except pymysql.Error as e:
                print('DB Error: {}'.format(e))
    db_conn.commit()

### Cleanup database connection

cur.close()
db_conn.close()
