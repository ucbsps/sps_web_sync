from datetime import datetime, timedelta
from re import findall

import pymysql
from googleapiclient.discovery import build, HttpError

from sps_secrets import MARIADB_USER, MARIADB_PASSWORD, MARIADB_DB, MARIADB_HOST, SCOPES
from db_util import load_set_id
from gd_util import get_creds

creds = get_creds('token.pickle')

service = build('calendar', 'v3', credentials=creds)

try:
    db_conn = pymysql.connect(user=MARIADB_USER, password=MARIADB_PASSWORD, database=MARIADB_DB,
                              host=MARIADB_HOST, port=3306, autocommit=True)
except pymysql.Error as e:
    print('Error connecting to database: {}'.format(e))

    exit()

cur = db_conn.cursor()

try:
    cur.execute('SELECT updated FROM web2020_events ORDER BY updated DESC LIMIT 1')
except pymysql.Error as e:
    print('Error selecting from database: {}'.format(e))

    exit()

last_updates = cur.fetchall()

try:
    last_update = last_updates[0][0].isoformat() + 'Z'
except:
    last_update = None

time_min = (datetime.utcnow() - timedelta(days=100)).isoformat() + 'Z'
time_max = (datetime.utcnow() + timedelta(days=100)).isoformat() + 'Z'

try:
    events_result = service.events().list(calendarId='ucbsps@gmail.com', updatedMin=last_update,
                                          timeMin=time_min, timeMax=time_max,
                                          maxResults=2500, singleEvents=True).execute()
except HttpError:
    print('Updating all events')
    events_result = service.events().list(calendarId='ucbsps@gmail.com',
                                          maxResults=2500, singleEvents=True).execute()

events = events_result.get('items', [])

if not events:
    print('No upcoming events found.')

    exit()

for event in events:
    google_cal_id = event['id']
    try:
        cur.execute('SELECT id FROM web2020_events WHERE google_cal_id=%s', (google_cal_id,))
    except pymysql.Error as e:
        print('DB Error: {}'.format(e))

        continue

    ids = cur.fetchall()
    if len(ids) == 0:
        id = None
    else:
        id = ids[0][0]

    try:
        event_start_time = datetime.fromisoformat(event['start']['dateTime'])
        event_end_time = datetime.fromisoformat(event['end']['dateTime'])
    except KeyError: # Sometimes there is no time

        if 'start' not in event:
            # A calendar event with no times? Skip it
            continue

        event_start_time = datetime.fromisoformat(event['start']['date'])
        event_end_time = datetime.fromisoformat(event['end']['date'])

    try:
        location = event['location']
    except:
        location = None

    try:
        title = event['summary']
    except:
        title = ''

    try:
        description = event['description']
    except:
        description = ''

    tags = findall('#[A-Za-z][A-Za-z0-9\-\.\_]*', description)

    print('Found event {}'.format(title))

    try:
        cur.execute('INSERT INTO web2020_events' +
                    ' (id, title, description, start_time, end_time, location, google_cal_id)' +
                    ' VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE' +
                    ' title=VALUES(title), description=VALUES(description),' +
                    ' start_time=VALUES(start_time), end_time=VALUES(end_time),' +
                    ' location=VALUES(location)',
                    (id, title, description, event_start_time, event_end_time,
                     location, google_cal_id,))
    except pymysql.Error as e:
        print('DB Error: {}'.format(e))

        continue

    if id == None:
        try:
            cur.execute('SELECT id FROM web2020_events WHERE google_cal_id=%s', (google_cal_id,))
        except pymysql.Error as e:
            print('DB Error: {}'.format(e))

            continue

        ids = cur.fetchall()
        if len(ids) == 0:
            continue
        else:
            id = ids[0][0]


    cur.execute('DELETE FROM web2020_events_tags where event_id=%s', (id,))

    for tag in tags:
        tag_id = load_set_id(cur, 'web2020_tags', 'tag', tag)

        if not tag_id == None:
            try:
                cur.execute('INSERT INTO web2020_events_tags (event_id, tag_id) VALUES (%s, %s)',
                            (id, tag_id,))
            except pymysql.Error as e:
                print('DB Error: {}'.format(e))

                continue

    # Remove events that SPS officers are invited to
    # They should really not be on this calendar at all, since it is our public events calendar, but yeah
    cur.execute('DELETE FROM web2020_events where description like "%Kathleen Cooney%"')
