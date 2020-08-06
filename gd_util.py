import pickle
import os.path
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request

# Google API Scopes
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly',
          'https://www.googleapis.com/auth/calendar.readonly',
          'https://www.googleapis.com/auth/drive.readonly']

def get_creds(pickle_file, port=2020):

    creds = None

    if os.path.exists(pickle_file):
        with open(pickle_file, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = Flow.from_client_secrets_file('credentials.json', scopes=SCOPES,
                                                 redirect_uri='urn:ietf:wg:oauth:2.0:oob')
            auth_url, _ = flow.authorization_url(prompt='consent')
            print(auth_url)
            code = input()
            flow.fetch_token(code=code)
            print('Fetched token')
            creds = flow.credentials

        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds
