#!/bin/bash

virtualenv -p python3.7 venv

venv/bin/pip install --upgrade pymysql
venv/bin/pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
