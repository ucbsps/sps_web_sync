#!/bin/bash

virtualenv -p python3 venv

venv/bin/pip install --upgrade mariadb
venv/bin/pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
