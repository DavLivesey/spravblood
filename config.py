import os

from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv('HOST')
PG_PSWD = os.getenv('PG_PSWD')
PG_USER = os.getenv('PG_USER')
ADMIN_CHAT = int(os.getenv('ADMIN_CHAT'))
PATH_TO_BASES = os.getenv('PATH_TO_BASES')
PROJECT_DESTINATION = os.getenv('PROJECT_DESTINATION')
