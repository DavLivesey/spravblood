import os

from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv('HOST')
PG_PSWD = os.getenv('PG_PSWD')
PG_USER = os.getenv('PG_USER')
PG_PORT = os.getenv('PG_PORT')
BASE_NAME = os.getenv('BASE_NAME')
ADMIN_CHAT = int(os.getenv('ADMIN_CHAT'))
PATH_TO_BASES = os.getenv('PATH_TO_BASES')
