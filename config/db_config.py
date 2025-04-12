import os
from dotenv import load_dotenv
from pymongo import MongoClient

# .env 파일에서 환경 변수 불러오기
load_dotenv(dotenv_path="./env/.env")

#client 객체 생성
def get_mongo_client():
    mongo_uri = os.getenv("MONGO_URI")
    client = MongoClient(mongo_uri)
    return client

#데이터베이스 가져오기
def get_database():
    client = get_mongo_client()
    db = client[os.getenv("DB_NAME")]
    return db

