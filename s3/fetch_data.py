# s3/fetch_data.py
import boto3
import pandas as pd
import io
import os
from dotenv import load_dotenv
from utils.logger import get_logger  # 로그 추가

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")

logger = get_logger("fetch_data")  # 로거 생성

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)


def fetch_and_concat_from_s3(prefix, chunk_size=10000):
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)

    all_csv_keys = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                all_csv_keys.append(key)

    all_csv_keys.sort()  # 정렬된 순서로 로딩

    df_list = []
    for idx, key in enumerate(all_csv_keys, 1):
        try:
            response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
            chunk_iter = pd.read_csv(io.BytesIO(response["Body"].read()), chunksize=chunk_size)

            # 각 청크를 df_list에 추가
            for chunk in chunk_iter:
                df_list.append(chunk)

            logger.info(f"[{idx}/{len(all_csv_keys)}] 📥 {key} 로드 완료")  # 성공 로그 출력
        except Exception as e:
            logger.warning(f"⚠️ {key} 로드 실패: {e}")  # 실패 로그 출력

    # 모든 청크를 합쳐서 하나의 DataFrame으로 반환
    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()


def fetch_place_info(chunk_size=10000):
    return fetch_and_concat_from_s3("place_info/", chunk_size)


def fetch_reviews(chunk_size=10000):
    return fetch_and_concat_from_s3("reviews/", chunk_size)


