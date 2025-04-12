# s3/fetch_data.py
import boto3
import pandas as pd
import io
import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

def fetch_and_concat_from_s3(prefix):
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)

    all_csv_keys = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                all_csv_keys.append(key)

    # 파일명을 기준으로 오름차순 정렬
    all_csv_keys.sort()

    df_list = []
    for key in all_csv_keys:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        df = pd.read_csv(io.BytesIO(response["Body"].read()))
        df_list.append(df)

    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

def fetch_place_info():
    return fetch_and_concat_from_s3("place_info/")

def fetch_reviews():
    return fetch_and_concat_from_s3("reviews/")
