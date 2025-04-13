from dotenv import load_dotenv
import os

# .env 파일을 명시적으로 로드
load_result = load_dotenv(dotenv_path='env/.env')  # 로드 시도
print(f"환경 변수 로드 결과: {load_result}")  # 로드 성공 여부 확인

# 환경 변수 확인
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
bucket_name = os.getenv("BUCKET_NAME")

print(f"AWS Access Key: {aws_access_key}")
print(f"AWS Secret Key: {aws_secret_key}")
print(f"Bucket Name: {bucket_name}")
