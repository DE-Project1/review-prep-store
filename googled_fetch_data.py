from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
import pandas as pd
import io
import os
import json
from dotenv import load_dotenv
from utils.logger import get_logger  # 로그 추가

load_dotenv(dotenv_path='env/.env')

# 로깅
logger = get_logger("fetch_data")

# 구글 서비스 계정 키 파일 경로
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

# 구글 드라이브 서비스 객체 생성
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive_service = build("drive", "v3", credentials=creds)


def list_files_in_folder(folder_id):
    """구글 드라이브 폴더 안의 모든 파일 리스트 가져오기 (페이징 처리 포함)"""
    query = f"'{folder_id}' in parents and mimeType='application/json' and trashed=false"
    all_files = []
    page_token = None

    while True:
        response = drive_service.files().list(
            q=query,
            fields="nextPageToken, files(id, name)",
            pageSize=500,  # 한 번에 최대 500개
            pageToken=page_token
        ).execute()

        all_files.extend(response.get("files", []))
        page_token = response.get("nextPageToken")

        if not page_token:
            break  # 더 이상 다음 페이지 없으면 종료

    return all_files

def download_json_file(file_id):
    """파일 ID로 JSON 파일 다운로드 후 pandas DataFrame으로 변환"""
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    data = json.load(fh)
    return pd.json_normalize(data)  # JSON을 DataFrame으로 변환


def fetch_and_concat_from_gdrive(folder_id):
    """폴더 ID 내의 모든 JSON 파일을 다운로드하고 합치기"""
    files = list_files_in_folder(folder_id)
    files.sort(key=lambda x: x["name"])  # 파일 이름순 정렬

    df_list = []
    for idx, file in enumerate(files, 1):
        try:
            df = download_json_file(file["id"])
            df_list.append(df)
            logger.info(f"[{idx}/{len(files)}] 📥 {file['name']} 로드 완료")
        except Exception as e:
            logger.warning(f"⚠️ {file['name']} 로드 실패: {e}")

    return pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()


def fetch_place_info():
    place_info_folder_id = os.getenv("PLACE_INFO_FOLDER_ID")
    return fetch_and_concat_from_gdrive(place_info_folder_id)


def fetch_reviews():
    reviews_folder_id = os.getenv("REVIEWS_FOLDER_ID")
    return fetch_and_concat_from_gdrive(reviews_folder_id)
