# pipeline.py
from s3.fetch_data import fetch_place_info, fetch_reviews
from preprocess.deduplicate_places import deduplicate_places
from preprocess.filter_columns import filter_columns
from preprocess.clean_text import clean_text
from preprocess.extract_nouns import extract_nouns_from_reviews
from preprocess.validate_reviews import validate_reviews
from preprocess.csv_to_json import convert_to_json
from db.init_collections import init_collections
from db.insert_data import insert_data
from utils.logger import get_logger


logger = get_logger("pipeline")

def run_pipeline(region_csv_path: str):
    try:
        logger.info("🚀 Step 1: S3에서 데이터 불러오는 중...")
        df_place_info_raw = fetch_place_info()
        df_reviews_raw = fetch_reviews()
    except Exception as e:
        logger.error(f"❌ S3 데이터 불러오기 실패: {e}")
        return

    try:
        logger.info("🔍 Step 2: 중복 place_id 처리 및 리뷰 필터링 중...")
        df_place_dedup, df_reviews_dedup, _ = deduplicate_places(df_place_info_raw, df_reviews_raw)
    except Exception as e:
        logger.error(f"❌ 중복 제거 실패: {e}")
        return

    try:
        logger.info("📑 Step 3: 컬럼 필터링...")
        df_place_filtered, df_reviews_filtered = filter_columns(df_place_dedup, df_reviews_dedup)
    except Exception as e:
        logger.error(f"❌ 컬럼 필터링 실패: {e}")
        return

    try:
        logger.info("🧼 Step 4: 리뷰 텍스트 클렌징...")
        df_reviews_cleaned = clean_text(df_reviews_filtered)
    except Exception as e:
        logger.error(f"❌ 텍스트 클렌징 실패: {e}")
        return

    try:
        logger.info("🧠 Step 5: 명사 추출 및 불용어 제거...")
        df_reviews_nouns = extract_nouns_from_reviews(df_reviews_cleaned)
    except Exception as e:
        logger.error(f"❌ 명사 추출 실패: {e}")
        return

    try:
        logger.info("✅ Step 6: 유효 리뷰만 필터링...")
        df_reviews_valid = validate_reviews(df_reviews_nouns)
    except Exception as e:
        logger.error(f"❌ 유효 리뷰 필터링 실패: {e}")
        return

    try:
        logger.info("📦 Step 7: JSON 변환 중...")
        place_info_json, reviews_json = convert_to_json(df_place_filtered, df_reviews_valid)
    except Exception as e:
        logger.error(f"❌ JSON 변환 실패: {e}")
        return

    try:
        logger.info("🌍 Step 8: 지역구 컬렉션 초기화...")
        init_collections(region_csv_path)
    except Exception as e:
        logger.error(f"❌ 지역구 컬렉션 초기화 실패: {e}")
        return

    try:
        logger.info("📤 Step 9: MongoDB에 데이터 적재 중...")
        insert_data(place_info_json, reviews_json)
    except Exception as e:
        logger.error(f"❌ MongoDB 데이터 적재 실패: {e}")
        return

    logger.info("🎉 모든 데이터 파이프라인 완료 및 MongoDB 저장 완료!")

# 실행 (python pipeline.py)
if __name__ == "__main__":
    run_pipeline("data/adm_dong_list.csv")