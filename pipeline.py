# pipeline.py
from s3.fetch_data import fetch_and_concat_from_s3, fetch_place_info, fetch_reviews
from preprocess.deduplicate_places import deduplicate_places
from preprocess.filter_columns import filter_place_columns, filter_review_columns
from preprocess.clean_text import clean_review_text
from preprocess.extract_nouns import extract_nouns_from_reviews
from preprocess.validate_reviews import validate_reviews
from preprocess.csv_to_json import convert_to_json
from db.init_collections import init_collections
from db.insert_data import insert_data
from utils.logger import get_logger


logger = get_logger("pipeline")

def main():
    logger.info("🚀 파이프라인 시작")

    # Step 1: S3에서 데이터 로드
    logger.info("📦 S3에서 데이터 로딩 중...")
    place_df, reviews_df = fetch_and_concat_from_s3()
    logger.info(f"✅ 데이터 로딩 완료: place_info={len(place_df)}개, reviews={len(reviews_df)}개")

    # Step 2: 음식점 중복 제거
    logger.info("🧹 중복 음식점 제거 중...")
    place_df = deduplicate_places(place_df)
    logger.info(f"✅ 중복 제거 후 음식점 수: {len(place_df)}개")

    # Step 3: 칼럼 필터링
    logger.info("🔍 칼럼 필터링 중...")
    place_df = filter_place_columns(place_df)
    reviews_df = filter_review_columns(reviews_df)

    # Step 4: 리뷰 텍스트 클렌징
    logger.info("🧼 리뷰 텍스트 클렌징 중...")
    reviews_df = clean_review_text(reviews_df)

    # Step 5: 명사 추출 + 불용어 제거
    logger.info("🧠 리뷰에서 명사 추출 중...")
    reviews_df = extract_nouns_from_reviews(reviews_df)

    # Step 6: 유효 리뷰 필터링
    logger.info("🧪 유효 리뷰 필터링 중...")
    reviews_df = validate_reviews(reviews_df)
    logger.info(f"✅ 유효 리뷰 수: {len(reviews_df)}개")

    # Step 7: CSV → JSON 구조 변환
    logger.info("🔁 JSON 적재 형식으로 변환 중...")
    place_json, reviews_json = convert_to_json(place_df, reviews_df)

    # Step 8: MongoDB 컬렉션 초기화
    logger.info("🗃️ MongoDB 컬렉션 생성 중...")
    init_collections()

    # Step 9: MongoDB에 데이터 적재
    logger.info("📥 MongoDB에 데이터 적재 중...")
    insert_data(place_json, reviews_json)
    logger.info("✅ MongoDB 적재 완료")

    logger.info("🎉 전체 파이프라인 완료")

if __name__ == "__main__":
    main()