import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # 루트 디렉토리 경로 추가

import pandas as pd
from pipeline import deduplicate_places, filter_columns, clean_text, extract_nouns_from_reviews, validate_reviews, convert_to_json
from utils.logger import get_logger

# logger 설정
logger = get_logger("test_pipeline")

# 테스트 데이터를 로컬에서 읽어오기
def load_test_data():
    # 테스트 데이터 파일 경로
    test_place_info_1 = os.path.join(os.path.dirname(__file__), 'test_place_info_1111051500.csv')
    test_place_info_2 = os.path.join(os.path.dirname(__file__), 'test_place_info_1111053000.csv')
    test_reviews_1 = os.path.join(os.path.dirname(__file__), 'test_reviews_1111051500.csv')
    test_reviews_2 = os.path.join(os.path.dirname(__file__), 'test_reviews_1111053000.csv')

    # CSV 파일 읽어오기
    df_place_info_test = pd.concat([pd.read_csv(test_place_info_1), pd.read_csv(test_place_info_2)], ignore_index=True)
    df_reviews_test = pd.concat([pd.read_csv(test_reviews_1), pd.read_csv(test_reviews_2)], ignore_index=True)

    return df_place_info_test, df_reviews_test


def test_deduplicate_places():
    logger.info("🔍 Step 1: 중복 place_id 처리 중...")
    df_place_info_test, df_reviews_test = load_test_data()

    # 중복 제거 실행
    df_place_info_dedup_test, df_reviews_final_test, duplicated_place_ids_test = deduplicate_places(df_place_info_test, df_reviews_test)

    # 결과 확인
    logger.info(f"중복 제거 전 place_info 데이터: {df_place_info_test.shape[0]}개, 중복 제거 후: {df_place_info_dedup_test.shape[0]}개")
    logger.info(f"중복 제거 전 리뷰 데이터: {df_reviews_test.shape[0]}개, 중복 제거 후: {df_reviews_final_test.shape[0]}개")
    logger.info(f"중복된 place_id 수: {len(duplicated_place_ids_test)}")

    assert df_place_info_dedup_test["place_id"].nunique() == len(df_place_info_dedup_test), "중복 place_id가 여전히 존재합니다."
    assert df_reviews_final_test.shape[0] > 0, "리뷰 데이터가 비어 있습니다."
    logger.info("test_deduplicate_places 통과!")

    return df_place_info_dedup_test, df_reviews_final_test  # 다음 단계로 이어지도록 반환


def test_filter_columns(df_place_info_test, df_reviews_test):
    logger.info("🔄 Step 2: 컬럼 필터링 중...")
    # 필터링 실행
    df_place_filtered_test, df_reviews_filtered_test = filter_columns(df_place_info_test, df_reviews_test)

    # 결과 확인
    logger.info(f"place_info 필터링 전: {df_place_info_test.shape[1]} 컬럼, 필터링 후: {df_place_filtered_test.shape[1]} 컬럼")
    logger.info(f"reviews 필터링 전: {df_reviews_test.shape[1]} 컬럼, 필터링 후: {df_reviews_filtered_test.shape[1]} 컬럼")
    logger.info(f"place_info 필터링 후 데이터 샘플:\n{df_place_filtered_test.head()}")
    logger.info(f"reviews 필터링 후 데이터 샘플:\n{df_reviews_filtered_test.head()}")

    assert set(df_place_filtered_test.columns) == {"place_id", "adm_dong_code", "name", "category", "address",
                                              "opening_hours", "naver_rating"}, "place_info 컬럼 필터링이 잘못되었습니다."
    assert set(df_reviews_filtered_test.columns) == {"place_id", "visit_count", "content"}, "reviews 컬럼 필터링이 잘못되었습니다."
    logger.info("test_filter_columns 통과!")

    return df_place_filtered_test, df_reviews_filtered_test  # 다음 단계로 이어지도록 반환


def test_clean_text(df_reviews_test):
    logger.info("🧼 Step 3: 텍스트 정제 중...")
    # 텍스트 정제 실행
    df_reviews_cleaned_test = clean_text(df_reviews_test)

    # 결과 확인
    logger.info(f"정제 전 리뷰 개수: {df_reviews_test.shape[0]}, 정제 후 리뷰 개수: {df_reviews_cleaned_test.shape[0]}")
    logger.info(f"정제된 첫 5개 리뷰:\n{df_reviews_cleaned_test['content'].head()}")

    assert df_reviews_cleaned_test["content"].isnull().sum() == 0, "결측값이 남아 있습니다."
    assert df_reviews_cleaned_test["content"].apply(lambda x: isinstance(x, str)).all(), "리뷰 내용이 문자열이 아닙니다."
    logger.info("test_clean_text 통과!")

    return df_reviews_cleaned_test  # 다음 단계로 이어지도록 반환


def test_extract_nouns(df_reviews_test):
    logger.info("🧠 Step 4: 명사 추출 중...")
    # 명사 추출 실행
    df_reviews_nouns_test = extract_nouns_from_reviews(df_reviews_test)

    # 결과 확인
    logger.info(f"명사 추출된 리뷰 샘플:\n{df_reviews_nouns_test[['content', 'content_nouns']].head()}")
    assert df_reviews_nouns_test["content_nouns"].apply(lambda x: isinstance(x, list)).all(), "명사 추출이 제대로 되지 않았습니다."
    logger.info("test_extract_nouns 통과!")

    return df_reviews_nouns_test  # 다음 단계로 이어지도록 반환


def test_validate_reviews(df_reviews_test):
    logger.info("✅ Step 5: 리뷰 유효성 검증 중...")
    # 리뷰 유효성 검증 실행
    df_reviews_valid_test = validate_reviews(df_reviews_test)

    # 결과 확인
    logger.info(f"유효성 검증 전 리뷰 개수: {df_reviews_test.shape[0]}, 유효성 검증 후 리뷰 개수: {df_reviews_valid_test.shape[0]}")
    logger.info(f"유효성 검증 후 데이터 샘플:\n{df_reviews_valid_test.head()}")
    assert df_reviews_valid_test.shape[0] > 0, "유효하지 않은 리뷰가 제거되었습니다."
    logger.info("test_validate_reviews 통과!")

    return df_reviews_valid_test  # 다음 단계로 이어지도록 반환


def test_convert_to_json(df_place_info_test, df_reviews_test):
    logger.info("📦 Step 6: JSON 변환 중...")
    # JSON 변환 실행
    place_info_json_test, reviews_json_test = convert_to_json(df_place_info_test, df_reviews_test)

    # 결과 확인
    logger.info(f"place_info JSON 샘플: {place_info_json_test[:2]}")
    logger.info(f"reviews JSON 샘플: {reviews_json_test[:2]}")
    assert isinstance(place_info_json_test, list), "place_info JSON 변환 실패"
    assert isinstance(reviews_json_test, list), "reviews JSON 변환 실패"
    logger.info("test_convert_to_json 통과!")


if __name__ == "__main__":
    logger.info("테스트 시작")
    # 각 테스트 실행
    df_place_info, df_reviews = load_test_data()

    # 데이터 중복 제거
    df_place_info_dedup, df_reviews_final = test_deduplicate_places()
    # 컬럼 필터링
    df_place_filtered, df_reviews_filtered = test_filter_columns(df_place_info_dedup, df_reviews_final)
    # 텍스트 정제
    df_reviews_cleaned = test_clean_text(df_reviews_filtered)
    # 명사 추출
    df_reviews_nouns = test_extract_nouns(df_reviews_cleaned)
    # 리뷰 유효성 검증
    df_reviews_valid = test_validate_reviews(df_reviews_nouns)
    # JSON 변환
    test_convert_to_json(df_place_filtered, df_reviews_valid)

    logger.info("모든 테스트가 성공적으로 완료되었습니다!")

