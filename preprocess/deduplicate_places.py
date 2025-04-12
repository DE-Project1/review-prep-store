# preprocess/deduplicate_places.py
import pandas as pd

def deduplicate_places(df_place_info: pd.DataFrame, df_reviews: pd.DataFrame):
    # 1. 중복 place_id 찾기
    duplicated_place_ids = df_place_info[df_place_info.duplicated(subset="place_id", keep=False)]["place_id"].unique().tolist()

    # 2. adm_dong_code 기준으로 정렬 후, 중복 제거 (adm_dong_code가 가장 작은 것만 남김)
    df_place_info_sorted = df_place_info.sort_values("adm_dong_code")
    df_place_info_dedup = df_place_info_sorted.drop_duplicates(subset="place_id", keep="first")

    # 3. reviews: dedup된 place_id 중 중복이었던 place_id에 대해 각각 100개씩만 남기기
    df_reviews_filtered = df_reviews[df_reviews["place_id"].isin(duplicated_place_ids)].copy()
    df_reviews_limited = (
        df_reviews_filtered
        .groupby("place_id")
        .head(100)
        .reset_index(drop=True)
    )

    # 4. 중복이 아니었던 place_id 리뷰 전부 유지
    non_duplicated_place_ids = set(df_place_info_dedup["place_id"]) - set(duplicated_place_ids)
    df_reviews_remaining = df_reviews[df_reviews["place_id"].isin(non_duplicated_place_ids)]

    # 5. 최종 리뷰 데이터 합치기
    df_reviews_final = pd.concat([df_reviews_limited, df_reviews_remaining], ignore_index=True)

    return df_place_info_dedup.reset_index(drop=True), df_reviews_final.reset_index(drop=True), duplicated_place_ids
