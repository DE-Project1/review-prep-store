import pandas as pd

def filter_columns(df_place_info: pd.DataFrame, df_reviews: pd.DataFrame):
    df_place_filtered = df_place_info[[
        "place_id", "adm_dong_code", "name", "category", "address", "opening_hours", "naver_rating"
    ]].copy()

    df_reviews_filtered = df_reviews[["place_id", "visit_count", "content"]].copy()

    return df_place_filtered, df_reviews_filtered
