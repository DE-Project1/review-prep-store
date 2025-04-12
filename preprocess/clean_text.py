import pandas as pd
import re

def clean_text(df_reviews: pd.DataFrame):
    # 결측값 제거
    df_cleaned = df_reviews.dropna(subset=["content"])

    # 이모지, 특수문자 제거
    df_cleaned.loc["content"] = df_cleaned["content"].apply(
        lambda x: re.sub(r"[^가-힣a-zA-Z0-9\s]", "", x)
    )

    # 공백만 남은 리뷰 제거
    df_cleaned = df_cleaned[df_cleaned["content"].str.strip() != ""]

    return df_cleaned.reset_index(drop=True)
