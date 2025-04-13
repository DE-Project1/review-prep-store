import pandas as pd
import re

def clean_text(df_reviews: pd.DataFrame):
    # 1. content 컬럼의 결측치 먼저 제거
    df_reviews = df_reviews.dropna(subset=["content"])

    # 2. 특수문자, 이모지 제거
    df_reviews.loc[:, "content"] = df_reviews["content"].apply(
        lambda x: re.sub(r"[^가-힣a-zA-Z0-9\s]", "", str(x))
    )

    # 3. 공백만 남은 리뷰 제거 → 이건 "내용이 없다"는 거니까 제거
    df_reviews = df_reviews[df_reviews["content"].str.strip() != ""]

    # 4. 혹시 모르니 다시 한 번 결측치 제거 (위 처리로 인해 생겼을 수도 있는 NaN 방지)
    df_reviews = df_reviews.dropna(subset=["content"])

    return df_reviews.reset_index(drop=True)


