import pandas as pd

#place_info, reviews 데이터 프레임을 json 형식으로 저장
def convert_to_json(df_place_info: pd.DataFrame, df_reviews: pd.DataFrame):
    #df_reviews에서 content 칼럼 제외
    df_reviews = df_reviews[["place_id", "visit_count", "content_nouns"]]

    #df_place_info, reviews json 형식으로 및 저장
    place_info_json = df_place_info.to_dict(orient="records")
    reviews_json = df_reviews.to_dict(orient="records")

    return place_info_json, reviews_json


