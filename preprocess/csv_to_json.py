import pandas as pd

#place_info, reviews 데이터 프레임을 json 형식으로 저장
def convert_to_json(df_place_info: pd.DataFrame, df_reviews: pd.DataFrame):
    place_info_json = df_place_info.to_dict(orient="records")
    reviews_json = df_reviews.to_dict(orient="records")
    return place_info_json, reviews_json


