import pandas as pd
from config.db_config import get_database

#데이터 초기화 및 지역구 컬렉션 저장
def init_collections(region_csv_path):
    db = get_database()

    region_df = pd.read_csv(region_csv_path)
    region_df = region_df[["adm_dong_code", "district", "neighborhood"]]

    region_map_data = region_df.to_dict(orient = "records")

    db["region_map"].drop()
    db["region_map"].insert_many(region_map_data)