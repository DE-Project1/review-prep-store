import pandas as pd
from config.db_config import get_database

#place_info_json, reviews_json 몽고디비 적재
def insert_data(place_info_json, reviews_json):
    db = get_database()

    db["place_info"].drop()
    db["place_info"].insert_many(place_info_json)

    db["place_info"].drop()
    db["place_info"].insert_many(reviews_json)