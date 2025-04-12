import json
import pandas as pd

def df_to_json_records(df: pd.DataFrame) -> list:
    return json.loads(df.to_json(orient="records"))

def save_json_file(data: list, filename: str):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)  # type: ignore

