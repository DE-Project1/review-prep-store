import pandas as pd

def validate_reviews(df_reviews: pd.DataFrame):
    df_valid = df_reviews[df_reviews["content_nouns"].apply(lambda x: len(x) > 4)]
    return df_valid.reset_index(drop=True)

