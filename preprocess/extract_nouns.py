import os
from konlpy.tag import Okt
from utils.logger import get_logger

logger = get_logger("extract_nouns")

okt = Okt() #형태소 분석기 for 명사추출

# ✅ korean_stopwords.txt 파일 경로 설정
STOPWORDS_PATH = os.path.join(os.path.dirname(__file__), "korean_stopwords.txt")

def load_stopwords():
    try:
        with open(STOPWORDS_PATH, "r", encoding="utf-8") as f:
            stopwords = set(line.strip() for line in f if line.strip())
        return stopwords
    except FileNotFoundError:
        logger.error(f"Stopwords file not found at {STOPWORDS_PATH}")
        return set()

def extract_nouns_from_text(text, stopwords):
    if not isinstance(text, str):
        return []

    nouns = okt.nouns(text)
    filtered = [word for word in nouns if word not in stopwords and len(word) > 1]
    return filtered


def extract_nouns_from_reviews(df_reviews):
    logger.info("명사 추출 및 불용어 제거 시작")
    stopwords = load_stopwords()
    total = len(df_reviews)

    content_nouns = []

    for idx, row in df_reviews.iterrows():
        text = row["content"]
        nouns = extract_nouns_from_text(text, stopwords)
        content_nouns.append(nouns)

        # 💡 중간 진행상황 로그 (1000개 단위로)
        if (idx + 1) % 1000 == 0 or (idx + 1) == total:
            logger.debug(f"🔄 명사 추출 진행 중: {idx + 1}/{total}개 완료")

    df_reviews["content_nouns"] = content_nouns
    logger.info("명사 추출 및 불용어 제거 완료")
    return df_reviews


