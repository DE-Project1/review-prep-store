# utils/logger.py
import logging

def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # DEBUG부터 찍히게 설정

    formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s", "%Y-%m-%d %H:%M:%S")

    # 콘솔 핸들러 (DEBUG 이상 출력)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    # 기존 핸들러 중복 방지
    if not logger.hasHandlers():
        logger.addHandler(console_handler)

    logger.propagate = False
    return logger
