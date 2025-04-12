import logging
from logging.handlers import RotatingFileHandler
import os

import logging

def get_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # 콘솔 핸들러 설정
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # 포맷 설정
    formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
    console_handler.setFormatter(formatter)

    # 핸들러 등록
    if not logger.hasHandlers():
        logger.addHandler(console_handler)

    return logger
