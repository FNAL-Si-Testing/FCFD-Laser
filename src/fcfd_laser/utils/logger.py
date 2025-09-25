# logger.py
import logging
import os
import random
from datetime import datetime


def setup_logger(log_path):
    logger = logging.getLogger('logger')
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - [%(processName)s] - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

def get_logger(log_dir, log_filename = None, fingerprint = None):
    os.makedirs(log_dir, exist_ok=True)
    
    if fingerprint is None: 
        fingerprint = '%08x' % random.randrange(16**8)
    if log_filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"log_{timestamp}_{fingerprint}.txt"

    log_path = os.path.join(log_dir, log_filename)
    logger = setup_logger(log_path)

    return logger