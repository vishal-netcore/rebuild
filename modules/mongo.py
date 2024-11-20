from pymongo import MongoClient
from .logger import logger

def connect(username, password, host, port, is_direct_connection=False):
    ## option for direction connection 
    mongodb_uri = f'mongodb://{username}:{password}@{host}:{port}'
    try:
        client = MongoClient(mongodb_uri)
    except Exception as e:
        logger.fatal("Failed to connect to MongoDB", e)
    
    logger.info(f"Connected to {host}:{port}")

    return client