import logging


logger = logging.getLogger("key_value_log")
logger.setLevel(logging.DEBUG)
stream_logs = logging.StreamHandler()
# stream_logs.setLevel(logging.INFO)
stream_logs.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_logs.setFormatter(formatter)
logger.addHandler(stream_logs)
