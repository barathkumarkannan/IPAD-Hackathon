import logging
from datetime import datetime
import pytz
import sys



def logger(base_dir):
    current_date = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y%m%d_%H%M%S')
    log_filename = base_dir + "framework_logs" + "/Framework_logs" + current_date + ".log"
    loglevel = logging.DEBUG
    logger = logging.getLogger(__name__)
    if not getattr(logger, 'handler_set', None):
        logger.setLevel(loglevel)
        handler = logging.FileHandler(log_filename, mode='w')
        format = logging.Formatter(
            '%(asctime)s-   %(levelname)s-     %(message)s -   %(filename)s -     %(funcName)s -    %(lineno)d-   %(pathname)s')
        handler.setFormatter(format)
        logger.addHandler(handler)
        logger.setLevel(loglevel)
        logger.handler_set = True

    return logger