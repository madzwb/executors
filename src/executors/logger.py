import logging
import os

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.addHandler(logging.NullHandler())
