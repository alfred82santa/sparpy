import sys
from logging import DEBUG, INFO, StreamHandler, getLogger

from .config import ConfigParser


def build_logger(config, debug=False):
    try:
        config = config['logger']
    except (KeyError, TypeError):
        config = ConfigParser(default_sections=('logger',))
        config = config['logger']

    logger = getLogger('sparpy')
    logger.addHandler(StreamHandler(stream=sys.stdout))
    if debug:
        logger.setLevel(DEBUG)
    else:
        logger.setLevel(config.getint('level', fallback=INFO))

    return logger
