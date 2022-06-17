# configuration .py file

import logging

################################# LOGGER #################################
# Logger Parameters
log_level = logging.DEBUG
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(log_level)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(log_level)
# create formatter -> add formatter to ch -> add ch to logger
ch.setFormatter(formatter)
logger.addHandler(ch)

