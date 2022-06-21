"""
PseudonymIdentities

DESCRIPTION:
============

This scripts stores all unique AdSwizz-IDs per partner as a document (_id: partnerkey_adswizzid) with relevant
data (e.g. tcString, CMP user ids), which will be used for a regular upload to a DMP. You can set up a specific
time frame (days_to_count in config.ini) to define for how many days unique AdSwizz-IDs should be pulled from
Elastic.

FUNCTIONS:
==========

data_handler.get_cardinality
    Counts unique AdSwizz-IDs to calculate number of partitions for pulling data (due to high amount of documents)

data_handler.get_aggregation_data
    Gets all aggregated AdSwizz data per partner and stores information in a list object

data_handler.upload_data
    Uploads all data from list object to pseudonym index

"""


from elasticsearch import Elasticsearch
import datetime, pytz, configparser, os, sys, cfg, data_handler

timezone = pytz.timezone("Europe/Berlin")

########################### Reading config.ini ###########################
config = configparser.ConfigParser()
absFilePath = os.path.abspath(__file__)
path, filename = os.path.split(absFilePath)
config_file = os.path.join(path, "config.ini")

################################ LOGGER ######################################
logger = cfg.logger

########################### Script starts here ###########################
logger.info("*** Script started")
try:
    config.read(config_file)
except:
    print("Error reading config.ini. Script stops.")
    sys.exit()

env = config['PARAMETERS']["environment"]
es_client_url = config[env]["ES_CLIENT"]
auth = (config[env]["USER"], config[env]["PASSWORD"])
es_client = Elasticsearch(hosts=es_client_url, http_auth=auth, max_retries=10, retry_on_timeout=True)

index_datastream = config[env]["INDEX_DATASTREAM"]
index_identities = config[env]["INDEX_IDENTITIES"]


# Defining timeframe for Elastic
timestamp_end = datetime.datetime.today()
timestamp_start = timestamp_end - datetime.timedelta(days = int(config['PARAMETERS']["days_to_count"]))
kill_date = timestamp_end - datetime.timedelta(days = int(config['PARAMETERS']["kill_days"]))
timeframe_end = datetime.datetime.combine(timestamp_end, datetime.datetime.min.time()).timestamp()
timeframe_start = datetime.datetime.combine(timestamp_start, datetime.datetime.min.time()).timestamp()
kill_date_ts = datetime.datetime.combine(kill_date, datetime.datetime.min.time()).timestamp()

tf_start_elastic = datetime.datetime.fromtimestamp(timeframe_start, timezone).isoformat()
tf_end_elastic = datetime.datetime.fromtimestamp(timeframe_end, timezone).isoformat()
kill_date = datetime.datetime.fromtimestamp(kill_date_ts, timezone).isoformat()

logger.info("Now deleting all AdSwizz-IDs which are older than " + str(config['PARAMETERS']["kill_days"]) + " days.")
data_handler.kill_oldadswizzids(es_client_url + "/" + index_identities + "/_delete_by_query", auth, kill_date)

logger.info("Pseudonym identities will be counted for timeframe: "+ str(tf_start_elastic) + " - " + str(tf_end_elastic) + ".")

# Universal query for all ES requests
query = {"bool": {"must": [{"range": {"createdon": {"lt": tf_end_elastic, "gte": tf_start_elastic }}}]}}

# Count unique Adswizz-IDs (be aware: Low precision of Elastic) to define number of partitions, which will be
# increased by 2.
total_adswizz_ids = data_handler.get_cardinality(query, es_client_url + "/" + index_datastream + "/_search", auth)
partitions = round(total_adswizz_ids / 5000) + round(total_adswizz_ids / 50000)
logger.info("Approximately " + str(total_adswizz_ids) + " total unique AdSwizz-IDs found. Aggregations will be now "
                                                        "pulled in " + str(partitions) + " partitions.")

# Pulling data from Elastic
data = data_handler.get_aggregation_data(query, partitions, es_client, index_datastream)

# Writing documents to pseudonym identities index
logger.info("Now writing documents to pseudonym identities index.")
data_handler.upload_data(data, index_identities, es_client_url, auth)
logger.info("** Script finished.")