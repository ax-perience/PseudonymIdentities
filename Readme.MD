# PseudonymIdentities

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
