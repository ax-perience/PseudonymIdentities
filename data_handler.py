import datetime, pytz, requests, json, cfg, copy

################################ LOGGER ######################################
logger = cfg.logger

timezone = pytz.timezone("Europe/Berlin")
header = {'Accept': "application/binary", 'Content-type': "application/x-ndjson"}


def kill_oldadswizzids(url, auth, kill_date):

    data = {
        "query": {
            "bool": {
                "filter": [
                    {"range": {
                        "last_activity": {
                            "lt": kill_date
                        }
                    }}]
            }
        }
    }
    data = json.dumps(data)
    r = requests.post(url=url, auth=auth, headers=header, data=data)
    respond = json.loads(r.text)
    if r.status_code != 200: logger.info("! Error while deleting old AdSwizzIDs: " + r.text)
    else: logger.info("Deleted " + str(respond["deleted"]) + " documents/AdSwizz-IDs.")

def get_cardinality(query, url, auth):
    # Get total unique AdSwizz-IDs
    body = {"size": 0,
            "query": query,
            "aggs": {
                "adswizz_ids": {
                    "cardinality": {
                        "field": "identifiers_flat.adswizz-listenerid"
                    }
                }
            }
            }

    data = json.dumps(body)
    r = requests.post(url, auth=auth, headers=header, data=data)
    respond = json.loads(r.text)
    total_adswizz_ids = int(respond["aggregations"]["adswizz_ids"]["value"])

    return total_adswizz_ids

def get_aggregation_data(query, partitions, es_client, index):
    list_of_docs = []

    for partition in range(0, partitions):

        body = {"size": 0,
                "query": query,
                "aggs": {
                    "adswizz_ids": {
                        "terms": {
                            "field": "identifiers_flat.adswizz-listenerid",
                            "include": {
                                "partition": partition,
                                "num_partitions": partitions
                            },
                            "size": 6000
                        },
                        "aggs": {
                            "partnerkeys": {
                                "terms": {
                                    "field": "partnerkey",
                                    "size": 500
                                },

                                "aggs": {
                                    "last_activity": {
                                        "max": {
                                            "field": "payload.occurredon",
                                            "format": "YYYY-MM-dd'T'HH:mm:ssZZZZZ"
                                        }
                                    },
                                    "cmp-userids": {
                                        "terms": {
                                            "field": "identifiers_flat.cmp-userid"
                                        }
                                    },
                                    "latest": {
                                        "top_hits": {
                                            "size": 1,
                                            "_source": {
                                                "includes": [
                                                    "identifiers_flat.cmp-userid",
                                                    "uuid",
                                                    "payload.occurredon",
                                                    "partnerkey",
                                                    "payload.tc_string"
                                                ]
                                            },
                                            "sort": {
                                                "payload.occurredon": "desc"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                }

        result = es_client.search(index=index, body=body)


        for adswizzid_id in result["aggregations"]["adswizz_ids"]["buckets"]:

            adswizz_id = adswizzid_id["key"].lower()
            if adswizz_id == "": continue

            # Count total number of cmp userids to check if an AdSwizz ID is unique globally
            total_cmp_userids_global = 0
            for pkey in adswizzid_id["partnerkeys"]["buckets"]:
                total_cmp_userids_global = total_cmp_userids_global + len(pkey["cmp-userids"]["buckets"])

            # Pulling data for each partner
            for pkey in adswizzid_id["partnerkeys"]["buckets"]:
                document = {}
                partnerkey = pkey["key"]

                last_activity = pkey["last_activity"]["value_as_string"]

                total_cmp_userids = len(pkey["cmp-userids"]["buckets"])

                latest_data = pkey["latest"]["hits"]["hits"][0]["_source"]

                if "uuid" in latest_data: document["uuid"] = latest_data["uuid"]

                if "tc_string" in latest_data["payload"]:
                    document["tc_string"] = latest_data["payload"]["tc_string"]
                    document["tc_string_exists"] = True
                else: document["tc_string_exists"] = False

                if len(latest_data["identifiers_flat"]["cmp-userid"]) > 0: document["cmp_userid"] = \
                latest_data["identifiers_flat"]["cmp-userid"][0]

                document["adswizz_id"] = adswizz_id
                document["partnerkey"] = partnerkey
                document["last_activity"] = last_activity
                document["total_cmp_userids"] = total_cmp_userids

                if total_cmp_userids == 1:
                    document["unique_adswizzid_partner"] = True
                else:
                    document["unique_adswizzid_partner"] = False

                if total_cmp_userids_global == 1:
                    document["unique_adswizzid_global"] = True
                else:
                    document["unique_adswizzid_global"] = False

                document["createdon"] = int(datetime.datetime.now().timestamp())
                document["updatedon"] = int(datetime.datetime.now().timestamp())
                list_of_docs.append(document)

        logger.info("Partition: " + str(partition) + ". Total docs so far: " + str(len(list_of_docs)))

    return list_of_docs

def upload_data(docs, psid_index, es_client_url, auth):
    docs_to_write = []

    for doc in range(0, len(docs)):
        bulk_intro = {"update": {"_index": psid_index,
                                 "_id": docs[doc]["partnerkey"] + "_" + docs[doc]["adswizz_id"]}}

        # If an existing document will be updated, createdon key will be not used
        doc_updated = copy.copy(docs[doc])
        del doc_updated["createdon"]
        del docs[doc]["updatedon"]

        # If an Adswizz-ID for a partner already exists, "doc" will be used for updating - otherwise "upsert" doc will
        # be used.
        document = {"doc": doc_updated,
                    "upsert": docs[doc]}

        docs_to_write.append(bulk_intro)
        docs_to_write.append(document)

        if len(docs_to_write) > 500:
            data_to_post = "\n".join(json.dumps(d) for d in docs_to_write) + "\n"
            r = requests.post(url=es_client_url + "/" + psid_index + "/_bulk", auth=auth, headers=header,
                              data=data_to_post)
            docs_to_write = []
            if r.status_code != 200:
                logger.info("! Error while writing data: " + r.text)

    if len(docs_to_write) > 0:
        data_to_post = "\n".join(json.dumps(d) for d in docs_to_write) + "\n"
        r = requests.post(url=es_client_url + "/" + psid_index + "/_bulk", auth=auth, headers=header,
                          data=data_to_post)

        if r.status_code != 200:
            logger.info("! Error while writing data: " + r.text)