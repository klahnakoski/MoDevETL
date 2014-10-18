# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


from datetime import datetime, timedelta
from pyLibrary.collections import MIN, UNION
from pyLibrary.env.elasticsearch import Cluster, Index
from pyLibrary.struct import nvl
from pyLibrary.thread.threads import ThreadedQueue
from pyLibrary.times.timer import Timer
from pyLibrary.cnv import CNV
from pyLibrary.env.logs import Log
from pyLibrary.queries import Q
from pyLibrary.env import startup
from pyLibrary.env.files import File
from pyLibrary.collections.multiset import Multiset


# REPLICATION
#
# Replication has a few benefits:
# 1) The slave can have scripting enabled, allowing more powerful set of queries
# 2) Physical proximity increases the probability of reduced latency
# 3) The slave can be configured with better hardware
# 4) The slave's exclusivity increases availability (Mozilla's public cluster my have time of high load)


far_back = datetime.utcnow() - timedelta(weeks=52)
BATCH_SIZE = 20000


def get_last_updated(es):
    try:
        results = es.search({
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": {
                    "range": {
                        "modified_ts": {"gte": CNV.datetime2milli(far_back)}
                    }}
            }},
            "from": 0,
            "size": 0,
            "sort": [],
            "facets": {"modified_ts": {"statistical": {"field": "modified_ts"}}}
        })

        if results.facets.modified_ts.count == 0:
            return CNV.milli2datetime(0)
        return CNV.milli2datetime(results.facets.modified_ts.max)
    except Exception, e:
        return CNV.milli2datetime(0)


def get_pending(es, since):
    result = es.search({
        "query": {"match_all": {}},
        "from": 0,
        "size": 0,
        "sort": [],
        "facets": {"default": {"statistical": {"field": "bug_id"}}}
    })

    max_bug = int(result.facets.default.max)
    pending_bugs = None

    for s, e in Q.intervals(0, max_bug + 1, 100000):
        Log.note("Collect history for bugs from {{start}}..{{end}}", {"start": s, "end": e})
        result = es.search({
            "query": {"filtered": {
                "query": {"match_all": {}},
                "filter": {"and": [
                    {"range": {"modified_ts": {"gte": CNV.datetime2milli(since)}}},
                    {"range": {"bug_id": {"gte": s, "lte": e}}}
                ]}
            }},
            "from": 0,
            "size": 0,
            "sort": [],
            "facets": {"default": {"terms": {"field": "bug_id", "size": 200000}}}
        })

        temp = Multiset(
            result.facets.default.terms,
            key_field="term",
            count_field="count"
        )

        if pending_bugs is None:
            pending_bugs = temp
        else:
            pending_bugs = pending_bugs + temp

    Log.note("Source has {{num}} bug versions for updating", {
        "num": len(pending_bugs)
    })
    return pending_bugs


def replicate(source, destination, pending, last_updated):
    """
    COPY THE DEPENDENCY RECORDS TO THE destination
    NOTE THAT THE PUBLIC CLUSTER HAS HOLES, SO WE USE blocked TO FILL THEM
    """
    for g, bugs in Q.groupby(pending, max_size=BATCH_SIZE):
        with Timer("Replicate {{num_bugs}} bug versions", {"num_bugs": len(bugs)}):
            data = source.search({
                "query": {"filtered": {
                    "query": {"match_all": {}},
                    "filter": {"and": [
                        {"terms": {"bug_id": set(bugs)}},
                        {"range": {"expires_on": {"gte": CNV.datetime2milli(last_updated)}}},
                        {"or": [
                            {"exists": {"field": "dependson"}},
                            {"exists": {"field": "blocked"}}
                        ]}
                    ]}
                }},
                "from": 0,
                "size": 200000,
                "sort": [],
                "fields": ["bug_id", "modified_ts", "expires_on", "dependson", "blocked"]
            })

            with Timer("Push to destination"):
                d2 = [
                    {
                        "id": str(x.bug_id) + "_" + str(x.modified_ts)[:-3],
                        "value": {
                            "bug_id": x.bug_id,
                            "modified_ts": x.modified_ts,
                            "expires_on": x.expires_on,
                            "dependson": x.dependson
                        }
                    }
                    for x in data.hits.hits.fields
                    if x.dependson
                ]
                destination.extend(d2)

            with Timer("filter"):
                d4 = Q.run({
                    "from": data.hits.hits.fields,
                    "where": {"exists": {"field": "blocked"}}
                })

            with Timer("select"):
                d3 = Q.run({
                    "from": d4,
                    "select": [
                        {"name": "bug_id", "value": "blocked."},  # SINCE blocked IS A LIST, ARE SELECTING THE LIST VALUES, AND EFFECTIVELY PERFORMING A JOIN
                        "modified_ts",
                        "expires_on",
                        {"name": "dependson", "value": "bug_id"}
                    ]
                })

            with Timer("Push to destination"):
                destination.extend([
                    {
                        "id": str(x.bug_id) + "_" + str(x.dependson) + "_" + str(x.modified_ts)[:-3],
                        "value": x
                    }
                    for x in d3
                    if x.dependson
                ])






def main(settings):
    current_time = datetime.utcnow()
    time_file = File(settings.param.last_replication_time)

    # SYNCH WITH source ES INDEX
    source = Index(settings.source)
    destination = Cluster(settings.destination).get_or_create_index(settings.destination)

    # GET LAST UPDATED
    from_file = None
    if time_file.exists:
        from_file = CNV.milli2datetime(CNV.value2int(time_file.read()))
    from_es = get_last_updated(destination) - timedelta(hours=1)
    last_updated = MIN(nvl(from_file, CNV.milli2datetime(0)), from_es)
    Log.note("updating records with modified_ts>={{last_updated}}", {"last_updated": last_updated})

    pending = get_pending(source, last_updated)
    with ThreadedQueue(destination, size=1000) as data_sink:
        replicate(source, data_sink, pending, last_updated)

    # RECORD LAST UPDATED
    time_file.write(unicode(CNV.datetime2milli(current_time)))


def start():
    try:
        settings = startup.read_settings()
        Log.start(settings.debug)
        main(settings)
    except Exception, e:
        Log.error("Problems exist", e)
    finally:
        Log.stop()


if __name__ == "__main__":
    start()
