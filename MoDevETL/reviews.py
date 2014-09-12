# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
import functools
from MoDevETL.util.cnv import CNV

from MoDevETL.util.env import startup, elasticsearch
from MoDevETL.util.env.elasticsearch import Cluster
from MoDevETL.util.env.logs import Log
from MoDevETL.util.maths import Math
from MoDevETL.util.queries import Q, index
from MoDevETL.util.queries.es_query import ESQuery
from MoDevETL.util.struct import nvl
from MoDevETL.util.thread.multithread import Multithread
from MoDevETL.util.thread.threads import ThreadedQueue
from MoDevETL.util.times.dates import Date


TYPES = ["review", "superreview", "ui-review"]

# TODO: ADD TESTS
# BUGS WITH PROBLEM REVIEWS
#   1012506



def full_etl(settings, sink, bugs):
    es = elasticsearch.Index(settings.source)
    with ESQuery(es) as esq:
        versions = esq.query({
            "from": "bugs",
            "select": "*",
            "where": {"terms": {"bug_id": list(bugs)}}
        })

    starts = Q.run({
        "select": [
            "bug_id",
            "bug_status",
            {"name": "attach_id", "value": "attachments.attach_id"},
            {"name": "request_time", "value": "modified_ts"},
            {"name": "request_type", "value": "attachments.flags.request_type"},
            {"name": "reviewer", "value": "attachments.flags.requestee"},
            {"name": "created_by", "value": "attachments.created_by"},
            "product",
            "component"
        ],
        "from":
            versions,
        "where":
            {"and": [
                {"terms": {"attachments.flags.request_status": ["?"]}},
                {"terms": {"attachments.flags.request_type": TYPES}},
                {"equal": ["attachments.flags.modified_ts", "modified_ts"]},
                {"term": {"attachments.isobsolete": 0}}
            ]},
        "sort": ["bug_id", "attach_id", "created_by"]
    })

    ends = Q.run({
        "select": [
            {"name": "bug_id", "value": "bug_id"},
            "bug_status",
            {"name": "attach_id", "value": "attachments.attach_id"},
            {"name": "modified_ts", "value": lambda r: Math.max(r.modified_ts, r.attachments.modified_ts, r.attachments.flags.modified_ts)},
            {"name": "reviewer", "value": "attachments.flags.requestee"},
            {"name": "request_type", "value": "attachments.flags.request_type"},
            {"name": "modified_by", "value": "attachments.flags.modified_by"},
            {"name": "product", "value": "product"},
            {"name": "component", "value": "component"},
            {"name": "review_end_reason", "value": lambda r: 'done' if r.attachments.flags.request_status != '?' else ('obsolete' if r.attachments.isobsolete == 1 else 'closed')},
            {"name": "review_result", "value": lambda r: '+' if r.attachments.flags.request_status == '+' else ('-' if r.attachments.flags.request_status == '-' else '?')}
        ],
        "from":
            versions,
        "where":
            {"and": [
                {"terms": {"attachments.flags.request_type": TYPES}},
                {"or": [
                    {"and": [# IF THE REQUESTEE SWITCHED THE ? FLAG, THEN IT IS DONE
                        {"term": {"attachments.flags.previous_status": "?"}},
                        {"not": {"term": {"attachments.flags.request_status": "?"}}},
                        {"equal": ["attachments.flags.modified_ts", "modified_ts"]}
                    ]},
                    {"and": [# IF OBSOLETED THE ATTACHMENT, IT IS DONE
                        {"term": {"attachments.isobsolete": 1}},
                        {"term": {"previous_values.isobsolete_value": 0}}
                    ]},
                    {"and": [# SOME BUGS ARE CLOSED WITHOUT REMOVING REVIEW
                        {"terms": {"bug_status": ["resolved", "verified", "closed"]}},
                        {"not": {"terms": {"previous_values.bug_status_value": ["resolved", "verified", "closed"]}}}
                    ]}
                ]}
            ]}
    })

    # REVIEWS END WHEN REASSIGNED TO SOMEONE ELSE
    changes = Q.run({
        "select": [
            "bug_id",
            {"name": "attach_id", "value": "changes.attach_id"},
            "modified_ts",
            {"name": "reviewer", "value": lambda r: r.changes.old_value.split("?")[1]},
            {"name": "request_type", "value": lambda r: r.changes.old_value.split("?")[0]},
            {"name": "modified_by", "value": "null"},
            "product",
            "component",
            {"name": "review_end_reason", "value": "'reassigned'"}
        ],
        "from":
            versions,
        "where":
            {"and": [# ONLY LOOK FOR NAME CHANGES IN THE "review?" FIELD
                {"term": {"changes.field_name": "flags"}},
                {"or": [{"prefix": {"changes.old_value": t + "?"}} for t in TYPES]}
            ]}
    })

    ends.extend(changes)

    # PYTHON VERSION NOT CAPABLE OF THIS JOIN, YET
    # reviews = Q.run({
    #     "from":
    #         starts,
    #     "select": [
    #         {"name": "bug_status", "value": "bug_status", "aggregate": "one"},
    #         {"name": "review_time", "value": "doneReview.modified_ts", "aggregate": "minimum"},
    #         {"name": "review_result", "value": "doneReview.review_result", "aggregate": "minimum"},
    #         {"name": "product", "value": "nvl(doneReview.product, product)", "aggregate": "minimum"},
    #         {"name": "component", "value": "nvl(doneReview.component, component)", "aggregate": "minimum"},
    #         # {"name": "keywords", "value": "(nvl(keywords, '')+' '+ETL.parseWhiteBoard(whiteboard)).trim()+' '+flags", "aggregate": "one"},
    #         {"name": "requester_review_num", "value": "-1", "aggregate": "one"}
    #     ],
    #     "analytic": [
    #         {"name": "is_first", "value": "rownum==0 ? 1 : 0", "sort": "request_time", "edges": ["bug_id"]}
    #     ],
    #     "edges": [
    #         "bug_id",
    #         "attach_id",
    #         {"name": "reviewer", "value": "requestee"},
    #         {"name": "requester", "value": "created_by"},
    #         {"name": "request_time", "value": "modified_ts"},
    #         {
    #             "name": "doneReview",
    #             "test":
    #                 "bug_id==doneReview.bug_id && " +
    #                 "attach_id==doneReview.attach_id && " +
    #                 "requestee==doneReview.requestee && " +
    #                 "!(bug_status=='closed' && doneReview.review_end_reason=='closed') && " +
    #                 "modified_ts<=doneReview.modified_ts",
    #             "allowNulls": True,
    #             "domain": {"type": "set", "key":["bug_id", "attach_id", "requestee", "modified_ts"], "partitions": ends}
    #         }
    #     ]
    # })


    reviews = []
    ends = index.Index(ends, keys=["bug_id", "attach_id", "request_type", "reviewer"])

    for g, s in Q.groupby(starts, ["bug_id", "attach_id", "request_type", "reviewer"]):
        start_candidates = Q.sort(s, {"value": "request_time", "sort": 1})
        end_candidates = Q.sort(ends[g], {"value": "modified_ts", "sort": 1})

        #ZIP, BUT WITH ADDED CONSTRAINT s.modified_ts<=e.modified_ts
        if len(start_candidates) > 1:
            Log.note("many reviews on one attachment")
        ei = 0
        for i, s in enumerate(start_candidates):
            while ei < len(end_candidates) and end_candidates[ei].modified_ts < nvl(s.request_time, CNV.datetime2milli(Date.MAX)):
                ei += 1
            e = end_candidates[ei]

            s.review_time = e.modified_ts
            s.review_result = e.review_result
            s.product = nvl(e.product, s.product)
            s.component = nvl(e.component, s.component)
            s.requester_review_num = -1
            ei += 1

            if s.bug_status == 'closed' and e.review_end_reason == 'closed':
                #reviews on closed bugs are ignored
                continue
            reviews.append(s)

    Q.run({
        "from": reviews,
        "window": [{
            "name": "is_first",
            "value": "rownum == 0",
            "edges": ["bug_id"],
            "sort": ["request_time"],
            "aggregate": "none"
        }]
    })

    sink.extend({"value": r} for r in reviews)


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)
    try:
        reviews = Cluster(settings.destination).create_index(settings.destination)
        bugs = Cluster(settings.source).get_index(settings.source)

        with ESQuery(bugs) as esq:
            max_bug = esq.query({
                "from": "private_bugs",
                "select": {"name": "max_bug", "value": "bug_id", "aggregate": "maximum"}
            })

        #PROBE WHAT RANGE OF BUGS IS LEFT TO DO (IN EVENT OF FAILURE)
        with ESQuery(reviews) as esq:
            min_bug = esq.query({
                "from": "reviews",
                "select": {"name": "min_bug", "value": "bug_id", "aggregate": "minimum"}
            })

        size = nvl(settings.size, 1000)
        threads = nvl(settings.threads, 4)

        min_bug = Math.max(0, Math.floor(Math.min(min_bug+ size*threads, max_bug), size))
        min_bug = 1000

        with ThreadedQueue(reviews, size=size) as sink:
            func = functools.partial(full_etl, settings, sink)
            with Multithread(func, threads=threads) as m:
                m.inbound.silent = True
                m.execute(reversed([{"bugs": xrange(s, e)} for s, e in Q.intervals(0, min_bug, size=1000)]))

        reviews.add_alias()
        reviews.delete_all_but_self()
    finally:
        Log.stop()


if __name__ == '__main__':
    main()
