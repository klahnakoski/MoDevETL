# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import division
from __future__ import unicode_literals

from future.utils import text_type
from jx_python import jx
from mo_dots import coalesce
from mo_json import value2json
from mo_logs import Log
from mo_logs import startup
from mo_math import Math
from mo_threads import ThreadedQueue, Thread, THREAD_STOP, Queue
from mo_times import Date, Timer

from jx_elasticsearch.jx_usingES import FromES
from mo_collections.index import Index
from mo_json.encoder import datetime2milli
from pyLibrary.env import elasticsearch
from pyLibrary.env.elasticsearch import Cluster

TYPES = ["review", "superreview", "ui-review"]

# TODO: ADD TESTS
# BUGS WITH PROBLEM REVIEWS
#   1012506



def full_etl(settings, sink, bugs):
    with Timer("process block {{start}}", {"start": min(bugs)}):
        esq = FromES(settings.source)
        versions = esq.query({
            "from": "bugs",
            "select": "*",
            "where": {"terms": {"bug_id": bugs}}
        })

        starts = jx.run({
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
                    {"eq": ["attachments.flags.modified_ts", "modified_ts"]},
                    {"eq": {"attachments.isobsolete": 0}}
                ]},
            "sort": ["bug_id", "attach_id", "created_by"]
        })

        ends = jx.run({
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
                            {"eq": {"attachments.flags.previous_status": "?"}},
                            {"not": {"eq": {"attachments.flags.request_status": "?"}}},
                            {"eq": ["attachments.flags.modified_ts", "modified_ts"]}
                        ]},
                        {"and": [# IF OBSOLETED THE ATTACHMENT, IT IS DONE
                            {"eq": {"attachments.isobsolete": 1}},
                            {"eq": {"previous_values.isobsolete_value": 0}}
                        ]},
                        {"and": [# SOME BUGS ARE CLOSED WITHOUT REMOVING REVIEW
                            {"terms": {"bug_status": ["resolved", "verified", "closed"]}},
                            {"not": {"terms": {"previous_values.bug_status_value": ["resolved", "verified", "closed"]}}}
                        ]}
                    ]}
                ]}
        })

        # SOME ATTACHMENTS GO MISSING, CLOSE THEM TOO
        closed_bugs = {b.bug_id: b for b in jx.filter(versions, {"and": [# SOME BUGS ARE CLOSED WITHOUT REMOVING REVIEW
            {"terms": {"bug_status": ["resolved", "verified", "closed"]}},
            {"range": {"expires_on": {"gte": Date.now().milli}}}
        ]})}

        for s in starts:
            if s.bug_id in closed_bugs:
                e = closed_bugs[s.bug_id]
                ends.append({
                    "bug_id": e.bug_id,
                    "bug_status": e.bug_status,
                    "attach_id": s.attach_id,
                    "modified_ts": e.modified_ts,
                    "reviewer": s.reviewer,
                    "request_type": s.request_type,
                    "modified_by": e.modified_by,
                    "product": e.product,
                    "component": e.component,
                    "review_end_reason": 'closed',
                    "review_result": '?'
                })

        # REVIEWS END WHEN REASSIGNED TO SOMEONE ELSE
        changes = jx.run({
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
                    {"eq": {"changes.field_name": "flags"}},
                    {"or": [{"prefix": {"changes.old_value": t + "?"}} for t in TYPES]}
                ]}
        })

        ends.extend(changes)

    # PYTHON VERSION NOT CAPABLE OF THIS JOIN, YET
    # reviews = jx.run({
    #     "from":
    #         starts,
    #     "select": [
    #         {"name": "bug_status", "value": "bug_status", "aggregate": "one"},
    #         {"name": "review_time", "value": "doneReview.modified_ts", "aggregate": "minimum"},
    #         {"name": "review_result", "value": "doneReview.review_result", "aggregate": "minimum"},
    #         {"name": "product", "value": "coalesce(doneReview.product, product)", "aggregate": "minimum"},
    #         {"name": "component", "value": "coalesce(doneReview.component, component)", "aggregate": "minimum"},
    #         # {"name": "keywords", "value": "(coalesce(keywords, '')+' '+ETL.parseWhiteBoard(whiteboard)).trim()+' '+flags", "aggregate": "one"},
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

    with Timer("match starts and ends for block {{start}}", {"start":min(*bugs)}):
        reviews = []
        ends = Index(data=ends, keys=["bug_id", "attach_id", "request_type", "reviewer"])

        for g, s in jx.groupby(starts, ["bug_id", "attach_id", "request_type", "reviewer"]):
            start_candidates = jx.sort(s, {"value": "request_time", "sort": 1})
            end_candidates = jx.sort(ends[g], {"value": "modified_ts", "sort": 1})

            #ZIP, BUT WITH ADDED CONSTRAINT s.modified_ts<=e.modified_ts
            if len(start_candidates) > 1:
                Log.note("many reviews on one attachment")
            ei = 0
            for i, s in enumerate(start_candidates):
                while ei < len(end_candidates) and end_candidates[ei].modified_ts < coalesce(s.request_time, datetime2milli(Date.MAX)):
                    ei += 1
                e = end_candidates[ei]

                s.review_time = e.modified_ts
                s.review_duration = e.modified_ts - s.request_time
                s.review_result = e.review_result
                s.review_end_reason = e.review_end_reason
                s.product = coalesce(e.product, s.product)
                s.component = coalesce(e.component, s.component)
                s.requester_review_num = -1
                ei += 1

                if s.bug_status == 'closed' and e.review_end_reason == 'closed':
                    #reviews on closed bugs are ignored
                    continue
                reviews.append(s)

        jx.run({
            "from": reviews,
            "window": [{
                "name": "is_first",
                "value": "rownum == 0",
                "edges": ["bug_id"],
                "sort": ["request_time"],
                "aggregate": "none"
            }]
        })

    with Timer("add {{num}} reviews to ES for block {{start}}", {"start": min(*bugs), "num": len(reviews)}):
        sink.extend({"json": value2json(r)} for r in reviews)


def main():
    settings = startup.read_settings(defs={
       "name": ["--restart", "--reset", "--redo"],
       "help": "force a reprocessing of all data",
       "action": "store_true",
       "dest": "restart"
    })
    Log.start(settings.debug)

    try:
        with startup.SingleInstance(flavor_id=settings.args.filename):
            if settings.args.restart:
                reviews = Cluster(settings.destination).create_index(settings.destination)
            else:
                reviews = Cluster(settings.destination).get_proto(settings.destination)

            bugs = Cluster(settings.source).get_index(settings.source)

            esq = FromES(settings.source)
            es_sample = esq.query({
                "from": "private_bugs",
                "select": [
                    {"name": "max_bug", "value": "bug_id", "aggregate": "maximum"},
                    {"name": "min_bug", "value": "bug_id", "aggregate": "minimum"}
                ]
            })
            batch_size = coalesce(bugs.settings.batch_size, settings.size, 1000)
            threads = coalesce(settings.threads, 4)
            Log.note("Starting at bug {{num}}", num=settings.min_bug)
            min_bug = int(coalesce(settings.min_bug, 0))
            max_bug = int(coalesce(settings.max_bug, Math.max(es_sample.min_bug.value + batch_size * threads, es_sample.max_bug.value)))


            Log.note("bugs from {{min}} to {{max}}, step {{step}}", {
                "min": min_bug,
                "max": max_bug,
                "step": batch_size
            })
            work_queue = Queue("review blocks", max=1*1000*1000, silent=True)

            def worker(please_stop):
                while not please_stop:
                    bugs = work_queue.pop()
                    full_etl(settings, reviews,  bugs)

            live_threads = [
                Thread.run(
                    text_type(n),
                    worker
                )
                for n in range(threads)
            ]

            for bugs in reversed([range(s, e) for s, e in jx.intervals(min_bug, max_bug, size=1000)]):
                work_queue.add(bugs)
            work_queue.add(THREAD_STOP)


            for t in live_threads:
                t.join()

            if settings.args.restart:
                reviews.add_alias()
                reviews.delete_all_but_self()
    finally:
        Log.stop()


if __name__ == '__main__':
    main()
