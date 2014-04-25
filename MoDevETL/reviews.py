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

from MoDevETL.util.env import startup
from MoDevETL.util.env.elasticsearch import ElasticSearch
from MoDevETL.util.env.logs import Log
from MoDevETL.util.queries.es_query import ESQuery



def full_etl(settings, bugs):
    es = ElasticSearch(settings.source)
    esq = ESQuery(es)

    esq.query({
        "select": [
            "bug_id",
            {"name": "attach_id", "value": "attachments.attach_id"},
            "modified_ts",
            {"name": "requestee", "value": "attachments.flags.requestee"},
            {"name": "created_by", "value": "attachments.created_by"},
            "product",
            "component"
        ],
        "from":
            "bugs.attachments.flags",
        "where":
            {"and": [
                {"terms": {"attachments.flags.request_status": ["?"]}},
                {"terms": {"attachments.flags.request_type": ["review", "superreview"]}},
                {"script": {"script": "attachments.flags.modified_ts==modified_ts"}},
                {"term": {"attachments.isobsolete": 0}}
            ]},
        "esfilter":
            {"terms": {"bug_id": bugs}}
    })


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)
    try:
        full_etl(settings, range(900000, 1000000))
    finally:
        Log.stop()


if __name__ == '__main__':
    main()
