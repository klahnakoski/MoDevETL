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

from datetime import timedelta, datetime
from pyLibrary.cnv import CNV
from pyLibrary.collections import MAX
from pyLibrary.collections.relation import Relation
from pyLibrary.env import startup
from pyLibrary.env.elasticsearch import Cluster, Index
from pyLibrary.env.logs import Log
from pyLibrary.queries import Q
from pyLibrary.queries.es_query import ESQuery
from pyLibrary.struct import Struct, nvl
from pyLibrary.times.dates import Date
from pyLibrary.times.timer import Timer


MIN_DEPENDENCY_LIFETIME = 2 * 24 * 60 * 60 * 1000  # less than 2 days of dependency is ignored (mistakes happen)

def pull_from_es(settings, destq, all_parents, all_children, all_descendants, work_queue):
    #LOAD PARENTS FROM ES

    for g, r in Q.groupby(Q.sort(work_queue), size=100):
        result = destq.query({
            "from": settings.destination.index,
            "select": "*",
            "where": {"terms": {"bug_id": r}}
        })
        for r in result:
            all_parents.extend(r.bug_id, r.parents)
            all_children.extend(r.bug_id, r.children)
            all_descendants.extend(r.bug_id, r.descendants)


def push_to_es(settings, data, dirty):
    #PREP RECORDS FOR ES
    records = []
    for bug_id in dirty:
        d = data.descendants[bug_id]
        if not d:
            continue

        p = data.parents[bug_id]
        c = data.children[bug_id]
        records.append({"id": bug_id, "value": {
            "bug_id": bug_id,
            "parents": Q.sort(p),
            "children": Q.sort(c),
            "descendants": Q.sort(d),
            "etl": {"timestamp": Date.now().unix}
        }})

    dest = Index(settings.destination)
    for g, r in Q.groupby(records, size=200):
        with Timer("Push {{num}} records to ES", {"num": len(r)}):
            dest.extend(r)


def full_etl(settings):
    dest = Cluster(settings.destination).get_or_create_index(settings.destination, CNV.JSON2object(CNV.object2JSON(SCHEMA), paths=True), limit_replicas=True)
    destq = ESQuery(dest)
    min_bug_id = destq.query({
        "from": nvl(settings.destination.alias, settings.destination.index),
        "select": {"name": "max_bug_id", "value": "bug_id", "aggregate": "max"}
    })
    min_bug_id = MAX(min_bug_id-1000, 0)
    min_bug_id = 0

    source = Index(settings.source)
    sourceq = ESQuery(source)
    max_bug_id = sourceq.query({
        "from": nvl(settings.source.alias, settings.source.index),
        "select": {"name": "max_bug_id", "value": "bug_id", "aggregate": "max"}
    }) + 1
    max_bug_id = nvl(max_bug_id, 0)

    #FIRST, GET ALL MISSING BUGS
    for s, e in Q.reverse(list(Q.intervals(min_bug_id, max_bug_id, 10000))):
        with Timer("pull {{start}}..{{end}} from ES", {"start": s, "end": e}):
            children = sourceq.query({
                "from": settings.source.alias,
                "select": ["bug_id", "dependson", "blocked", "modified_ts", "expires_on"],
                "where": {"and": [
                    {"range": {"bug_id": {"gte": s, "lt": e}}},
                    {"or": [
                        {"exists": {"field": "dependson"}},
                        {"exists": {"field": "blocked"}}
                    ]}
                ]}
            })

        with Timer("fixpoint work"):
            to_fix_point(settings, destq, children)

    #PROCESS RECENT CHANGES
    with Timer("pull recent dependancies from ES"):
        children = sourceq.query({
            "from": settings.source.alias,
            "select": ["bug_id", "dependson", "blocked"],
            "where": {"and": [
                {"range": {"modified_ts": {"gte": CNV.datetime2milli(datetime.utcnow() - timedelta(days=7))}}},
                {"or": [
                    {"exists": {"field": "dependson"}},
                    {"exists": {"field": "blocked"}}
                ]}
            ]}
        })

    to_fix_point(settings, destq, children)


def to_fix_point(settings, destq, children):
    """
    GIVEN A GRAPH FIND THE TRANSITIVE CLOSURE OF THE parent -> child RELATIONS
    """
    all_parents = Relation()
    all_children = Relation()
    all_descendants = Relation()
    loaded = set()
    load_queue = set()
    dirty = set()

    #LOAD GRAPH
    for r in children:
        me = r.bug_id
        if r.expires_on-r.modified_ts < MIN_DEPENDENCY_LIFETIME:
            continue

        load_queue.add(me)
        childs = r.dependson
        for c in childs:
            load_queue.add(c)
            all_parents.add(c, me)
            all_children.add(me, c)
        parents = r.blocked
        for p in parents:
            load_queue.add(p)
            all_parents.add(me, p)
            all_children.add(p, me)

    while load_queue:
        frontier = load_queue - loaded
        if frontier:
            with Timer("pull {{num}} from ES for the frontier", {"num": len(frontier)}):
                pull_from_es(settings, destq, all_parents, all_children, all_descendants, frontier)
                loaded.update(frontier)

        work_queue = load_queue
        load_queue = set()

        while work_queue:
            next_queue = set()
            with Timer("fixpoint work"):
                for me in work_queue:
                    is_dirty = False
                    for c in all_children[me]:
                        if all_descendants.testAndAdd(me, c):
                            is_dirty = True
                        for d in all_descendants[c]:
                            if all_descendants.testAndAdd(me, d):
                                is_dirty = True
                    if is_dirty:
                        dirty.add(me)
                        load_queue.update(all_parents[me])
                        next_queue.update(all_parents[me])

            work_queue = next_queue

    Log.note("{{num}} new records to ES", {"num": len(dirty)})
    push_to_es(settings, Struct(
        parents=all_parents,
        children=all_children,
        descendants=all_descendants
    ), dirty)


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)
    with startup.SingleInstance(flavor_id=settings.args.filename):
        try:
            full_etl(settings)
        finally:
            Log.stop()


SCHEMA = {
    "settings": {
        "index.number_of_shards": 1,
        "index.number_of_replicas": 2
    },
    "mappings": {
        "bug_hierarchy": {
            "_all": {
                "enabled": False
            },
            "_source": {
                "compress": False,
                "enabled": True
            },
            "_id": {"path": "bug_id"},
            "properties": {
                "bug_id": {
                    "type": "integer",
                    "store": "yes"
                },
                "modified_ts": {
                    "type": "long",
                    "store": "yes"
                },
                "expires_on": {
                    "type": "long",
                    "store": "yes"
                },
                "children": {
                    "type": "integer",
                    "enabled": False,
                    "index": "no",
                    "store": "yes"
                },
                "parents": {
                    "type": "integer",
                    "enabled": False,
                    "index": "no",
                    "store": "yes"
                },
                "descendants": {
                    "type": "integer",
                    "enabled": False,
                    "index": "no",
                    "store": "yes"
                }
            }
        },
        "bug_version": {
            "_all": {
                "enabled": False
            },
            "_source": {
                "compress": False,
                "enabled": True
            },
            "_id": {"path": "bug_id"},
            "properties": {
                "bug_id": {
                    "type": "integer",
                    "store": "yes"
                },
                "modified_ts": {
                    "type": "long",
                    "store": "yes"
                },
                "expires_on": {
                    "type": "long",
                    "store": "yes"
                },
                "depends_on": {
                    "type": "integer",
                    "store": "yes"
                }
            }
        }

    }
}

if __name__ == '__main__':
    main()
