from datetime import timedelta, datetime
from MoDevETL.util.cnv import CNV
from MoDevETL.util.collections import MAX
from MoDevETL.util.collections.queue import Queue
from MoDevETL.util.collections.relation import Relation
from MoDevETL.util.env import startup
from MoDevETL.util.env.elasticsearch import ElasticSearch
from MoDevETL.util.env.logs import Log
from MoDevETL.util.maths import Math
from MoDevETL.util.queries import Q
from MoDevETL.util.queries.es_query import ESQuery
from MoDevETL.util.struct import Struct, nvl
from MoDevETL.util.times.timer import Timer


def pull_from_es(settings):
    dest = ElasticSearch(settings.destination)
    aliases = dest.get_aliases()
    if settings.destination.index not in aliases.index:
        dest = ElasticSearch.create_index(settings.destination, CNV.JSON2object(CNV.object2JSON(SCHEMA), paths=True), limit_replicas=True)

    destq = ESQuery(dest)

    parents = Relation()
    children = Relation()
    descendants = Relation()

    max_bug_id = destq.query({
        "from": settings.destination.index,
        "select": {"name": "max_bug_id", "value": "bug_id", "aggregate": "max"}
    }) + 1

    for s, e in Q.intervals(0, nvl(max_bug_id, 0), 10000):
        result = destq.query({
            "from": settings.destination.index,
            "select": "*",
            "where": {"range": {"bug_id": {"gte": s, "lt": e}}}
        })

        for r in result:
            parents.extend(r.bug_id, r.parents)
            children.extend(r.bug_id, r.children)
            descendants.extend(r.bug_id, r.descendants)

    return Struct(
        parents=parents,
        children=children,
        descendants=descendants
    )


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
            "parents": p,
            "children": c,
            "descendants": d
        }})

    dest = ElasticSearch(settings.destination)
    for g, r in Q.groupby(records, size=1000):
        dest.extend(r)


def full_etl(settings):
    source = ElasticSearch(settings.source)
    sourceq = ESQuery(source)

    data = pull_from_es(settings)

    max_bug_id = sourceq.query({
        "from": settings.source.alias,
        "select": {"name": "max_bug_id", "value": "bug_id", "aggregate": "max"}
    }) + 1

    min_bug_id = MAX(Math.floor(nvl(MAX(data.children.domain()), 0), 10000), 0)

    #FIRST, GET ALL MISSING BUGS
    for s, e in Q.intervals(min_bug_id, nvl(max_bug_id, 0), 10000):
        with Timer("pull {{start}}..{{end}} from ES", {"start": s, "end": e}):
            children = sourceq.query({
                "from": settings.source.alias,
                "select": ["bug_id", "dependson"],
                "where": {"and": [
                    {"range": {"bug_id": {"gte": s, "lt": e}}},
                    {"exists": {"field": "dependson"}}
                ]}
            })

        dirty = set()
        with Timer("fixpoint work"):
            to_fix_point(children, data, dirty)

        Log.note("{{num}} new records to ES", {"num": len(dirty)})
        push_to_es(settings, data, dirty)

    #PROCESS RECENT CHANGES
    with Timer("pull recent dependancies from ES"):
        children = sourceq.query({
            "from": settings.source.alias,
            "select": ["bug_id", "dependson"],
            "where": {"and": [
                {"range": {"modified_ts": {"gte": CNV.datetime2milli(datetime.utcnow() - timedelta(days=7))}}},
                {"exists": {"field": "dependson"}}
            ]}
        })

    dirty = set()
    with Timer("fixpoint work"):
        to_fix_point(children, data, dirty)

    Log.note("{{num}} new records to ES", {"num": len(dirty)})
    push_to_es(settings, data, dirty)


def to_fix_point(children, data, dirty):
    """
    GIVEN A GRAPH FIND THE TRANSITIVE CLOSURE OF THE parent -> child RELATIONS
    """

    all_parents = data.parents
    all_children = data.children
    all_descendants = data.descendants
    work_queue = Queue()

    for r in children:
        p = r.bug_id
        childs = r.dependson
        work_queue.add(p)
        for c in childs:
            all_parents.add(c, p)
            all_children.add(p, c)

    while work_queue:
        p = work_queue.pop()
        # Log.note("work queue {{length}}", {"length":len(work_queue)})
        is_dirty = False
        for c in all_children[p]:
            # Log.note("work on {{parent}}->{{child}}", {"parent":p, "child":c})
            if all_descendants.testAndAdd(p, c):
                is_dirty = True
            for d in all_descendants[c]:
                if all_descendants.testAndAdd(p, d):
                    is_dirty = True
        if is_dirty:
            dirty.add(p)
            work_queue.extend(all_parents[p])


def main():
    settings = startup.read_settings()
    Log.start(settings.debug)
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
        "hierarchy": {
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
        }
    }
}

if __name__ == '__main__':
    main()
