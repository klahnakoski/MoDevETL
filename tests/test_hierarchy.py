import unittest
from MoDevETL.hierarchy import to_fix_point
from MoDevETL.util.cnv import CNV
from MoDevETL.util.env.elasticsearch import ElasticSearch
from MoDevETL.util.env.files import File
from MoDevETL.util.env.logs import Log
from MoDevETL.util.queries.es_query import ESQuery


class TestETL(unittest.TestCase):
    def setUp(self):
        self.settings = CNV.JSON2object(File("settings.json").read())
        Log.start(self.settings.debug)

    def tearDown(self):
        Log.stop()

    def test_single_add(self):
        source = ElasticSearch(self.settings.source)
        sourceq = ESQuery(source)

        dest = ElasticSearch(self.settings.destination)
        destq = ESQuery(dest)

        children = sourceq.query({
            "from": self.settings.source.alias,
            "select": ["bug_id", "dependson"],
            "where": {"and": [
                {"term": {"bug_id": 961592}},
                {"exists": {"field": "dependson"}}
            ]}
        })

        to_fix_point(self.settings, destq, children)
