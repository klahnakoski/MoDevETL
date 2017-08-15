import unittest

from jx_elasticsearch.qb_usingES import FromES
from mo_files import File
from mo_logs import Log
from pyLibrary import convert

from MoDevETL.hierarchy import to_fix_point
from pyLibrary.env.elasticsearch import Index


class TestETL(unittest.TestCase):
    def setUp(self):
        self.settings = convert.json2value(File("settings.json").read())
        Log.start(self.settings.debug)

    def tearDown(self):
        Log.stop()

    def test_single_add(self):
        source = Index(self.settings.source)
        sourceq = FromES(source)

        dest = Index(self.settings.destination)
        destq = FromES(dest)

        children = sourceq.query({
            "from": self.settings.source.alias,
            "select": ["bug_id", "dependson"],
            "where": {"and": [
                {"term": {"bug_id": 961592}},
                {"exists": {"field": "dependson"}}
            ]}
        })

        to_fix_point(self.settings, destq, children)
