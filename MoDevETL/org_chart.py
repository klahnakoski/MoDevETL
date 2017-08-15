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

import requests

from pyLibrary import convert
from mo_logs import startup, constants
from mo_logs.logs import Log
from mo_dots import listwrap
from pyLibrary.env import elasticsearch


def etl():
    url = settings.source.url

    response = requests.get(url, auth=(settings.source.auth.user, settings.source.auth.password))
    people = convert.json2value(convert.utf82unicode(response.content))

    docs = []
    for i, v in enumerate(people):
        emails = set(listwrap(v.emailalias) + listwrap(v.bugzillaemail) + [m.split(" ")[0].lower() for m in listwrap(v.mail)])
        docs.append({"value": {
            "id": v.dn,
            "name": v.cn,
            "manager": v.manager.dn,
            "email": emails
        }})

    Log.note("{{num}} people found", num=len(people))

    cluster = elasticsearch.Cluster(settings.destination)
    olds = [a.index for a in cluster.get_aliases() if a.alias==settings.destination.index]

    # FILL NEW INDEX
    index = cluster.create_index(settings.destination)
    index.add_alias(settings.destination.index)
    index.extend(docs)

    # CLEANUP
    for o in olds:
        cluster.delete(o)



settings = startup.read_settings()
constants.set(settings.constants)
Log.start(settings.debug)
try:
    etl()
finally:
    Log.stop()

