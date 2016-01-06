set PYTHONPATH=.
pypy MoDevETL\hierarchy.py --settings=public-hierarchy-settings.json
pypy MoDevETL\hierarchy.py --settings=private-hierarchy-settings.json
pypy MoDevETL\replicate_dependencies.py --settings=public_replicate_dependencies_settings.json


