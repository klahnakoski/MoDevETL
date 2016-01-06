set PYTHONPATH=.
pypy MoDevETL\hierarchy.py --settings=resources\settings\prod\hierarchy-private.json
pypy MoDevETL\hierarchy.py --settings=resources\settings\prod\hierarchy-public.json
rem pypy MoDevETL\replicate_dependencies.py --settings=resources\settings\replicate_dependencies_settings.json


