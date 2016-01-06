set PYTHONPATH=.
pypy MoDevETL\hierarchy.py --settings=resources\settings\prod\hierarchy_private.json
pypy MoDevETL\hierarchy.py --settings=resources\settings\prod\hierarchy_public.json
rem pypy MoDevETL\replicate_dependencies.py --settings=resources\settings\replicate_dependencies_settings.json


