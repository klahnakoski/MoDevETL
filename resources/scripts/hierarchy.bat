set PYTHONPATH=.
pypy MoDevETL\hierarchy.py --settings=resources/settings/hierarchy_public.json
python MoDevETL\org_chart.py --settings=resources/settings/org_chart.json
