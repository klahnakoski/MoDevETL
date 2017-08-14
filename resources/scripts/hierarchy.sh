cd ~/hierarchy
export PYTHONPATH=.
python MoDevETL/hierarchy.py --settings=resources/settings/prod/hierarchy_private.json
python MoDevETL/hierarchy.py --settings=resources/settings/prod/hierarchy_public.json
