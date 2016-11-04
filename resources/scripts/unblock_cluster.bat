curl -XPUT http://elasticsearch-private.bugs.scl3.mozilla.com:9200/_cluster/settings -d "{\"persistent\":{\"cluster\":{\"blocks\":{\"read_only\":false}}}}"
