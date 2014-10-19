set PYTHONPATH=.

start "private1" pypy MoDevETL\reviews.py --settings=resources/settings/private_review_settings_part1.json --reset
start "public1" pypy MoDevETL\reviews.py --settings=resources/settings/public_review_settings_part1.json --reset
timeout 30
start "private2" pypy MoDevETL\reviews.py --settings=resources/settings/private_review_settings_part2.json
start "public2" pypy MoDevETL\reviews.py --settings=resources/settings/public_review_settings_part2.json

