#!/bin/bash

trim=0
adj=2
echo "$1"

python scripts/compute_clusters.py $1 $2 $3 $4 $5 | awk '{printf "%s\\n", $0}' | sed 's/'\''/"/g' | sed 's/\\n$//' >| out.json
len=$(($(cat out.json | wc -c)-2))
len2=$((len-$trim+$adj))
echo "$len"
cat out.json | sed 's/.*clusterCounts/\{"clusterCounts/' >| "$6"
rm out.json
