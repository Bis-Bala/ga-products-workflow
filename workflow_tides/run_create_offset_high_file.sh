#!/bin/bash
INPUTDIR="/g/data2/v10/ARG25-tidal-analysis/test/bb_high"

cd $INPUTDIR
rm final_*
	for i in $(ls datafile*); do LOW=$(cat $i|tail -n1|awk -F"," '{print $3}');HIGH=$(cat $i|head -n1|awk -F"," '{print $3}'); cat $i|sort -t"," -k1,1|awk -F"," -v ll=$LOW -v hh=$HIGH '{print $1}' > aa; echo $LOW > final_$i; echo $HIGH >> final_$i;cat aa >> final_$i ;done
