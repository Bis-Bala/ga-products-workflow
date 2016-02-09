#!/bin/bash
cnt=0
rec=0

while true
do
	let cnt=$cnt+1
	let rec=$rec+50
        echo rec is $rec
        echo count is $cnt
	head -n$rec cells.txt|tail -n50|awk -F"," '{printf "%s,", $1}END{printf "\n"}'|sed -e 's/[,]$//g'|awk '{print "xcells = "$0}' > cell_$cnt.txt
	head -n$rec cells.txt|tail -n50|awk -F"," '{printf "%s,", $2}END{printf "\n"}'|sed -e 's/[,]$//g'|awk '{print "ycells = "$0}' >> cell_$cnt.txt
        if [ $rec -gt 1000 ] ; then
		exit
	fi
done
