#!/bin/bash
cd /home/547/bxb547/agdc/api/source/main/python/datacube/api/workflow_baresoil
source load_modules
cnt=0
while true
do
	let cnt=$cnt+1
        #newdir="/g/data2/v10/ARG25-minerals/product-data-fin/all_data/bb$cnt"
        #newdir="/g/data/u46/PRODUCTS_WORKFLOW/ARG25_FINAL/bb$cnt"
        newdir="/g/data2/v10/ARG25-minerals/test_CF_100/bb$cnt"
  	echo count now $cnt and new directory is $newdir 
        
        #less config.cfg|egrep -v "^xcells|ycells" > tmp_config.cfg
	less config.cfg|egrep -v "^xcells|ycells"|awk -v cc=$newdir '{ if ($0 ~ "^output_dir") {print "output_directory = "cc} else {print $0}}' > tmp_config.cfg
	cat cell_$cnt.txt >> tmp_config.cfg
	cp tmp_config.cfg config.cfg
        cp config.cfg config.cfg.666$cnt
        #source load_modules 
        MODULEPATH=/home/547/bxb547/agdc/api/source/main/python/datacube/api/workflow_baresoil:$MODULEPATH 
        python query.py
        # max = 21
	if [ $cnt -eq 21 ] ; then
  		exit
	fi
        echo sleeping for 15 secs 
	sleep 15 

done
