#!/usr/bin/env bash

#PBS -N gdal_merge 
#PBS -P u46
#PBS -q express
#PBS -l walltime=03:00:00,ncpus=16,mem=32GB
#PBS -l wd

DIR="/g/data2/v10/ARG25-tidal-analysis/prod-high-offset"
#DIR="/g/data2/v10/ARG25-tidal-analysis/prod"
ADIR="/g/data2/v10/ARG25-tidal-analysis/test"
STAT=$ADIR/stats_2
COUNT=$ADIR/count/high
module load gdal/1.9.2 
echo directory is $DIR
cd $DIR

while read line
do
     echo reading $line
	 TILE=$line
 gdal_merge.py -separate -ot Int16 -o ARG25_"$TILE"_1995_01_01_2015_12_31_high_observed_tide_10.tif $STAT/ARG25_"$TILE"*BLUE*.tif $STAT/ARG25_"$TILE"*GREEN*.tif $STAT/ARG25_"$TILE"*_RED_*.tif $STAT/ARG25_"$TILE"*NEAR_INFRARED*.tif $STAT/ARG25_"$TILE"*SHORT_WAVE_INFRARED_1*.tif $STAT/ARG25_"$TILE"*SHORT_WAVE_INFRARED_2*.tif $COUNT/bb*/$TILE/ARG25_NBAR* &

done < $ADIR/all_coastal_files

echo "Waiting for jobs to finish..."
wait

echo "done"

