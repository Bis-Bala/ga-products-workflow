#!/usr/bin/env bash

#PBS -N gdal_merge 
#PBS -P u46
#PBS -q express
#PBS -l walltime=01:00:00,ncpus=5,mem=10GB
#PBS -l wd

DIR="/g/data2/v10/ARG25-tidal-analysis/datasource"
IDIR="/g/data2/v10/ARG25-tidal-analysis/datasource/NDWI_"
ODIR="/g/data/v10/ARG25-tidal-analysis/datasource/sourcefiles"


module load gdal/1.9.2 
echo directory is $DIR
cd $DIR

while read line
do
     echo reading $line
	 TILE=$line
     for cnt in {10..100..10}
     do gdal_merge.py -separate -ot Float32 -o $ODIR/NDWI_"$TILE"_MED_SD_tide_"$cnt".tif "$IDIR""$cnt"/ARG25_"$TILE"*PERCENTILE_50.tif "$IDIR""$cnt"/ARG25_"$TILE"*STANDARD_DEVIATION.tif "$IDIR""$cnt"/TidalExtra_"$TILE"* & 
     done


done < /g/data2/v10/ARG25-tidal-analysis/datasource/bb_test_tiles

echo "Waiting for jobs to finish..."
wait

echo "done"

