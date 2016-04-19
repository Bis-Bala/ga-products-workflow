#!/usr/bin/env bash

#PBS -N tidal_anal
#PBS -P u46
#PBS -q express 
#PBS -l walltime=00:20:00,ncpus=8,mem=16GB
#PBS -l wd

#module load agdc-api????

DIR=/g/data/v10/ARG25-tidal-analysis/datasource/NDWI_
IFDIR=/g/data2/v10/ARG25-tidal-analysis/datasource/origfile/final_datafile_"$xmin"_"$ymin"_"$count"
echo "output directory is " $DIR$count
echo "input reading file is " $IFDIR
CURR_YEAR=`date +%Y`
#EXEC_PATH=$(which band_statistics_all.py)
EXEC_PATH="$HOME/agdc/api/source/main/python/datacube/api/inter-tides/band_statistics_tidal_variant.py"
#node_count=$(wc -l cell_fin.txt|cut -d" " -f1)

        export GDAL_CACHEMAX=1073741824 
        export GDAL_SWATH_SIZE=1073741824 
        export GDAL_DISABLE_READDIR_ON_OPEN=TRUE 
        source /projects/u46/venvs/agdc/bin/activate 
	python $EXEC_PATH  --acq-min 1987-01-01 --acq-max 2015-12-31 --tidal_workflow --tidal_ifile $IFDIR   --season CALENDAR_YEAR --x-min $xmin --x-max $xmax --y-min $ymin --y-max $ymax --statistic PERCENTILE_50 STANDARD_DEVIATION --interpolation LINEAR  --chunk-size-x 4000 --chunk-size-y 400 --epoch 29 29 --workers 16 --file-per-statistic --satellite LS5 LS7 LS8 --mask-pqa-apply --dataset-type NDWI --band NDWI  --output-directory $DIR$count &

echo "Waiting for jobs to finish..."
wait

echo "done"

