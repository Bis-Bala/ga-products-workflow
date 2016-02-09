#!/usr/bin/env bash

#PBS -N tidal_anal
#PBS -P v10
#PBS -q express 
#PBS -l walltime=02:00:00,ncpus=16,mem=32GB
#PBS -l wd

#module load agdc-api????

DIR=/g/data2/v10/ARG25-tidal-analysis/test/stats_2
CURR_YEAR=`date +%Y`
#EXEC_PATH=$(which band_statistics_tidal.py)
EXEC_PATH="$HOME/agdc/api/source/main/python/datacube/api/workflow_tides/band_statistics_tidal.py"
#node_count=$(wc -l cell_fin.txt|cut -d" " -f1)

        export GDAL_CACHEMAX=1073741824 
        export GDAL_SWATH_SIZE=1073741824 
        export GDAL_DISABLE_READDIR_ON_OPEN=TRUE 
        source /projects/u46/venvs/agdc/bin/activate 
	python $EXEC_PATH  --acq-min 1995-01-01 --acq-max 2015-12-31 --tidal_workflow --season CALENDAR_YEAR --x-min $xmin --x-max $xmax --y-min $ymin --y-max $ymax --statistic MEDIAN  --chunk-size-x 4000 --chunk-size-y 100 --epoch 21 21 --workers 16 --file-per-statistic --satellite LS5 LS7 LS8 --mask-pqa-apply --dataset-type ARG25 --band BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 SHORT_WAVE_INFRARED_2  --output-directory $DIR &

echo "Waiting for jobs to finish..."
wait

echo "done"

