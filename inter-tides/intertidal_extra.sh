#!/usr/bin/env bash

#PBS -N tidal_anal
#PBS -P u46
#PBS -q express 
#PBS -l walltime=00:20:00,ncpus=8,mem=16GB
#PBS -l wd

#module load agdc-api????

IODIR=/g/data/v10/ARG25-tidal-analysis/datasource/sourcefiles
echo "input output dir is " $IODIR
#EXEC_PATH=$(which band_statistics_all.py)
EXEC_PATH="$HOME/agdc/api/source/main/python/datacube/api/inter-tides/intertidal_extra.py"
#node_count=$(wc -l cell_fin.txt|cut -d" " -f1)

        export GDAL_CACHEMAX=1073741824 
        export GDAL_SWATH_SIZE=1073741824 
        export GDAL_DISABLE_READDIR_ON_OPEN=TRUE 
        source /projects/u46/venvs/agdc/bin/activate 
	python $EXEC_PATH --x-min $xmin --x-max $xmin --y-min $ymin --y-max $ymin --acq-min 1987 --acq-max 2015 --output-directory $IODIR --local-scheduler
echo "Waiting for jobs to finish..."
wait

echo "done"

