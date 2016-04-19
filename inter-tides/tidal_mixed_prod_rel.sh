#!/usr/bin/env bash

#PBS -N tidal_anal
#PBS -P u46
#PBS -q express
#PBS -l walltime=00:10:00,ncpus=1,mem=2GB
#PBS -l wd

IODIR="/g/data/v10/ARG25-tidal-analysis/datasource/sourcefiles"
cd $HOME/agdc/api/source/main/python/datacube/api/inter-tides
EXEC_PATH="$HOME/agdc/api/source/main/python/datacube/api/inter-tides/tidal_mixed_prod_relative_book.py"
source /projects/u46/venvs/agdc/bin/activate 
	echo my lon and lat are $xmin $ymin
	python $EXEC_PATH --x-min $xmin --x-max $xmax --y-min $ymin --y-max $ymax --acq-min 1987 --acq-max 2015 --output-directory $IODIR --local-scheduler &
        

echo waiting for all process to finish
wait
