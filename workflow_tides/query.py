#!/usr/bin/env python

import luigi

import argparse
from datetime import date
import os
from os.path import join as pjoin, exists, dirname, abspath
import cPickle as pickle
import subprocess
import logging

from datacube.api.query import list_tiles_as_list
from datacube.api.model import DatasetType, Satellite, BANDS
from eotools.tiling import generate_tiles


PBS_DSH = (
"""#!/bin/bash

#PBS -P {project}
#PBS -q {queue}
#PBS -l walltime={walltime},ncpus={ncpus},mem={mem}GB,jobfs=350GB
#PBS -l wd
#PBS -me
#PBS -M {email}

NNODES={nnodes}

for i in $(seq 1 $NNODES); do
   pbsdsh -n $((16 *$i)) -- bash -l -c "{modules} PBS_NNODES=$NNODES PBS_VNODENUM=$i python {pyfile} \
   --tile $[$i - 1]" &
done;
wait
""")


def query_cells(xcells, ycells, satellites, min_date, max_date, dataset_types,
                output_dir):
    """
    Query the DB for each cell.
    Currently the config file for this workflow requires the user to
    specify a rectangular region. I have another workflow that takes a 
    vector file as input.
    """
    base_out_fname = CONFIG.get('outputs', 'query_filename')
    cell_queries = []
    for ycell in ycells:
        for xcell in xcells:
            # create the output directory
            cell_dir = '{}_{}'.format(int(xcell), int(ycell))
            out_cell_dir = pjoin(output_dir, cell_dir)
            if not exists(out_cell_dir):
                os.makedirs(out_cell_dir)
            tiles = list_tiles_as_list(x=[xcell], y=[ycell],
                                       acq_min=min_date,
                                       acq_max=max_date,
                                       dataset_types=dataset_types,
                                       satellites=satellites)
            out_fname = pjoin(out_cell_dir, base_out_fname)
            cell_queries.append(out_fname)
            with open(out_fname, 'w') as outf:
                pickle.dump(tiles, outf)

    return cell_queries

def load_filterfile(xcell, ycell):
    fname="/g/data2/v10/ARG25-tidal-analysis/test/bb_high/final_datafile_"+ str(xcell) + "_" +  str(ycell).zfill(4)
    print "loading filter file ", fname 
    try:
        if os.path.exists(fname):
            with open(fname) as f:
                return f.read().splitlines()
    except IOError:
        print "Could not read a filter file"


def query_cells2(xcells, ycells, satellites, min_date, max_date, dataset_types,
                output_dir):
    """
    Query the DB for each cell.
    Currently the config file for this workflow requires the user to
    specify a rectangular region. I have another workflow that takes a 
    vector file as input.
    """
    base_out_fname = CONFIG.get('outputs', 'query_filename')
    cell_queries = []
    for i in range(len(ycells)):
        ycell = ycells[i]
        xcell = xcells[i]
        # create the output directory
        #cell_dir = '{}_{}'.format(int(xcell), int(ycell))
        cell_dir = str(xcell) + "_" + str(ycell).zfill(4)
        out_cell_dir = pjoin(output_dir, cell_dir)
        if not exists(out_cell_dir):
            os.makedirs(out_cell_dir)
        tiles = list_tiles_as_list(x=[xcell], y=[ycell],
                                   acq_min=min_date,
                                   acq_max=max_date,
                                   dataset_types=dataset_types,
                                   satellites=satellites)
        fileTile = [ ]
        lines = load_filterfile(xcell, ycell)
        cnt=0
        print "\tlength of original tiles is ", len(tiles)
        for tile in tiles:
            #import pdb; pdb.set_trace()
            cnt=cnt+1
            dataset = tile.datasets[DatasetType.ARG25]
            tdate= str(tile.end_datetime.strftime("%Y-%m-%d"))
            if tdate in lines:
                fileTile.append(tile)
        out_fname = pjoin(out_cell_dir, base_out_fname)
        cell_queries.append(out_fname)
        with open(out_fname, 'w') as outf:
            pickle.dump(fileTile, outf)
        print "\tlength of new filtered tiles is %d", len(fileTile)

    return cell_queries


if __name__ == '__main__':
    desc = "Queries the AGDC for the tidal workflow."
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("--cfg",
                        help=("The config file used to setup "
                              "tidal workflow."))

    args = parser.parse_args()
    cfg = args.cfg

    # Setup the config file
    global CONFIG
    if cfg is None:
        CONFIG = luigi.configuration.get_config()
        CONFIG.add_config_path(pjoin(dirname(__file__), 'config.cfg'))
    else:
        CONFIG = luigi.configuration.get_config()
        CONFIG.add_config_path(cfg)


    # Create the output directory
    out_dir = CONFIG.get('work', 'output_directory')
    if not exists(out_dir):
        os.makedirs(out_dir)

    # setup logging
    log_dir = CONFIG.get('work', 'logs_directory')
    if not exists(log_dir):
        os.makedirs(log_dir)

    logfile = "{log_path}/query_{uname}_{pid}.log"
    logfile = logfile.format(log_path=log_dir, uname=os.uname()[1],
                             pid=os.getpid())
    logging_level = logging.INFO
    logging.basicConfig(filename=logfile, level=logging_level,
                        format=("%(asctime)s: [%(name)s] (%(levelname)s) "
                                "%(message)s "))

    # Define the DatasetTypes
    # TODO have this defined through the config.cfg
    ds_types = [DatasetType.ARG25, DatasetType.PQ25]

    # Get the satellites we wish to query
    satellites = CONFIG.get('db', 'satellites')
    satellites = [Satellite(i) for i in satellites.split(',')]

    # Get the min/max date range to query
    min_date = CONFIG.get('db', 'min_date')
    min_date = [int(i) for i in min_date.split('_')]
    min_date = date(min_date[0], min_date[1], min_date[2])
    max_date = CONFIG.get('db', 'max_date')
    max_date = [int(i) for i in max_date.split('_')]
    max_date = date(max_date[0], max_date[1], max_date[2])

    # Do we use a list a recatangular block of cells
    cell_list = int(CONFIG.get('db', 'cell_list'))
    if cell_list <= 0:
        # Get the min/max cell range to query
        cell_xmin = int(CONFIG.get('db', 'cell_xmin'))
        cell_ymin = int(CONFIG.get('db', 'cell_ymin'))
        cell_xmax = int(CONFIG.get('db', 'cell_xmax'))
        cell_ymax = int(CONFIG.get('db', 'cell_ymax'))
        xcells = range(cell_xmin, cell_xmax, 1)
        ycells = range(cell_ymin, cell_ymax, 1)
    else:
        xcells = CONFIG.get('db', 'xcells')
        xcells = [int(x) for x in xcells.split(',')]
        ycells = CONFIG.get('db', 'ycells')
        ycells = [int(y) for y in ycells.split(',')]
    

    output_dir = CONFIG.get('work', 'output_directory')
    cell_queries = query_cells2(xcells, ycells, satellites, min_date, max_date,
                                ds_types, output_dir)

    queries_fname = pjoin(output_dir, CONFIG.get('outputs', 'queries_list'))
    with open(queries_fname, 'w') as outf:
        pickle.dump(cell_queries, outf)


    # Create the tiles list that contains groups of cells to operate on
    # Each node will have a certain number of cells to work on
    cpnode = int(CONFIG.get('internals', 'cells_per_node'))
    tiles = generate_tiles(len(cell_queries), 1, cpnode, 1, False)
    tiles = [x for y, x in tiles]
    tiles_list_fname = pjoin(out_dir, CONFIG.get('outputs', 'cell_groups'))
    with open(tiles_list_fname, 'w') as out_file:
        pickle.dump(tiles, out_file)


    # Setup the modules to use for the job
    modules = CONFIG.get('pbs', 'modules').split(',')

    modules = ['module load {}; '.format(module) for module in modules]
    modules.insert(0, 'module purge;module use /projects/u46/opt/modules/modulefiles;')
    modules = ''.join(modules)

    # Calculate the job node and cpu requirements
    nnodes = len(tiles)
    ncpus = nnodes * 16
    mem = nnodes * 32

    # Populate the PBS shell script
    project = CONFIG.get('pbs', 'project')
    queue = CONFIG.get('pbs', 'queue')
    walltime = CONFIG.get('pbs', 'walltime')
    email = CONFIG.get('pbs', 'email')
    py_file = abspath(pjoin(dirname(__file__), 'percentile_workflow.py'))
    py_path = abspath(dirname(__file__))
    pbs_job = PBS_DSH.format(project=project, queue=queue,
                             walltime=walltime, ncpus=ncpus, mem=mem,
                             email=email, nnodes=nnodes,
                             modules=modules,
                             pyfile=py_file)

    # Out put the shell script to disk
    pbs_fname = pjoin(out_dir, CONFIG.get('outputs', 'pbs_filename'))
    with open(pbs_fname, 'w') as out_file:
        out_file.write(pbs_job)

    subprocess.check_call(['qsub', pbs_fname])
