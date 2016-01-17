#!/usr/bin/env python

import luigi

import os
from os.path import join as pjoin, exists, dirname
import cPickle as pickle
import glob
import argparse
import logging

from datacube.api.model import DatasetType
from baresoil_percentile_baseline import bs_workflow


CONFIG = luigi.configuration.get_config()
CONFIG.add_config_path(pjoin(dirname(__file__), 'config.cfg'))


class PercentileTask(luigi.Task):

    """
    Computes the percentile task for a given cell.
    """

    query_fname = luigi.Parameter()
    pct = luigi.IntParameter()
    output_dir = luigi.Parameter()
    lat_lon = luigi.Parameter()
    start_date = luigi.Parameter()
    end_date = luigi.Parameter()

    def requires(self):
        return []

    def output(self):
        basename = pjoin(self.output_dir, 'percentile_{}.completed')
        out_fname = basename.format(self.pct)
        return luigi.LocalTarget(out_fname)

    def run(self):
        data_groups = CONFIG.get('outputs', 'groups').split(',')
        base_fname_format = pjoin(self.output_dir,
                                  CONFIG.get('outputs', 'basefname_format'))
        out_fnames = []
        for group in data_groups:
            out_fnames.append(base_fname_format.format(group=group,
                                                       pct=self.pct, lat_lon=self.lat_lon, start_date=self.start_date, end_date=self.end_date))

        # Load the db query
        with open(self.query_fname, 'r') as f:
            db_tiles = pickle.load(f)

        # Get the processing tile sizes                                            
        xtile = int(CONFIG.get('internals', 'x_tile_size'))                            
        ytile = int(CONFIG.get('internals', 'y_tile_size'))                            
        xtile = None if xtile <= 0 else xtile                                   
        ytile = None if ytile <= 0 else ytile

        # Execute
        bs_workflow(db_tiles, percentile=self.pct, xtile=xtile,
                    ytile=ytile, out_fnames=out_fnames)

        with self.output().open('w') as outf:
            outf.write('Complete')


class DefinePercentileTasks(luigi.Task):

    """
    Issues PercentileTask's to each cell associated
    with the tile defined by the start and end index.
    """

    idx1 = luigi.IntParameter()
    idx2 = luigi.IntParameter()

    def requires(self):
        base_out_dir = CONFIG.get('work', 'output_directory')
        queries_fname = pjoin(base_out_dir,
                              CONFIG.get('outputs', 'queries_list'))
        with open(queries_fname, 'r') as infile:
            cell_queries = pickle.load(infile)

        start_date = CONFIG.get('db', 'min_date')
        end_date = CONFIG.get('db', 'max_date')

        percentiles = CONFIG.get('internals', 'percentiles').split(',')
        percentiles = [int(pct) for pct in percentiles]

        tasks = []
        for cell_query in cell_queries[self.idx1:self.idx2]:
            print cell_query
            out_dir = dirname(cell_query)
            lat_lon = out_dir.split("/")[-1]
            for pct in percentiles:
                tasks.append(PercentileTask(cell_query, pct, out_dir, lat_lon, start_date, end_date))

        return tasks

    def output(self):                                                              
        out_dir = CONFIG.get('work', 'output_directory')                           
        out_fname = pjoin(out_dir,                                                 
                          'DefinePercentileTasks{}:{}.completed')              
        out_fname = out_fname.format(self.idx1, self.idx2)                         
                                                                                   
        return luigi.LocalTarget(out_fname)                                        
                                                                                   
    def run(self):                                                                 
        with self.output().open('w') as outf:                                      
            outf.write('Completed')


if __name__ == '__main__':
    # Setup command-line arguments
    desc = "Processes zonal stats for a given set of cells."
    hlp = ("The tile/chunk index to retieve from the tiles list. "
           "(Needs to have been previously computed to a file named tiles.pkl")
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--tile', type=int, help=hlp)
    parsed_args = parser.parse_args()
    tile_idx = parsed_args.tile


    # setup logging
    log_dir = CONFIG.get('work', 'logs_directory')
    if not exists(log_dir):
        os.makedirs(log_dir)

    logfile = "{log_path}/stats_workflow_{uname}_{pid}.log"
    logfile = logfile.format(log_path=log_dir, uname=os.uname()[1],
                             pid=os.getpid())
    logging_level = logging.INFO
    logging.basicConfig(filename=logfile, level=logging_level,
                        format=("%(asctime)s: [%(name)s] (%(levelname)s) "
                                "%(message)s "))


    # Get the list of tiles (groups of cells that each node will operate on
    tiles_list_fname = pjoin(CONFIG.get('work', 'output_directory'),
                             CONFIG.get('outputs', 'cell_groups'))
    with open(tiles_list_fname, 'r') as in_file:
        tiles = pickle.load(in_file)

    # Initialise the job
    tile = tiles[tile_idx]
    tasks = [DefinePercentileTasks(tile[0], tile[1])]
    luigi.build(tasks, local_scheduler=True, workers=16)
    luigi.run()
