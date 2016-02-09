#!/usr/bin/env python

from datetime import date
from os.path import join as pjoin
import gdal
import luigi
import numpy
import numexpr
import sys
from enum import Enum

import logging
from datacube.api.model import DatasetType, Fc25Bands, Ls57Arg25Bands, Ls8Arg25Bands
from datacube.api.model import Pq25Bands
from datacube.api.model import Satellite
from datacube.api.query import list_tiles_as_list
from datacube.config import Config
from datacube.api.utils import get_dataset_metadata, get_dataset_data_stack
from datacube.api.utils import  get_mask_pqa, get_dataset_type_ndv
from datacube.api.utils import NDV, calculate_ndvi#, date_to_integer
from datacube.api.utils import get_dataset_data, get_mask_wofs
from eotools.tiling import generate_tiles
from eotools.tiling import TiledOutput
from eotools.geobox import GriddedGeoBox
from eotools.pq_utils import extract_pq_flags
from eotools.pq_utils import pq_apply_dict

_log = logging.getLogger()


class TidalProd(Enum):
    __order__ = "BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 SHORT_WAVE_INFRARED_2 CNT LOW_OFFSET HIGH_OFFSET"
    BLUE = 1
    GREEN = 2
    RED  = 3
    NEAR_INFRARED = 4
    SHORT_WAVE_INFRARED_1 = 5
    SHORT_WAVE_INFRARED_2 = 6
    CNT  = 7
    LOW_OFFSET  = 8
    HIGH_OFFSET  = 9

class extraInfo(Enum):
    __order__="CNT LOW_OFFSET HIGH_OFFSET"
   
    CNT = 1
    LOW_OFFSET = 2
    HIGH_OFFSET = 3

def tidal_workflow(tiles, percentile=10, xtile=None, ytile=None, low_off=0, high_off=0,
                out_fnames=None):
    """
    A baseline workflow for doing the baresoil percentile, NBAR, FC
    corresponding mosaics.
    """
    # Get some basic image info
    ds_type = DatasetType.ARG25
    ds = tiles[0]
    dataset = ds.datasets[ds_type]
    md = get_dataset_metadata(dataset)
    _log.info("low and high offset %s , %s ", low_off, high_off) 
    if md is None:
        _log.info("Tile path not exists %s",dataset.path)
        return
    samples, lines = md.shape
    #_log.info("dataset shape %s for %s", md.shape, out_fnames)
    time_slices = len(tiles)
    _log.info("length of time slices [%d] for %s", time_slices, out_fnames)
    geobox = GriddedGeoBox.from_gdal_dataset(gdal.Open(dataset.path))
    lat_lon = ""
    for line in out_fnames:
        lat_lon = line.split("/")[-2]
        break;
    # Initialise the tiling scheme for processing                                  
    if xtile is None:                                                             
        xtile = samples                                                              
    if ytile is None:                                                             
        ytile = lines
    chunks = generate_tiles(samples, lines, xtile=samples, ytile=ytile,
                            generator=False)

    # Define no-data
    no_data_value = NDV
    nan = numpy.float32(numpy.nan) # for the FC dtype no need for float64

    # Define the output files
    if out_fnames is None:
        nbar_outfname = 'nbar_best_pixel'
    else:
        nbar_outfname = out_fnames[0]

    #nbar_outnb = len(TidalProd)
    nbar_outnb = len(extraInfo)
    #fc_outnb = len(Fc25Bands)
    out_dtype = gdal.GDT_Int16
    #_log.info("input xtile [%d] ytile [%d] for %s", xtile, ytile, out_fnames)
    nbar_outds = TiledOutput(nbar_outfname, samples=samples, lines=lines,
                             bands=nbar_outnb, dtype=out_dtype,
                             nodata=no_data_value, geobox=geobox, fmt="GTiff")

    satellite_code = {Satellite.LS5: 5, Satellite.LS7: 7, Satellite.LS8: 8}
    count=0

    # Loop over each spatial tile/chunk and build up the time series
    for chunk in chunks:
        count=0
        ys, ye = chunk[0]
        xs, xe = chunk[1]
        ysize = ye - ys
        xsize = xe - xs
        dims = (time_slices, ysize, xsize)

	#_log.info("got chunk  [%s] for %s", chunk, out_fnames)
        # Initialise the intermediate and best_pixel output arrays
        data = {}
        median_nbar = {}
        stack_tidal = numpy.zeros(dims, dtype='float32')
        stack_lowOff = numpy.zeros(dims, dtype='int16')
        stack_highOff = numpy.zeros(dims, dtype='int16')
        stack_count = numpy.zeros(dims, dtype='int16')

        median_lowOff = numpy.zeros((ysize, xsize), dtype='int16')
        median_highOff = numpy.zeros((ysize, xsize), dtype='int16')
        median_count = numpy.zeros((ysize, xsize), dtype='int16')
        median_lowOff.fill(no_data_value)
        median_highOff.fill(no_data_value)
        median_count.fill(no_data_value)
        stack_nbar = {}
        #_log.info("all initialised successfully")
        for band in Ls57Arg25Bands:
            stack_nbar[band] = numpy.zeros(dims, dtype='int16')
            median_nbar[band] = numpy.zeros((ysize, xsize), dtype='int16')
            median_nbar[band].fill(no_data_value)

        for idx, ds in enumerate(tiles):

            pqa = ds.datasets[DatasetType.PQ25]
            nbar = ds.datasets[DatasetType.ARG25]
            mask = get_mask_pqa(pqa, x=xs, y=ys, x_size=xsize, y_size=ysize)

            # NBAR
            data[DatasetType.ARG25] = get_dataset_data(nbar, x=xs, y=ys,
                                                       x_size=xsize,
                                                       y_size=ysize)
            #mask |= numexpr.evaluate("(bare_soil < 0) | (bare_soil > 8000)")A
            errcnt=0
            # apply the mask to each dataset and insert into the 3D array
	    if satellite_code[nbar.satellite] == 8:
                for band in Ls57Arg25Bands:
		    for oband in Ls8Arg25Bands:
                        try:
                            if oband.name == band.name: 
	                        data[DatasetType.ARG25][oband][mask] = no_data_value
        	                stack_nbar[band][idx] = data[DatasetType.ARG25][oband]
			        break
                        except ValueError:
                            errcnt=1
                            _log.info("Data converting error LS8")
                        except IOError:
                            errcnt=1
                            _log.info("reading error LS8")
                        except KeyError:
                            errcnt=1
                            _log.info("Key error LS8")
                        except:
                            errcnt=1
                            _log.info("Unexpected error for LS8: %s",sys.exc_info()[0])

	    else:
                 for band in Ls57Arg25Bands:
                     try:
                         data[DatasetType.ARG25][band][mask] = no_data_value
                         stack_nbar[band][idx] = data[DatasetType.ARG25][band]
                     except ValueError:
                         errcnt=1
                         _log.info("Data converting error LS57")
                     except IOError:
                         errcnt=1
                         _log.info("NBAR reading error LS57")
                     except KeyError:
                         errcnt=1
                         _log.info("Key error LS57")
                     except:
                         errcnt=1
                         _log.info("Unexpected error LS57: %s",sys.exc_info()[0])

            if errcnt != 0:
                if errcnt == 1:
                    _log.info("nbar tile has problem  %s",nbar.path)
		errcnt=0
                continue

            # Add bare soil, satellite and date to the 3D arrays
            try:
                #_log.info("bare soil for %s %s",bare_soil, out_fnames)
                low=int(float(low_off) * 100)
                high = int(float(high_off) * 100)
                stack_lowOff[idx][:] = low
                stack_highOff[idx][:] = high
                #_log.info("count observed  [%d] on %d", count, dtime)

                count1 = int(numpy.ma.count(numpy.ma.masked_less(stack_nbar, 1)))
                if count1 < 1 :
                    _log.info("no data present on %d and year %d for tile %s reducing count by one", mtime, dtime, lat_lon )
                else:
                    count=count+1 
                stack_count[idx][:] = count

            except:
                _log.info("stacking - Unexpected error: %s",sys.exc_info()[0])

        # Loop over each time slice and generate a mosaic for each dataset_type
        _log.info("checking - flow path: ")
        ndv = get_dataset_type_ndv(DatasetType.ARG25)
        try:
            _log.info("ndv is %s", ndv)
            for idx in range(time_slices):
                median_count = stack_count[idx]
                median_lowOff = stack_lowOff[idx]
                median_highOff = stack_highOff[idx]
            _log.info("ccccc_data  ")
            for band in TidalProd:
                bn = band.value
                if bn == 1:
		    nbar_outds.write_tile(median_count, chunk, raster_band=bn)
	        elif bn == 2:
                    nbar_outds.write_tile(median_lowOff, chunk, raster_band=bn)
	        elif bn == 3:
                    nbar_outds.write_tile(median_highOff, chunk, raster_band=bn)
	except ValueError:
            _log.info("Data converting final error")
        except IOError:
            _log.info("writing error LS57")
        except KeyError:
            _log.info("Key error final")
        except:
            _log.info("Final Unexpected error: %s",sys.exc_info()[0])    
        _log.info("total dataset counts for each chunk is %d for tile %s", count, lat_lon)

    # Close the output files
    nbar_outds.close()

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    config = Config()
    satellites = [Satellite(i) for i in ['LS5', 'LS7', 'LS8']]
    min_date = date(1995, 01, 01)
    max_date = date(2015, 12, 31)
    ds_type = DatasetType.ARG25

    x_cell = [146]
    y_cell = [-34]

    tiles = list_tiles_as_list(x=x_cell, y=y_cell, acq_min=min_date,
                               acq_max=max_date,
                               satellites=satellites,
                               datasets=ds_type,
                               database=config.get_db_database(),
                               user=config.get_db_username(),
                               password=config.get_db_password(),
                               host=config.get_db_host(),
                               port=config.get_db_port())

    #bs_workflow(tiles, outdir='/g/data/v10/testing_ground/jps547/percentiles')
    #bs_workflow(tiles, outdir='/g/data/v10/testing_ground/jps547/percentiles/pct_95', percentile=95)
    tidal_workflow(tiles, outdir='/g/data2/v10/ARG25-tidal-analysis/test', percentile=10)
