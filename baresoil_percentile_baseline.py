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
from datacube.api.utils import get_dataset_metadata
from datacube.api.utils import  get_mask_pqa
from datacube.api.utils import NDV, calculate_ndvi#, date_to_integer
from datacube.api.utils import get_dataset_data, get_mask_wofs
from eotools.tiling import generate_tiles
from eotools.tiling import TiledOutput
from eotools.geobox import GriddedGeoBox
from eotools.pq_utils import extract_pq_flags
from eotools.pq_utils import pq_apply_dict

_log = logging.getLogger()


class BareSoil(Enum):
    __order__ = "BARE_SOIL PHOTOSYNTHETIC_VEGETATION NON_PHOTOSYNTHETIC_VEGETATION UNMIXING_ERROR BLUE GREEN RED NEAR_INFRARED SHORT_WAVE_INFRARED_1 SHORT_WAVE_INFRARED_2 SID YEAR MD CNT"
    BARE_SOIL =	1
    PHOTOSYNTHETIC_VEGETATION = 2
    NON_PHOTOSYNTHETIC_VEGETATION = 3
    UNMIXING_ERROR = 4
    BLUE = 5
    GREEN = 6
    RED  = 7
    NEAR_INFRARED = 8
    SHORT_WAVE_INFRARED_1 = 9
    SHORT_WAVE_INFRARED_2 = 10
    SID  = 11
    YEAR = 12
    MD  = 13
    CNT  = 14

def bs_workflow(tiles, percentile=90, xtile=None, ytile=None,
                out_fnames=None):
    """
    A baseline workflow for doing the baresoil percentile, NBAR, FC
    corresponding mosaics.
    """
    # Get some basic image info
    ds_type = DatasetType.FC25
    ds = tiles[0]
    dataset = ds.datasets[ds_type]
    md = get_dataset_metadata(dataset)
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
        all_outfname = 'all_best_pixel'
        #fc_outfname = 'fc_best_pixel'
        #sat_outfname = 'sat_best_pixel'
        #date_outfnme = 'date_best_pixel'
        #count_outfnme = 'count_best_pixel'
    else:
        nbar_outfname = out_fnames[0]
        all_outfname = out_fnames[1]
        #fc_outfname = out_fnames[1]
        #sat_outfname = out_fnames[2]
        #date_outfnme = out_fnames[3]
        #count_outfnme = out_fnames[4]

    nbar_outnb = len(Ls57Arg25Bands)
    all_outnb = len(BareSoil)
    #fc_outnb = len(Fc25Bands)
    out_dtype = gdal.GDT_Int16
    #_log.info("input xtile [%d] ytile [%d] for %s", xtile, ytile, out_fnames)
    nbar_outds = TiledOutput(nbar_outfname, samples=samples, lines=lines,
                             bands=nbar_outnb, dtype=out_dtype,
                             nodata=no_data_value, geobox=geobox, fmt="GTiff")
    all_outds = TiledOutput(all_outfname, samples=samples, lines=lines,
                           bands=all_outnb, dtype=out_dtype,
                           nodata=no_data_value, geobox=geobox, fmt="GTiff")

    satellite_code = {Satellite.LS5: 5, Satellite.LS7: 7, Satellite.LS8: 8}
    fc_bands_subset = [Fc25Bands.PHOTOSYNTHETIC_VEGETATION,
                       Fc25Bands.NON_PHOTOSYNTHETIC_VEGETATION,
                       Fc25Bands.UNMIXING_ERROR]
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
        best_pixel_nbar = {}
        best_pixel_fc = {}
        stack_bare_soil = numpy.zeros(dims, dtype='float32')
        stack_sat = numpy.zeros(dims, dtype='int16')
        #stack_date = numpy.zeros(dims, dtype='int32')
        stack_year = numpy.zeros(dims, dtype='int16')
        stack_md = numpy.zeros(dims, dtype='int16')
        stack_count = numpy.zeros(dims, dtype='int16')
        best_pixel_satellite = numpy.zeros((ysize, xsize), dtype='int16')
        #best_pixel_date = numpy.zeros((ysize, xsize), dtype='int32')
        best_pixel_year = numpy.zeros((ysize, xsize), dtype='int16')
        best_pixel_md = numpy.zeros((ysize, xsize), dtype='int16')
        best_pixel_count = numpy.zeros((ysize, xsize), dtype='int16')
        best_pixel_satellite.fill(no_data_value)
        #best_pixel_date.fill(no_data_value)
        best_pixel_count.fill(no_data_value)

        stack_nbar = {}
        #_log.info("all initialised successfully")
        for band in Ls57Arg25Bands:
            stack_nbar[band] = numpy.zeros(dims, dtype='int16')
            best_pixel_nbar[band] = numpy.zeros((ysize, xsize),
                                                dtype='int16')
            best_pixel_nbar[band].fill(no_data_value)


        stack_fc = {}
        for band in fc_bands_subset:
            stack_fc[band] = numpy.zeros(dims, dtype='int16')
            best_pixel_fc[band] = numpy.zeros((ysize, xsize),
                                              dtype='int16')
            best_pixel_fc[band].fill(no_data_value)

        for idx, ds in enumerate(tiles):

            pqa = ds.datasets[DatasetType.PQ25]
            nbar = ds.datasets[DatasetType.ARG25]
            fc = ds.datasets[DatasetType.FC25]
            #_log.info("Processing nbar for index %d  ", idx)
            try:
                wofs = ds.datasets[DatasetType.WATER]
            except KeyError:
                print "Missing water for:\n {}".format(ds.end_datetime)
                wofs = None
            # mask = numpy.zeros((ysize, xsize), dtype='bool')
            # TODO update to use the api's version of extract_pq
            #pq_data = get_dataset_data(pqa, x=xs, y=ys, x_size=xsize,
            #                           y_size=ysize)[Pq25Bands.PQ]
            #mask = extract_pq_flags(pq_data, combine=True)
            #mask = ~mask
            mask = get_mask_pqa(pqa, x=xs, y=ys, x_size=xsize, y_size=ysize)

            # WOfS
            if wofs is not None:
                mask = get_mask_wofs(wofs, x=xs, y=ys, x_size=xsize,
                                     y_size=ysize, mask=mask)

            # NBAR
            data[DatasetType.ARG25] = get_dataset_data(nbar, x=xs, y=ys,
                                                       x_size=xsize,
                                                       y_size=ysize)
            # NDVI
            '''
            red = None
            nir = None
	    if satellite_code[fc.satellite] == 8:
		red = data[DatasetType.ARG25][Ls8Arg25Bands.RED]
		nir = data[DatasetType.ARG25][Ls8Arg25Bands.NEAR_INFRARED]
	    else:
	        red = data[DatasetType.ARG25][Ls57Arg25Bands.RED]
                nir = data[DatasetType.ARG25][Ls57Arg25Bands.NEAR_INFRARED]
	    
            ndvi = calculate_ndvi(red, nir)
            ndvi[mask] = no_data_value
            #mask |= numexpr.evaluate("(ndvi < 0.0) | (ndvi > 0.3)")
	    '''
            # FC
            data[DatasetType.FC25] = get_dataset_data(fc, x=xs, y=ys,
                                                      x_size=xsize,
                                                      y_size=ysize)
            bare_soil = data[DatasetType.FC25][Fc25Bands.BARE_SOIL]
            #mask |= numexpr.evaluate("(bare_soil < 0) | (bare_soil > 8000)")
            errcnt=0
            # apply the mask to each dataset and insert into the 3D array
	    if satellite_code[fc.satellite] == 8:
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

            for band in fc_bands_subset:
                try:
                    data[DatasetType.FC25][band][mask] = no_data_value
                    stack_fc[band][idx] = data[DatasetType.FC25][band]
                except ValueError:
                    errcnt=2
                    _log.info("FC Data converting error")
                except IOError:
                    errcnt=2
                    _log.info("FC reading error LS57")
                except KeyError:
                    errcnt=2
                    _log.info("FC Key error")
                except:
                    errcnt=2
                    _log.info("FC Unexpected error: %s",sys.exc_info()[0])

            if errcnt != 0:
                if errcnt == 1:
                    _log.info("nbar tile has problem  %s",nbar.path)
                else:
                    _log.info("fc tile has problem  %s",fc.path)
		errcnt=0
                continue

            # Add bare soil, satellite and date to the 3D arrays
            try:
                #_log.info("bare soil for %s %s",bare_soil, out_fnames)
                stack_bare_soil[idx] = bare_soil
                stack_bare_soil[idx][mask] = nan
                stack_sat[idx][:] = satellite_code[fc.satellite]
                #dtime = int(ds.end_datetime.strftime('%Y%m%d'))
                dtime = int(ds.end_datetime.strftime('%Y'))
                #_log.info("year of acquisition %d",dtime)
                stack_year[idx][:] = dtime
                #stack_date[idx][:] = dtime
                mtime = int(ds.end_datetime.strftime('%m%d'))
                stack_md[idx][:] = mtime
                count = count+1
                #count = int(numpy.ma.count(numpy.ma.masked_less(bare_soil, 1),axis=0)[0])
                #_log.info("count observed  [%d] on %d", count, dtime)
                count1 = int(numpy.ma.count(numpy.ma.masked_less(bare_soil, 1)))
                if count1 < 1 :
                    _log.info("no data present on %d and year %d for tile %s reducing count by one", mtime, dtime, lat_lon )
                    count=count-1 
                stack_count[idx][:] = count

            except:
                _log.info("stacking - Unexpected error: %s",sys.exc_info()[0])
        # Calcualte the percentile
        pct_fc = numpy.nanpercentile(stack_bare_soil, percentile,
                                     axis=0, interpolation='nearest')

        # Loop over each time slice and generate a mosaic for each dataset_type
        try:
            for idx in range(time_slices):
                pct_idx = pct_fc == stack_bare_soil[idx]
                for band in Ls57Arg25Bands:
                    band_data = stack_nbar[band]
                    best_pixel_nbar[band][pct_idx] = band_data[idx][pct_idx]
                for band in fc_bands_subset:
                    band_data = stack_fc[band]
                    best_pixel_fc[band][pct_idx] = band_data[idx][pct_idx]

                best_pixel_satellite[pct_idx] = stack_sat[idx][pct_idx]
                #best_pixel_date[pct_idx] = stack_date[idx][pct_idx]
                best_pixel_year[pct_idx] = stack_year[idx][pct_idx]
                best_pixel_md[pct_idx] = stack_md[idx][pct_idx]
                best_pixel_count[pct_idx] = stack_count[idx][pct_idx]
                #best_pixel_count[pct_idx] = time_slices
            # Output the current spatial chunk for each dataset
            for band in Ls57Arg25Bands:
                bn = band.value
                band_data = best_pixel_nbar[band]
                nbar_outds.write_tile(band_data, chunk, raster_band=bn)
            '''
         for band in fc_bands_subset:
            bn = band.value
            band_data = best_pixel_fc[band]
            fc_outds.write_tile(band_data, chunk, raster_band=bn)
            '''
            for band in BareSoil:
                bn = band.value
                if bn < 5:
                    if bn == 1:
                        all_outds.write_tile(pct_fc, chunk,raster_band=BareSoil.BARE_SOIL.value)
                    for oband in fc_bands_subset:
                        if oband.name == band.name:
                            band_data = best_pixel_fc[oband]
                            all_outds.write_tile(band_data, chunk, raster_band=bn)
                            break
                elif bn < 11:
                    for oband in Ls57Arg25Bands:
                        if oband.name == band.name:
                            band_data = best_pixel_nbar[oband]
                            all_outds.write_tile(band_data, chunk, raster_band=bn)
                            break
                elif bn == 11:
                    all_outds.write_tile(best_pixel_satellite, chunk, raster_band=bn) 		
	        elif bn == 12:
		    all_outds.write_tile(best_pixel_year, chunk, raster_band=bn)
	        elif bn == 13:
                    all_outds.write_tile(best_pixel_md, chunk, raster_band=bn)
	        elif bn == 14:
                    all_outds.write_tile(best_pixel_count, chunk, raster_band=bn)
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
    all_outds.close()

if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    config = Config()
    #satellites = [Satellite(i) for i in ['LS5', 'LS7', 'LS8']]
    satellites = [Satellite(i) for i in ['LS5', 'LS7']]
    min_date = date(1987, 01, 01)
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
    bs_workflow(tiles, outdir='/g/data/v10/testing_ground/jps547/percentiles/test', percentile=95)
