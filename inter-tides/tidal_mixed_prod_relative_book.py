#!/usr/bin/env python

# ===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
# * Redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither Geoscience Australia nor the names of its contributors may be
#       used to endorse or promote products derived from this software
#       without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# ===============================================================================


__author__ = "u81051"


import logging
import luigi
import numpy
import os
import osr
import fnmatch 
import gdal
from array import *
from gdalconst import GDT_Int16
from gdalconst import *
from datacube.api.utils import NDV
from datacube.api.utils import raster_create, format_date
from datacube.api.workflow.cell import Workflow, SummaryTask, CellTask


#ODIR='/g/data2/v10/ARG25-tidal-analysis/test/stats_variant'

_log = logging.getLogger()


class TidalImageRelWorkflow(Workflow):

    def __init__(self):

        Workflow.__init__(self, name="Tidal Image Relative Workflow")

    def create_summary_tasks(self):

        return [TidalImageSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                            acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                            output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                            mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask, mask_wofs_apply=self.mask_wofs_apply,
                                            mask_wofs_mask=self.mask_wofs_mask)]


class TidalImageSummaryTask(SummaryTask):

    def create_cell_tasks(self, x, y):

        return TidalWaveCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max,
                                        satellites=self.satellites,
                                        output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                        mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,mask_wofs_apply=self.mask_wofs_apply,
                                        mask_wofs_mask=self.mask_wofs_mask)


class TidalWaveCellTask(CellTask):

    def output(self):

        from datacube.api.workflow import format_date
        from datacube.api.utils import get_satellite_string

        satellites = get_satellite_string(self.satellites)

        acq_min = format_date(self.acq_min)
        acq_max = format_date(self.acq_max)

        filename = os.path.join(self.output_directory,
                                "ARG25_REL_{x:03d}_{y:04d}_{acq_min}_{acq_max}_model_test.tif".format(
                                    x=self.x, y=self.y,
                                    acq_min=acq_min,
                                    acq_max=acq_max))

        return luigi.LocalTarget(filename)

    def generate_raster_metadata(self):
        return {
            "X_INDEX": "{x:03d}".format(x=self.x),
            "Y_INDEX": "{y:04d}".format(y=self.y),
            "DATASET_TYPE": "ARG25",
            "ACQUISITION_DATE": "{acq_min} to {acq_max}".format(acq_min=format_date(self.acq_min), acq_max=format_date(self.acq_max)),
            "SEASON": "Calendar year",
            "SATELLITES": " ".join([s.name for s in self.satellites]),
            "DESCRIPTION": "Tidal Image for each 10 percent of datasets"
        }

    def read_write_multi_images(self):
        newpat = '*'+'NDWI_'+ str(self.x) +'_'+ str(self.y).zfill(4) + '*'
        print 'reading input directory and searching pattern ', self.output_directory , newpat 
        ndwi =None
        newimage=numpy.zeros((4000, 4000), dtype='int16')
        heights = ["tide_10","tide_20","tide_30","tide_40","tide_50","tide_60","tide_70","tide_80","tide_90","tide_100"]
        shape = (4000, 4000)
        band_desc= ["Relative DEM of average tidal offsets for each 10 percent of heights"]
        
        height_offset = dict()
        for height in heights:
            #height_offset[height] = numpy.zeros(shape, dtype=numpy.int16)
            height_offset[height] = 0
       
        _log.info("searching pattern %s" ,   newpat)
        for fl in os.listdir(self.output_directory):
            fpath= self.output_directory + '/' + fl 
            if fnmatch.fnmatch(fpath, newpat):
                _log.info("file matched %s" ,  fl)
                dataset = gdal.Open(fpath, GA_ReadOnly)
                if dataset is None: 
                   print 'No dataset found for ', fl
                   _log.info("dataset problem  %s" ,  fl)
                   continue

                for height in heights:
                    if height in fl:
                        #get the first ndwi band from the source file 
                        band = dataset.GetRasterBand(1)
                        ndwi = band.ReadAsArray(0,0, dataset.RasterXSize, dataset.RasterYSize)
                        mask_nan = numpy.ma.masked_invalid(ndwi)
                        fill = 0
                        mask_nan= numpy.ma.filled(mask_nan, fill)
                        _log.info("shape of ndwi [%s] and data after nan masked %s" ,  mask_nan.shape, mask_nan)
                        #ndwi>0 means water (0) else land(1)
                        if fl.find("tide_100") > 0:
                            if height.find("tide_100") != -1:
                                newimage+=numpy.where(mask_nan >= 0, 0,1)
                        else:
                            newimage+=numpy.where(mask_nan >= 0, 0, 1)
                         
                        _log.info("shape of newimage [%s] and data %s" ,  newimage.shape, newimage)
       
        metadata_info = self.generate_raster_metadata()
        _log.info("new binary data %s" ,  newimage)
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        projection = srs.ExportToWkt()
        transform = (self.x, 0.00025, 0.0, self.y+1, 0.0, -0.00025)
        raster_create(self.output().path, [newimage], transform, projection, NDV, GDT_Int16, dataset_metadata=metadata_info, band_ids=band_desc)

    def run(self):

        _log.info("loading multi images")
        self.read_write_multi_images()
       

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    TidalImageRelWorkflow().run() 
