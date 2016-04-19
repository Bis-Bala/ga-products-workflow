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

#This program will create four bands tiff file that has number of observartions, low tide, high tide
#and median tide heights. Data is extracted from database and stored in offset files
#This program loaded those data and created tif files which are to be merged in the final intertidal product

__author__ = "u81051"


import logging
import luigi
import numpy
import os
import osr
import gdal
from gdalconst import GDT_Int16
from datacube.api.utils import NDV, empty_array
from datacube.api.utils import raster_create, format_date
from datacube.api.workflow.cell import Workflow, SummaryTask, CellTask
from enum import Enum

_log = logging.getLogger()


class ExtraBands(Enum):
    __order__ = "OBSERVATIONS, LOW, MEDIAN, HIGH"
    OBSERVATIONS = 1
    LOW = 2
    MEDIAN = 3
    HIGH = 4
    

class Intertidal_extra_Workflow(Workflow):

    def __init__(self):

        Workflow.__init__(self, name="Intertidal Workflow")

    def create_summary_tasks(self):

        return [IntertidalSummaryTask(x_min=self.x_min, x_max=self.x_max, y_min=self.y_min, y_max=self.y_max,
                                            acq_min=self.acq_min, acq_max=self.acq_max, satellites=self.satellites,
                                            output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                            mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask, mask_wofs_apply=self.mask_wofs_apply,
                                            mask_wofs_mask=self.mask_wofs_mask)]


class IntertidalSummaryTask(SummaryTask):

    def create_cell_tasks(self, x, y):

        return IntertidalCellTask(x=x, y=y, acq_min=self.acq_min, acq_max=self.acq_max,
                                        satellites=self.satellites, 
                                        output_directory=self.output_directory, csv=self.csv, dummy=self.dummy,
                                        mask_pqa_apply=self.mask_pqa_apply, mask_pqa_mask=self.mask_pqa_mask,mask_wofs_apply=self.mask_wofs_apply,
                                        mask_wofs_mask=self.mask_wofs_mask)


class IntertidalCellTask(CellTask):

    def output(self):

        from datacube.api.workflow import format_date
        from datacube.api.utils import get_satellite_string

        acq_min = format_date(self.acq_min)
        acq_max = format_date(self.acq_max)

        filename = os.path.join(self.output_directory,
                                "TidalExtra_{x:03d}_{y:04d}_{acq_min}_{acq_max}.tif".format(
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
            "DESCRIPTION": "Intertidal offset fields"
        }

    def load_tidal_file(self):

        fname=self.dummy
        _log.info("\tloading tidal offset file %s ", fname)
        try:
            lines = 0
            if os.path.exists(fname):
                tmpstore=open(self.dummy,'r').read().split("\n")
                with open(fname) as f:
                    low = f.readlines(1)
                    high = f.readlines(2)
                for line in open(fname):
                    lines += 1

                lo_hi_data=[tmpstore[0],tmpstore[1],len(tmpstore)-3]
                return lo_hi_data
        except IOError:
            print "Could not read a filter file"


    def run(self):

        shape = (4000, 4000)

        extra = [ExtraBands.OBSERVATIONS, ExtraBands.LOW, ExtraBands.MEDIAN, ExtraBands.HIGH]

        band_desc = ["TOTAL_OBSERVATIONS","LOWEST TIDE","MEDIAN TIDE", "HIGHEST TIDE"]

        extra_fields = dict()
        for field in extra:
            extra_fields[field] = empty_array(shape=shape, dtype=numpy.int16, fill=0)

        metadata = None
        low_value = 0
        high_value = 0
        median_value = 0
        observations = 0
        cur = self.load_tidal_file()
        low=cur[0]
        high=cur[1]
        observations=cur[2]
        #import pdb; pdb.set_trace()
        low_value = int(float(low) * 1000)
        high_value = int(float(high) * 1000)
        median_value = (low_value + high_value)/2
        #import pdb; pdb.set_trace()
        _log.info("\toffset file low %d high %d median %d lines %d ", low_value, high_value, median_value, observations)
        for layer in extra_fields:
            lv = layer.value
            if lv == 1:
                extra_fields[layer][:] = observations
            if lv == 2:
                extra_fields[layer][:] = low_value
            if lv == 3:
                extra_fields[layer][:] = median_value
            if lv == 4:
                extra_fields[layer][:] = high_value
        transform = (self.x, 0.00025, 0.0, self.y+1, 0.0, -0.00025)

        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)

        projection = srs.ExportToWkt()

        # Create the output dataset
        metadata_info = self.generate_raster_metadata()
        raster_create(self.output().path, [extra_fields[field] for field in extra], transform, projection, NDV, GDT_Int16, dataset_metadata=metadata_info, band_ids=band_desc)
        

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    Intertidal_extra_Workflow().run()
