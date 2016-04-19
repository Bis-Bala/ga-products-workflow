#!/bin/bash

# ===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
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
#===============================================================================

PBS_SCRIPT="$HOME/agdc/api/source/main/python/datacube/api/inter-tides/run_band_stats_book_variant.sh"
#PBS_SCRIPT="/home/547/bxb547/agdc/api/source/main/python/datacube/api/workflow_tides/run_band_stats_book_extra.sh"
DIR="/g/data2/v10/ARG25-tidal-analysis/test/stats_variant"

cd $DIR
cnt=0
tt=0
while read line
do
	let tt=$tt+1
        #if [ $tt -lt 201 ] ; then
	#	continue
	#fi
        #if [ $tt -gt 200 ] ; then
	#	exit
	#fi
	echo "reading tiles and count is " $line $tt
	echo "reading tiles and count is " $line $tt
 	while true
	do 
		let cnt=$cnt+1
        	let ncnt=$((cnt*10))
        #if [ $cnt -lt 201 ] ; then
	#	continue
        #fi
        	lon=$(echo $line|awk -F"_" '{print $1}')
        	lat=$(echo $line|awk -F"_" '{print $2}')
		echo running median stats for $lon and $lat and count $cnt
		qsub -v xmin=$lon,xmax=$lon,ymin=$lat,ymax=$lat,count=$ncnt "${PBS_SCRIPT}"
		if [ "$line" == "150_-011" ] && [ $cnt -eq 1 ]; then
                    cnt=0
                    break
                fi     
		if [ "$line" == "151_-011" ] && [ $cnt -eq 6 ] ; then
			cnt=0
			break
		fi

 		if [ "$line" == "122_-011" ] && [ $cnt -eq 8 ] ; then 
                    cnt=0
                    break
                fi     
 		if [ "$line" == "123_-011" ] && [ $cnt -eq 8 ] ; then 
                    cnt=0
                    break
                fi     
 		if [ "$line" == "113_-027" ] && [ $cnt -eq 8 ] ; then 
			echo "no dataset for last cycle"
                    cnt=0
                    break
                fi     
 		if [ "$line" == "114_-027" ] && [ $cnt -eq 8 ] ; then 
                    cnt=0
                    break
                fi     
		if [ $cnt -eq 10 ]; then
                        echo "count max 10 reached"
                        cnt=0
			break	
	        fi
        	#echo sleeping 2 second
	        #sleep 2
	done
#done < all_coastal_files
done < bb_6
#done < bb_test
