#!/bin/bash
DIR="/g/data2/v10/ARG25-tidal-analysis/test/bb_high/datafile_"
TT1="\COPY (select  to_char(acquisition_date,'yyyy-mm-dd') as acquisition_date, tiles, height from tidal_height where acquisition_date > '1994-12-31' and acquisition_date < '2016-01-01' and tiles="
TT2=" ORDER BY height DESC limit (SELECT (count(*) / 10) as bb FROM tidal_height where acquisition_date > '1994-12-31' and acquisition_date < '2016-01-01' and tiles="

TT3=")) TO "
LT=" DELIMITER ',' csv"
cnt=0
rm comb_high_file
while read line
do
        let cnt=cnt+1
	echo $TT1\'$line\'$TT2\'$line\'$TT3$DIR$line$LT >> comb_high_file

done < all_coastal_files
