DIR="/g/data2/v10/ARG25-tidal-analysis/datasource/origfile/datafile_"
TT1="\COPY (select  DISTINCT to_char(acquisition_date,'yyyy-mm-dd') as acquisition_date, tiles, height from tidal_height where acquisition_date > '1986-12-31' and acquisition_date < '2016-01-01' and tiles="
TT2=" ORDER BY height ASC "

TT3=") TO "
LT=" DELIMITER ',' csv"
cnt=0
rm comb_tidal_file
while read line
do
        let cnt=cnt+1
	echo $TT1\'$line\'$TT2$TT3"$DIR"$line"$LT" >> comb_tidal_file

done < all_coastal_files
