for i in datafile*;
do bash create_10per_tidal_awk.sh $i >> count_inter_tides.out; sleep 5;
done
