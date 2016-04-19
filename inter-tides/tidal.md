# Inter tidal workflow

1.	 Run psql -h 130.56.244.224 -d tidal_wave -U cube_user < comb_tidal_file after creating comb_tidal_file by running run_concat_intertidal_sql.sh.  This will create datafiles for each tiles in the mentioned output file of the script.
2.	Run create_10per_tidal.sh to create 10% offset files.
3.	Run tidal_stats_variants_book.pbs.sh and intertidal_extra.pbs.sh to create two bands and four bands combined product as (ARG25_123_-017_19870101_20151231_JAN01_DEC31_NDWI_PERCENTILE_50.tif, ARG25_123_-017_19870101_20151231_JAN01_DEC31_NDWI_STANDARD_DEVIATION.tif)  and TidalExtra_123_-017_1987_01_01_2015_12_31.tif respectively.
4.	Run run_gdal_merge.sh to merge all bands to output in float32(very important).
5.	Run tidal_mixed_prod.pbs and tidal_mixed_prod_rel.pbs to get final DEM and relative products.
6.	Following are the scripts, programs that are required to complete the workflow.


all_coastal_files,                 intertidal_extra.sh,             tidal_mixed_prod_final.py,
band_statistics_tidal_variant.py,  run_band_stats_book_variant.sh,  tidal_mixed_prod_rel.pbs,
comb_tidal_file,                   run_concat_intertidal_sql.sh,    tidal_mixed_prod_rel.sh,
create_10per_tidal.sh,             run_gdal_merge.sh,               tidal_mixed_prod_relative_book.py,
intertidal_extra.pbs.sh,           tidal_mixed_prod.pbs,            tidal_stats_variants_book.pbs.sh,
intertidal_extra.py,               tidal_mixed_prod.sh

