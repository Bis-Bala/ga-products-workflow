Procedure

#This process will interact tidal_wave database, which is created under production host 130.56.244.224 and this is the same as our production host with the same user name - cube_user and same password used for it. It will collect 10% of high and 10% of low data from tidal_height table.
#1.	Clone the repository and create a new directory( like - workflow_tides) under ~/datacube/api.
#2.	bash run_concat_low_sql.sh  # output to comb_low_file
bash run_concat_high_sql.sh  # output to  comb_high_file
psql -h 130.56.244.224 -p 6432 -dtidal_wave -Ucube_user -f comb_low_file
psql -h 130.56.244.224 -p 6432 -dtidal_wave -Ucube_user -f comb_high_file

#3.	This process will create final output files for next step as input files
bash run_create_offset_low_file.sh
bash run_create_offset_high_file.sh
#4.	This process will create tif files of three bands of dataset, low offset and high offset values.
cd  workflow_tides; bash create_50.sh; bash run_tidal.sh
#5.	This process will create 6 bands tif files for median values.
bash tidal_stats.pbs.sh
#6.	Once step 4 is completed , run gdal_merge process to get final product after merging step 5 outputs to step-4 output. Output of step 4 (output_directory is set in config.cfg) and output of step 5 is set in  shell script of tidal_stats.pbs.sh/ run_band_stats.sh
bash run_gdal_merge.sh


