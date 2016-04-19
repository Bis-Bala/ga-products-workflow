#!/bin/bash

cd /g/data/v10/ARG25-tidal-analysis/datasource/origfile
cnt=0
datafile=$1
fr=$(head -n1 $datafile|awk -F"," '{print 100*$3}'|awk -F"." '{print $1}')
nr=$(wc $datafile|awk '{print $1}')
echo "total rec " $nr
totr=$nr
#nr=$(cat $datafile|awk -F"," '{cnt++;if ($cnt==1) {fr=$3}; if ($cnt==NR) {lr=$3}} END {print $lr-$fr}')  
nr=$(cat $datafile|awk -F"," -v bb=$nr '{cnt++;if (cnt==1) {ff=$3}; if (cnt==bb) {lr=$3}} END {print 100*(lr-ff)}'|awk -F"." '{print $1}' )
#lr=$(tail -n1 $datafile|awk -F"," '{print $3}')
#nr=$(($lr-$fr))
echo first $fr diff $nr
cells=$(ls $datafile|awk -F"_" '{print $2"_"$3}')
echo "number of records in the file "$nr  "and cell value " $cells
let nar=$((($nr*10/100)))
echo "actual record numbers is" $nar
let nfr=$(($fr+$nar))
echo "first cycle max="$nfr
cnt=0
fcnt=10
ncnt=1
nncnt=0
#rm n_final_datafile_$cells*

while read line
do
	of=$(echo $line|awk -F"," '{print 100*$3}'|awk -F"." '{print $1}')
        #echo first offset is $of
	let cnt=$cnt+1
        if [ $((of)) -le $nfr ] ; then
		if [ $nncnt -eq 0 ] ;  then
        	     nncnt=1
                     echo $line|awk -F"," '{print $NF}' >> final_datafile_"$cells"_$fcnt
                fi
                if [ $cnt -eq $totr ] ; then
			nn=$(echo $line|awk -F"," '{print $NF}')
                        echo "last offset is $nn line $line count $cnt num rec $nr "
			echo $line|awk -F"," '{print $NF}' >> final_datafile_"$cells"_$fcnt
		fi
		#if [ $nncnt -eq 0 ] ; then
        	#     nncnt=1
	        #     echo $line|awk -F"," '{print $NF}' >> n_final_datafile_"$cells"_$fcnt
		#fi
                
        else
                echo $line|awk -F"," '{print $NF}' >> final_datafile_"$cells"_$fcnt
                nncnt=0
                let ncnt=$(($ncnt+1))
                let fcnt=$(($ncnt*10))
                #echo file count is $fcnt
		
                let nfr=$(($of+$nar))
                #echo file records to go $nfr
        fi


done < $datafile

for i in final_datafile_*;
do
	rec=$(wc $i|awk '{print $1}')
	if [ $(wc $i|awk '{print $1}') -lt 2 ] ; then
		echo "This file $i has one record only"
 		head -n1 $i > tmp_fl
                cat $i >> tmp_fl
		cat tmp_fl > $i
	fi
done
let nfr=$(($fr+$nar))
cnt=0
fcnt=10
ncnt=1
while read line
do
	of=$(echo $line|awk -F"," '{print 100*$3}'|awk -F"." '{print $1}')
	let cnt=$cnt+1
        if [ $((of)) -le $nfr ] ; then 
	     echo $line|awk -F"," '{print $1}' >> final_datafile_"$cells"_$fcnt
        else
                echo $line|awk -F"," '{print $1}' >> final_datafile_"$cells"_$fcnt
		let ncnt=$(($ncnt+1))
		let fcnt=$(($ncnt*10))
		echo file count is $fcnt
                let nfr=$(($of+$nar))
		#echo file records to go $nfr
	fi  	


done < $datafile
