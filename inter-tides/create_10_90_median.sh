#!/bin/bash

cd /g/data/v10/ARG25-tidal-analysis/datasource/origfile
cnt=0
datafile=$1
fr=$(head -n1 $datafile|awk -F"," '{print 1000*$3}'|awk -F"." '{print $1}')
nr=$(wc $datafile|awk '{print $1}')
echo "total rec " $nr
totr=$nr
#nr=$(cat $datafile|awk -F"," '{cnt++;if ($cnt==1) {fr=$3}; if ($cnt==NR) {lr=$3}} END {print $lr-$fr}')  
nr=$(cat $datafile|awk -F"," -v bb=$nr '{cnt++;if (cnt==1) {ff=$3}; if (cnt==bb) {lr=$3}} END {print 1000*(lr-ff)}'|awk -F"." '{print $1}' )
#lr=$(tail -n1 $datafile|awk -F"," '{print $3}')
#nr=$(($lr-$fr))
echo first $fr diff $nr
cells=$(ls $datafile|awk -F"_" '{print $2"_"$3}')
echo "number of records in the file "$nr  "and cell value " $cells

nar=$(echo $nr|awk '{printf("%f",$1*(10/100))}')
echo "nar and fr " $nar $fr
nfr=$(echo $nar|awk -v ff=$fr '{printf("%f", (ff+$1)*10)}'|awk -F"." '{print $1}')
echo "nfr is" $nfr
#let nar=$((($nr*10/100)))
echo "actual record numbers is" $nar
#let nfr=$(($fr+$nar))
echo "first cycle max="$nfr
cnt=0
fcnt=10
ncnt=1
nncnt=0
rm med_final_datafile_$cells*
#cat $datafile|awk -F"," '{if ($3 <= $nar +$nfr){print   }' 
while read line
do
	of=$(echo $line|awk -F"," '{print 10000*$3}'|awk -F"." '{print $1}')
	let cnt=$cnt+1
        if [ $((of)) -le $nfr ] ; then
		if [ $nncnt -eq 0 ] ;  then
        	     nncnt=1
            #         echo "first offset" $of
                     echo $line|awk -F"," '{print $NF}' >> med_final_datafile_"$cells"_$fcnt
                fi
                if [ $cnt -eq $totr ] ; then
			nn=$(echo $line|awk -F"," '{print $NF}')
           #             echo "last offset is $nn line $line count $cnt num rec $nr "
			echo $line|awk -F"," '{print $NF}' >> med_final_datafile_"$cells"_$fcnt
		fi
		#if [ $nncnt -eq 0 ] ; then
        	#     nncnt=1
	        #     echo $line|awk -F"," '{print $NF}' >> n_final_datafile_"$cells"_$fcnt
		#fi
                
        else
                #echo $line|awk -F"," '{print $NF}' >> ffinal_datafile_"$cells"_$fcnt
                echo $prevv >> med_final_datafile_"$cells"_$fcnt
          #           echo "second offset" $of
                nncnt=0
                let ncnt=$(($ncnt+1))
                let fcnt=$(($ncnt*10))
                echo $line|awk -F"," '{print $NF}' > med_final_datafile_"$cells"_$fcnt
                nncnt=1

                bb=0	
                #let nfr=$(($of+$nar))
                #let of=$(($of/10))
                #nfr=$(echo $nar|awk -v ff=$of '{printf("%d", (ff+$1)*10)}'|awk -F"." '{print $1}')
	        let nfr=$(($nfr+$nr))        
         #       echo file records to go $nfr
        fi
        prevv=$(echo $line|awk -F"," '{print $NF}') 

done < $datafile

for i in ffinal_datafile_*;
do
	rec=$(wc $i|awk '{print $1}')
        cn=$(wc $i|awk '{print $1}')
	if [ $cn -lt 2 ] ; then
		echo "This file $i has one record only"
 		head -n1 $i > tmp_fl
                cat $i >> tmp_fl
		cat tmp_fl > $i
	fi
done
#let nfr=$(($fr+$nar))
nfr=$(echo $nar|awk -v ff=$fr '{printf("%d", (ff+$1)*10)}')
cnt=0
fcnt=10
ncnt=1
rm ll_*
while read line
do
	of=$(echo $line|awk -F"," '{print 10000*$3}'|awk -F"." '{print $1}')
	let cnt=$cnt+1
        if [ $((of)) -le $nfr ] ; then 
	     echo $line|awk -F"," '{print $3}' >> ll_$fcnt 
        else
                #echo $prevv >> ffinal_datafile_"$cells"_$fcnt 
		let ncnt=$(($ncnt+1))
		let fcnt=$(($ncnt*10))
                echo $line|awk -F"," '{print $3}' >> ll_$fcnt
	#	echo file count is $fcnt
                #let nfr=$(($of+$nar))
                let nfr=$(($nfr+$nr))
		#echo file records to go $nfr
	fi  	

done < $datafile

head -n1 med_final_datafile_"$cells"_90 > bb1
tail -n1 med_final_datafile_"$cells"_100 >> bb1

cat ll_90 ll_100 |sort -n > ll_110
cp ll_110 ll_90
for i in {10..80..10};
do
             cat ll_$i|awk 'BEGIN {c=0; SUM=0;}{a[c++]=$1; sum+=$1} END{if ((c%2) == 1) { print a[int(c/2)];} else {print (a[c/2]+a[c/2-1])/2;}}' >  med_final_datafile_"$cells"_$i
done;
cat bb1 > med_final_datafile_"$cells"_90
             cat ll_90|awk 'BEGIN {c=0; SUM=0;}{a[c++]=$1; sum+=$1} END{if ((c%2) == 1) { print a[int(c/2)];} else {print (a[c/2]+a[c/2-1])/2;}}' >  med_final_datafile_"$cells"_90

