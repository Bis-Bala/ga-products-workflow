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
# creates median files med_final_datafile_"$cells"_10
bash create_10_90_median.sh $datafile
rm ffinal_datafile_$cells*
#cat $datafile|awk -F"," '{if ($3 <= $nar +$nfr){print   }' 
while read line
do
	of=$(echo $line|awk -F"," '{print 10000*$3}'|awk -F"." '{print $1}')
	let cnt=$cnt+1
        # echo count and total $cnt $totr
        if [ $((of)) -le $nfr ] ; then
		if [ $nncnt -eq 0 ] ;  then
        	     nncnt=1
                     echo $line|awk -F"," '{print $NF}' >> ffinal_datafile_"$cells"_$fcnt
                fi
                if [ $cnt -eq $totr ] ; then
			nn=$(echo $line|awk -F"," '{print $NF}')
                        echo "last offset is $nn line $line count $cnt num rec $nr "
			echo $line|awk -F"," '{print $NF}' >> ffinal_datafile_"$cells"_$fcnt
		fi
		#if [ $nncnt -eq 0 ] ; then
        	#     nncnt=1
	        #     echo $line|awk -F"," '{print $NF}' >> n_final_datafile_"$cells"_$fcnt
		#fi
                
        else
                #echo $line|awk -F"," '{print $NF}' >> ffinal_datafile_"$cells"_$fcnt
                echo $prevv >> ffinal_datafile_"$cells"_$fcnt
                #     echo "second offset" $of
                nncnt=0
                let ncnt=$(($ncnt+1))
                let fcnt=$(($ncnt*10))
                echo $line|awk -F"," '{print $NF}' > ffinal_datafile_"$cells"_$fcnt
                nncnt=1

                bb=0	
                #let nfr=$(($of+$nar))
                #let of=$(($of/10))
                #nfr=$(echo $nar|awk -v ff=$of '{printf("%d", (ff+$1)*10)}'|awk -F"." '{print $1}')
	        let nfr=$(($nfr+$nr))        
        fi
        #if [ $fcnt -eq 100 ]; then
	#	break;
	#fi
        prevv=$(echo $line|awk -F"," '{print $NF}') 

done < $datafile

for i in ffinal_datafile_$cells*;
do
	rec=$(wc $i|awk '{print $1}')
	if [ $(wc $i|awk '{print $1}') -lt 2 ] ; then
		echo "This file $i has one record only"
 		head -n1 $i > tmp_fl
                cat $i >> tmp_fl
		cat tmp_fl > $i
	fi
done


#merge 90 and 100 now

        cat ffinal_datafile*$cells*_90 ffinal_datafile*$cells*_100|sort -n > my_file
        nz=$(wc my_file|awk '{print $1}')
        echo rec len $nr
        let nnr=$(($nz-3))
        echo new rec len $nnr
        cat my_file|head -n1 > ffinal_datafile_"$cells"_90
        cat my_file|tail -n1 >>  ffinal_datafile_"$cells"_90

for i in {10..90..10};
do
	cat med_final_datafile_"$cells"_$i >> ffinal_datafile_"$cells"_$i
done


#let nfr=$(($fr+$nar))
nfr=$(echo $nar|awk -v ff=$fr '{printf("%d", (ff+$1)*10)}')
cnt=0
fcnt=10
ncnt=1
while read line
do
	of=$(echo $line|awk -F"," '{print 10000*$3}'|awk -F"." '{print $1}')
	let cnt=$cnt+1
        if [ $((of)) -le $nfr ] ; then 
	     echo $line|awk -F"," '{print $1}' >> ffinal_datafile_"$cells"_$fcnt
        else
                #echo $prevv >> ffinal_datafile_"$cells"_$fcnt 
		let ncnt=$(($ncnt+1))
		let fcnt=$(($ncnt*10))
                echo $line|awk -F"," '{print $1}' >> ffinal_datafile_"$cells"_$fcnt
		#echo file count is $fcnt
                #let nfr=$(($of+$nar))
                if [ $fcnt -eq 90 ] ; then
			let nfr=$(($nfr+(2*$nr)))
		else
                	let nfr=$(($nfr+$nr))
		fi
            
		#echo file records to go $nfr
	fi  	
        prevv=$(echo $line|awk -F"," '{print $NF}')


done < $datafile

