#!/bin/bash

# ./create_infiles.sh diseasesFile countriesFile /home/..../data 10 10


if [ "$#" -ne 5 ]; then		# elegxoi
	echo "Wrong number of parameters"
	exit 1
fi
if [ ! -f $1 ]; then
	echo "File missing"
	exit 2
fi
if [ ! -f $2 ]; then
	echo "File missing"
	exit 2
fi
if [ "$4" -lt 1 ]; then
	echo "Wrong number"
	exit 1
fi
if [ "$5" -lt 1 ]; then
	echo "Wrong number"
	exit 1
fi

mkdir $3

j=0		# diavasma arxeiwn
while IFS= read -r line
do
	country[$j]=$line
	let j=j+1
done < "$2"

diseaseCount=0
while IFS= read -r line
do
	disease[$diseaseCount]=$line
	let diseaseCount=diseaseCount+1
done < "$1"

id=0
for j in ${country[@]}		# dimiourgia subdirectories
do
#	echo $j
	curRec=0
	countRec=0
	(cd $3 ; mkdir $j)
	i=0
	while [ $i -lt $4 ]
	do											# touch arxeiwn imerominiwn
		days[$i]=`expr $RANDOM % 30 + 1`
		months[$i]=`expr $RANDOM % 12 + 1`
		years[$i]=`expr $RANDOM % 21 + 2000`
		z=0
		flag=0
		while [ $z -lt $i ]
		do
			if [ ${days[$z]} -eq ${days[$i]} ]; then
				if [ ${months[$z]} -eq ${months[$i]} ]; then
					if [ ${years[$z]} -eq ${years[$i]} ]; then
							echo same day_month_year ${days[$i]}-${months[$i]}-${years[$i]} ###
							flag=1
							break
					fi
				fi
			fi
			let z=z+1
		done
		if [ ${days[$z]} -lt 10 ] && [ ${months[$z]} -lt 10 ]; then
			Date[$i]="0${days[$i]}-0${months[$i]}-${years[$i]}"
		elif [ ${days[$z]} -lt 10 ] && [ ! ${months[$z]} -lt 10 ]; then
			Date[$i]="0${days[$i]}-${months[$i]}-${years[$i]}"
		elif [ ! ${days[$z]} -lt 10 ] && [ ${months[$z]} -lt 10 ]; then
			Date[$i]="${days[$i]}-0${months[$i]}-${years[$i]}"
		else
			Date[$i]="${days[$i]}-${months[$i]}-${years[$i]}"
		fi
		if [ "$flag" -ne 1 ]; then
			(cd $3/$j ; touch ${Date[$i]})
			# deleted

			# create $5 records
			let i=i+1
		fi
	done

	for (( i = 0; i < $5; i++ )); do
		nameLength=`expr $RANDOM % 10 + 2`
		lastLength=`expr $RANDOM % 10 + 2`
		name[$curRec]=`head /dev/urandom | tr -dc A-Z | head -c 1 ; head /dev/urandom | tr -dc a-z | head -c $nameLength`
		last[$curRec]=`head /dev/urandom | tr -dc A-Z | head -c 1 ; head /dev/urandom | tr -dc a-z | head -c $lastLength`
		astheneia[$curRec]=${disease[`expr $RANDOM % $diseaseCount`]}
		age[$curRec]=`expr $RANDOM % 120 + 1`
		idArray[$curRec]=$id
		(cd $3/$j ; echo $id ENTER ${name[$curRec]} ${last[$curRec]} ${astheneia[$curRec]} ${age[$curRec]} >> ${Date[0]})
#		echo ${idArray[$curRec]} ENTER ${name[$curRec]} ${last[$curRec]} ${astheneia[$curRec]} ${age[$curRec]} $j

		let id=id+1
		let curRec=curRec+1
	done
	for (( z = 1; z < $4; z++ )); do
		for (( i = 0; i < $5; i++ )); do
			if [ `expr $RANDOM % 2` -eq 0 ] ||  [ $countRec -ge $curRec ] ; then
				nameLength=`expr $RANDOM % 10 + 2`
				lastLength=`expr $RANDOM % 10 + 2`
				name[$curRec]=`head /dev/urandom | tr -dc A-Z | head -c 1 ; head /dev/urandom | tr -dc a-z | head -c $nameLength`
				last[$curRec]=`head /dev/urandom | tr -dc A-Z | head -c 1 ; head /dev/urandom | tr -dc a-z | head -c $lastLength`
				astheneia[$curRec]=${disease[`expr $RANDOM % $diseaseCount`]}
				age[$curRec]=`expr $RANDOM % 120 + 1`
				idArray[$curRec]=$id
				(cd $3/$j ; echo $id ENTER ${name[$curRec]} ${last[$curRec]} ${astheneia[$curRec]} ${age[$curRec]} >> ${Date[$z]})
#				echo $id ENTER ${name[$curRec]} ${last[$curRec]} ${astheneia[$curRec]} ${age[$curRec]} $j
				let id=id+1
				let curRec=curRec+1
			else
				(cd $3/$j ; echo ${idArray[$countRec]} EXIT ${name[$countRec]} ${last[$countRec]} ${astheneia[$countRec]} ${age[$countRec]} >> ${Date[$z]})
#				echo ${idArray[$countRec]} EXIT ${name[$countRec]} ${last[$countRec]} ${astheneia[$countRec]} ${age[$countRec]} $j
				let countRec=countRec+1
			fi
		done
	done
done

exit 0
