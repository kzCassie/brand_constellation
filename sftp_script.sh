#!/bin/bash
# local path:   4.IG_3000
# remote path:  ~

# compress code folder
function tar_ig_folder(){
	cd .. 
	cp "4.IG_3000" "ig"
	tar -cf "ig.tar" "ig"
	cd "4.IG_3000"
}



# loop through AWS instances
function loop_instance(){
INPUTFILE="input/instance.csv"
timestamp=`date "+%Y%m%d_%H%M"`
echo $timestamp

while IFS="," read -r id ip rank; do
	echo "$id, $ip, $rank"; 
	rank=${rank:0:${#rank}-1};

##### sftp #####
##### 1. download data & log folder
##### 2. upload ig.tar
path="../instance_data/data$rank/$timestamp"
sftp -i ~/.ssh/Kehang_key.pem ubuntu@"$ip" <<DELIMTER
# lmkdir "../instance_data/data$rank/"
# lmkdir $path
# get -R "ig/data" $path
# get -R "ig/log" $path
# rm -r ig
# rm ig.tar
# cd ig/code
# put "code/brand.py"
quit
DELIMTER

##### ssh #####
##### 1. extract ig.tar
# total=1
# ssh -tt -i ~/.ssh/IG_key.pem ubuntu@"$ip" <<DELIMTER
# # tar -xf "ig.tar"
# # ps -ef | grep python3
# sudo chmod -R a+rw ig
# cd ig/code/
# nohup python3 post.py $rank $total &
# exit
# DELIMTER

	done < "$INPUTFILE"
}




##### MAIN #####
# tar_ig_folder
loop_instance


