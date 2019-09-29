# DATA420 Assignment 2
# Jing Wu 29696576




# Data Processing Q1


# (a) Give an overview of the structure of the datasets, including file formats, data types,
# and the expected level of parallelism that you can expect to achieve from HDFS.


# Recursively list subdirectories encountered

hdfs dfs -ls -R -h /data/msd/



# Determine the size of all of the msd data
hdfs dfs -du -h /data/
#279.9 M  1.1 G   /data/crime
#13.4 G   53.6 G  /data/ghcnd
#1.9 K    7.5 K   /data/helloworld
#12.9 G   51.8 G  /data/msd
#3.7 M    14.7 M  /data/openflights
#643      2.5 K   /data/primes
#19.1 M   76.5 M  /data/shakespeare



# Determine the size of each of the msd data
hdfs dfs -du -h /data/msd/

#12.3 G   49.1 G   /data/msd/audio
#30.1 M   120.5 M  /data/msd/genre
#174.4 M  697.6 M  /data/msd/main
#490.4 M  1.9 G    /data/msd/tasteprofile



######################### audio data type ###################################

# attributes data type

#Peek at the first 3 lines of each data file to check the data type in each file
files=`hdfs dfs -ls /data/msd/audio/attributes |  awk -F " " '{print $8}'`

for file in $files
do
	echo $file 
	hdfs dfs -cat $file | head -n3
done

#/data/msd/audio/attributes/msd-jmir-area-of-moments-all-v1.0.attributes.csv
#Area_Method_of_Moments_Overall_Standard_Deviation_1,real
#Area_Method_of_Moments_Overall_Standard_Deviation_2,real
#Area_Method_of_Moments_Overall_Standard_Deviation_3,real
#/data/msd/audio/attributes/msd-jmir-lpc-all-v1.0.attributes.csv
#LPC_Overall_Standard_Deviation_1,real
#LPC_Overall_Standard_Deviation_2,real
#LPC_Overall_Standard_Deviation_3,real
#......
#/data/msd/audio/attributes/msd-tssd-v1.0.attributes.csv
#"component_1",NUMERIC
#"component_2",NUMERIC
#"component_3",NUMERIC



# features files data type
files=`hdfs dfs -ls /data/msd/audio/attributes |  awk -F " " '{print $8}'`

# display the number of grouped data type in each file
for file in $files
do
	echo $file 
	hdfs dfs -cat $file | awk -F"," '{print $2}' | uniq -c
done

#/data/msd/audio/attributes/msd-jmir-area-of-moments-all-v1.0.attributes.csv
#     20 real
#      1 string
#/data/msd/audio/attributes/msd-jmir-lpc-all-v1.0.attributes.csv
#     20 real
#      1 string
#/data/msd/audio/attributes/msd-jmir-methods-of-moments-all-v1.0.attributes.csv
#     10 real
#      1 string
#/data/msd/audio/attributes/msd-jmir-mfcc-all-v1.0.attributes.csv
#     26 real
#      1 string
#/data/msd/audio/attributes/msd-jmir-spectral-all-all-v1.0.attributes.csv
#     16 real
#      1 string
#/data/msd/audio/attributes/msd-jmir-spectral-derivatives-all-all-v1.0.attributes.csv
#     16 real
#      1 string
#/data/msd/audio/attributes/msd-marsyas-timbral-v1.0.attributes.csv
#    124 real
#      1 string
#/data/msd/audio/attributes/msd-mvd-v1.0.attributes.csv
#    420 NUMERIC
#      1 STRING
#/data/msd/audio/attributes/msd-rh-v1.0.attributes.csv
#     60 NUMERIC
#      1 STRING
#/data/msd/audio/attributes/msd-rp-v1.0.attributes.csv
#   1440 NUMERIC
#      1 STRING
#/data/msd/audio/attributes/msd-ssd-v1.0.attributes.csv
#    168 NUMERIC
#      1 STRING
#/data/msd/audio/attributes/msd-trh-v1.0.attributes.csv
#    420 NUMERIC
#      1 STRING
#/data/msd/audio/attributes/msd-tssd-v1.0.attributes.csv
#   1176 NUMERIC
#      1 STRING



######################### genre data type ###################################

files=`hdfs dfs -ls /data/msd/genre |  awk -F " " '{print $8}'`

for file in $files
do
	echo $file 
	hdfs dfs -cat $file | head -n3
done

#/data/msd/genre/msd-MAGD-genreAssignment.tsv
#TRAAAAK128F9318786      Pop_Rock
#TRAAAAV128F421A322      Pop_Rock
#TRAAAAW128F429D538      Rap
#/data/msd/genre/msd-MASD-styleAssignment.tsv
#TRAAAAK128F9318786      Metal_Alternative
#TRAAAAV128F421A322      Punk
#TRAAAAW128F429D538      Hip_Hop_Rap
#/data/msd/genre/msd-topMAGD-genreAssignment.tsv
#TRAAAAK128F9318786      Pop_Rock
#TRAAAAV128F421A322      Pop_Rock
#TRAAAAW128F429D538      Rap





######################### main data type ###################################

hdfs dfs -cat /data/msd/main/summary/analysis.csv.gz | gunzip | head -n3

#analysis_sample_rate,audio_md5,danceability,duration,end_of_fade_in,energy,idx_bars_confidence,idx_bars_start,idx_beats_confidence,idx_beats_start,idx_sections_confidence,idx_sections_start,idx_segments_confidence,idx_segments_loudness_max,idx_segments_loudness_max_time,idx_segments_loudness_start,idx_segments_pitches,idx_segments_start,idx_segments_timbre,idx_tatums_confidence,idx_tatums_start,key,key_confidence,loudness,mode,mode_confidence,start_of_fade_out,tempo,time_signature,time_signature_confidence,track_id
#22050,aee9820911781c734e7694c5432990ca,0.0,252.05506,2.049,0.0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,10,0.777,-4.829,0,0.688,236.635,87.002,4,0.94,TRMMMYQ128F932D901
#22050,ed222d07c83bac7689d52753610a513a,0.0,156.55138,0.258,0.0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,9,0.808,-10.555,1,0.355,148.66,150.778,1,0.0,TRMMMKD128F425225D


hdfs dfs -cat /data/msd/main/summary/metadata.csv.gz | gunzip | head -n3

#analyzer_version,artist_7digitalid,artist_familiarity,artist_hotttnesss,artist_id,artist_latitude,artist_location,artist_longitude,artist_mbid,artist_name,artist_playmeid,genre,idx_artist_terms,idx_similar_artists,release,release_7digitalid,song_hotttnesss,song_id,title,track_7digitalid
#,4069,0.6498221002008776,0.3940318927141434,ARYZTJS1187B98C555,,,,357ff05d-848a-44cf-b608-cb34b5701ae5,Faster Pussy cat,44895,,0,0,Monster Ballads X-Mas,633681,0.5428987432910862,SOQMMHC12AB0180CB8,Silent Night,7032331
#,113480,0.4396039666767154,0.3569921077564064,ARMVN3U1187FB3A1EB,,,,8d7ef530-a6fd-4f8f-b2e2-74aec765e0f9,Karkkiautomaatti,-1,,0,0,Karkuteillä,145266,0.2998774882739778,SOVFVAK12A8C1350D9,Tanssi vaan,1514808


######################### tasteprofile data type ###################################
# mismatches
hdfs dfs -cat /data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt | head -n5 

hdfs dfs -cat /data/msd/tasteprofile/mismatches/sid_mismatches.txt | head -n5 


# triplets

files=`hdfs dfs -ls /data/msd/tasteprofile/triplets.tsv/ |  awk -F " " '{print $8}'`

for file in $files
do
	echo $file 
	hdfs dfs -cat $file | head -n3
done





# (c) Count the number of rows in each of the datasets. How do the counts compare to the total
# number of unique songs?

################# Count the number of rows in attributes directory #################

# Get the file names in the attributes directory
files=`hdfs dfs -ls /data/msd/audio/attributes |  awk -F " " '{print $8}'`

# Count the number of rows in each file
for file in $files
do
	hdfs dfs -cat $file | wc -l
done

#21
#21
#11
#27
#17
#17
#125
#421
#61
#1441
#169
#421
#1177


# Count total lines in attributes directory

hdfs dfs -cat /data/msd/audio/attributes/* | wc -l
# 3929


############### Count the number of rows in features directory ###########

# Get the file names in the features directory

files=`hdfs dfs -ls /data/msd/audio/features |  awk -F " " '{print $8}'`

# Count the number of rows in each csv
for file in $files
do
	echo $file
	hdfs dfs -cat $file/* | gunzip | wc -l
done

#/data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv
#994623
#/data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv
#994623
#/data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv
#994623
#/data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv
#994623
#/data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv
#994623
#/data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv
#994623
#/data/msd/audio/features/msd-marsyas-timbral-v1.0.csv
#995001
#/data/msd/audio/features/msd-mvd-v1.0.csv
#994188
#/data/msd/audio/features/msd-rh-v1.0.csv
#994188
#/data/msd/audio/features/msd-rp-v1.0.csv
#994188
#/data/msd/audio/features/msd-ssd-v1.0.csv
#994188
#/data/msd/audio/features/msd-trh-v1.0.csv
#994188
#/data/msd/audio/features/msd-tssd-v1.0.csv
#994188

	


############## Count the number of rows in statistics directory ######
hdfs dfs -cat /data/msd/audio/statistics/sample_properties.csv.gz | gunzip | wc -l
#992866



################# Count the number of rows in genre directory #################

# Get the file names in the genre directory
files=`hdfs dfs -ls /data/msd/genre/ | awk -F " " '{print $8}'`

# Count the number of rows in each file
for file in $files
do
	hdfs dfs -cat $file | wc -l
done

#422714
#273936
#406427



################# Count the number of rows in main directory #################

hdfs dfs -cat /data/msd/main/summary/analysis.csv.gz | gunzip | wc -l
# 1000001

hdfs dfs -cat /data/msd/main/summary/metadata.csv.gz | gunzip | wc -l
# 1000001


################# Count the number of rows in tasteprofile directory #################

hdfs dfs -cat /data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt | wc -l
# 938

hdfs dfs -cat /data/msd/tasteprofile/mismatches/sid_mismatches.txt | wc -l
# 19094

hdfs dfs -cat /data/msd/tasteprofile/triplets.tsv/* | gunzip | wc -l
# 48373586