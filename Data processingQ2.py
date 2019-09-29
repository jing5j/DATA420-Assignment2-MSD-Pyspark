# DATA420 Assignment 2
# Jing Wu 29696576



# Data Processing Q2



# start_pyspark_shell -e 4 -c 2 -w 4 -m 4

# Imports

from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()




# (a)
# Define schemas for mismathched songs 
schema = StructType([
    StructField("SONG_ID", StringType(),True),
    StructField("TRACK_ID", StringType(),True),
    StructField("SONG_ARTIST", StringType(),True),
    StructField("SONG_TITLE", StringType(),True),
    StructField("TRACK_ARTIST", StringType(),True),
    StructField("SONG_TITLE", StringType(),True)
])


# Load the matched text
matches_manually_text_only = (
    spark.read.format("text")
    .load("hdfs:///data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt")
)


# Extract the data from text
file = matches_manually_text_only.toPandas()
sid_matches_manually_accepted = []
for i in range(len(file)-1):
    line =  file.iloc[i+1,0]
    if line.startswith("< ERROR: "):
        a = line[10:28]
        b = line[29:47]
        c, d = line[49:].split("  !=  ")
        e, f = c.split("  -  ")
        g, h = d.split("  -  ")
        sid_matches_manually_accepted.append((a,b,e,f,g,h))


matches_manually_accepted = spark.createDataFrame(sc.parallelize(sid_matches_manually_accepted,3), schema = schema)
matches_manually_accepted.cache()
matches_manually_accepted.show(5,False)
#+------------------+------------------+-----------------+------------------------------------------------+----------------------------------------------------------------------------------------------------+-----------------------------------------------------------------+
#|SONG_ID           |TRACK_ID          |SONG_ARTIST      |SONG_TITLE                                      |TRACK_ARTIST                                                                                        |SONG_TITLE                                                       |
#+------------------+------------------+-----------------+------------------------------------------------+----------------------------------------------------------------------------------------------------+-----------------------------------------------------------------+
#|SOFQHZM12A8C142342|TRMWMFG128F92FFEF2|Josipa Lisac     |razloga                                         |Lisac Josipa                                                                                        |1000 razloga                                                     |
#|SODXUTF12AB018A3DA|TRMWPCD12903CCE5ED|Lutan Fyah       |Nuh Matter the Crisis Feat. Midnite             |Midnite                                                                                             |Nah Matter the Crisis                                            |
#|SOASCRF12A8C1372E6|TRMHIPJ128F426A2E2|Gaetano Donizetti|L'Elisir d'Amore: Act Two: Come sen va contento!|Gianandrea Gavazzeni_ Orchestra E Coro Del Maggio Musicale Fiorentino_ Carlo Bergonzi_ Renata Scotto|L'Elisir D'Amore_ Act 2: Come Sen Va Contento (Adina) (Donizetti)|
#|SOITDUN12A58A7AACA|TRMHXGK128F42446AB|C.J. Chenier     |Ay, Ai Ai                                       |Clifton Chenier                                                                                     |Ay_ Ai Ai                                                        |
#|SOLZXUM12AB018BE39|TRMRSOF12903CCF516|許志安           |男人最痛                                        |Andy Hui                                                                                            |Nan Ren Zui Tong                                                 |
#+------------------+------------------+-----------------+------------------------------------------------+----------------------------------------------------------------------------------------------------+-----------------------------------------------------------------+
#only showing top 5 rows


matches_manually_accepted.count() # 488


# Load the mismatched text
mismatches_text_only = (
    spark.read.format("text")
    .load("hdfs:///data/msd/tasteprofile/mismatches/sid_mismatches.txt")
)

misfile = mismatches_text_only.toPandas()
mismatches_list= []
for i in range(len(misfile)-1):
    line =  misfile.iloc[i+1,0]
    if line.startswith("ERROR: "):
        a = line[8:26]
        b = line[27:45]
        c, d = line[47:].split("  !=  ")
        e, f = c.split("  -  ")
        g, h = d.split("  -  ")
        mismatches_list.append((a,b,e,f,g,h))

mismatches = spark.createDataFrame(sc.parallelize(mismatches_list,3), schema = schema)
mismatches.cache()
mismatches.show(5,False)
#+------------------+------------------+------------------+-----------------------------------------+--------------+-------------------------+
#|SONG_ID           |TRACK_ID          |SONG_ARTIST       |SONG_TITLE                               |TRACK_ARTIST  |SONG_TITLE               |
#+------------------+------------------+------------------+-----------------------------------------+--------------+-------------------------+
#|SOCMRBE12AB018C546|TRMMREB12903CEB1B1|Jimmy Reed        |The Sun Is Shining (Digitally Remastered)|Slim Harpo    |I Got Love If You Want It|
#|SOLPHZY12AC468ABA8|TRMMBOC12903CEB46E|Africa HiTech     |Footstep                                 |Marcus Worgull|Drumstern (BONUS TRACK)  |
#|SONGHTM12A8C1374EF|TRMMITP128F425D8D0|Death in Vegas    |Anita Berber                             |Valen Hsu     |Shi Yi                   |
#|SONGXCA12A8C13E82E|TRMMAYZ128F429ECE6|Grupo Exterminador|El Triunfador                            |I Ribelli     |Lei M'Ama                |
#|SOMBCRC12A67ADA435|TRMMNVU128EF343EED|Fading Friend     |Get us out!                              |Masterboy     |Feel The Heat 2000       |
#+------------------+------------------+------------------+-----------------------------------------+--------------+-------------------------+

mismatches.count() # 19093

mismatches.printSchema()
#root
# |-- SONG_ID: string (nullable = true)
# |-- TRACK_ID: string (nullable = true)
# |-- SONG_ARTIST: string (nullable = true)
# |-- SONG_TITLE: string (nullable = true)
# |-- TRACK_ARTIST: string (nullable = true)
# |-- SONG_TITLE: string (nullable = true)


# Define triplets schemas 
schema_triplets = StructType([
    StructField('USER_ID', StringType()),
    StructField('SONG_ID', StringType()),
    StructField('COUNT', IntegerType())
])

# Load triplets file
triplets = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("delimiter", "\t")
    .schema(schema_triplets)
    .load("hdfs:///data/msd/tasteprofile/triplets.tsv")
)

triplets.show(10,False)
#+----------------------------------------+------------------+-----+
#|USER_ID                                 |SONG_ID           |COUNT|
#+----------------------------------------+------------------+-----+
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOQEFDN12AB017C52B|1    |
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOQOIUJ12A6701DAA7|2    |
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOQOKKD12A6701F92E|4    |
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOSDVHO12AB01882C7|1    |
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOSKICX12A6701F932|1    |
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOSNUPV12A8C13939B|1    |
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOSVMII12A6701F92D|1    |
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOTUNHI12B0B80AFE2|1    |
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOTXLTZ12AB017C535|1    |
#|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|SOTZDDX12A6701F935|1    |
#+----------------------------------------+------------------+-----+

triplets.count() # 48373586



# Filter out the manually accepted matches
mismatches_not_accepted = (
    mismatches
    .join(
        matches_manually_accepted,
        on = "SONG_ID",
        how = "left_anti"
    )
)

# Filter out the mismatches
triplets_matches = (
    triplets
    .join(
        mismatches_not_accepted,
        on = "SONG_ID",
        how = "left_anti"
    )
)
triplets_matches.show(5,False)
#+------------------+----------------------------------------+-----+
#|SONG_ID           |USER_ID                                 |COUNT|
#+------------------+----------------------------------------+-----+
#|SOQEFDN12AB017C52B|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|1    |
#|SOQOIUJ12A6701DAA7|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|2    |
#|SOQOKKD12A6701F92E|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|4    |
#|SOSDVHO12AB01882C7|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|1    |
#|SOSKICX12A6701F932|f1bfc2a4597a3642f232e7a4e5d5ab2a99cf80e5|1    |
#+------------------+----------------------------------------+-----+


triplets_matches.count() # 45795111



# (b) 

import os
import subprocess


cmd = 'hdfs dfs -ls /data/msd/audio/attributes/'
files = str(subprocess.check_output(cmd, shell=True)).split(".csv")
filenames = []
for file in files[:-1]:
    path = file.split()[-1]
    filename = path.split("/")[-1]
    filenames.append(filename)
    
#filenames = ['msd-jmir-area-of-moments-all-v1.0.attributes',
# 'msd-jmir-lpc-all-v1.0.attributes',
# 'msd-jmir-methods-of-moments-all-v1.0.attributes',
# 'msd-jmir-mfcc-all-v1.0.attributes',
# 'msd-jmir-spectral-all-all-v1.0.attributes',
# 'msd-jmir-spectral-derivatives-all-all-v1.0.attributes',
# 'msd-marsyas-timbral-v1.0.attributes',
# 'msd-mvd-v1.0.attributes',
# 'msd-rh-v1.0.attributes',
# 'msd-rp-v1.0.attributes',
# 'msd-ssd-v1.0.attributes',
# 'msd-trh-v1.0.attributes',
# 'msd-tssd-v1.0.attributes']



def create_schema(filename):    
    attributes = (
        spark.read
        .format("com.databricks.spark.csv")
        .option("header", "false")
        .option("inferSchema", "true")
        .load("hdfs:///data/msd/audio/attributes/"+filename+".csv")
    )


    schema_df = (
        attributes
        .withColumn('datatype', 
            F.when((attributes._c1 == 'string') | (attributes._c1 == 'STRING'), "StringType()")
            .when((attributes._c1 == 'NUMERIC') | (attributes._c1 == 'real'), "DoubleType()")
            .otherwise("StringType()"))
        .select(['_c0','datatype'])
    )

    
    schema= StructType(
        [StructField(schema_df._c0, eval(schema_df.datatype)) \
        for (schema_df._c0, schema_df.datatype) in  schema_df.rdd.collect()]
    )

    return schema



schema_names = []
for filename in filenames:

    name = filename.replace("-","_").replace("_v1.0.attributes","")
    schema_names.append(name+"_schema")

    locals()[name+'_schema'] = create_schema(filename)


# schema_names = ['msd_jmir_area_of_moments_all_schema',
# 'msd_jmir_lpc_all_schema',
# 'msd_jmir_methods_of_moments_all_schema',
# 'msd_jmir_mfcc_all_schema',
# 'msd_jmir_spectral_all_all_schema',
# 'msd_jmir_spectral_derivatives_all_all_schema',
# 'msd_marsyas_timbral_schema',
# 'msd_mvd_schema',
# 'msd_rh_schema',
# 'msd_rp_schema',
# 'msd_ssd_schema',
# 'msd_trh_schema',
# 'msd_tssd_schema']


