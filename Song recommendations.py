# DATA420 Assignment 2
# Jing Wu 29696576


# start_pyspark_shell -e 4 -c 2 -w 4 -m 4

# Imports

# Q1

# (a) How many unique songs and how many unique users

# unique songs
uniq_songs = (
    triplets_matches
    .select("SONG_ID")
    .dropDuplicates()
)

uniq_songs.count() # 378310



# unique users
uniq_users = (
    triplets_matches
    .select("USER_ID")
    .dropDuplicates()
)

uniq_users.count() # 1019318



# (b)

# Total number of songs that each user played
active_user = (
	triplets_matches
	.groupBy("USER_ID")
	.agg({"COUNT":"sum"})
	.select(
		F.col("USER_ID"),
		F.col("sum(COUNT)").alias("TOTAL_COUNT")
	)
	.orderBy('TOTAL_COUNT', ascending=False)
) 

active_user.show(5,False)

#+----------------------------------------+-----------+
#|USER_ID                                 |TOTAL_COUNT|
#+----------------------------------------+-----------+
#|093cb74eb3c517c5179ae24caf0ebec51b24d2a2|13074      |
#|119b7c88d58d0c6eb051365c103da5caf817bea6|9104       |
#|3fa44653315697f42410a30cb766a4eb102080bb|8025       |
#|a2679496cd0af9779a92a13ff7c6af5c81ea8c7b|6506       |
#|d7d2d888ae04d16e994d6964214a1de81392ee04|6190       |
#+----------------------------------------+-----------+


# Filter the most active user
song_list_most_active = (
	triplets_matches
	.where(
		triplets_matches.USER_ID == "093cb74eb3c517c5179ae24caf0ebec51b24d2a2"
	)
)

song_list_most_active.count() # 195




#(c)

# Visualize the distribution of user activity

import matplotlib.pyplot as plt


active_dist = active_user.toPandas()


plt.figure(figsize=(18, 8), dpi=80)
plt.subplot(1, 1, 1)

plt.hist(active_dist.TOTAL_COUNT, bins = 300, color = 'steelblue', edgecolor = 'k', label = 'Distribution of user activity')
plt.xlabel('Total Play Count')
plt.ylabel('Count of Users')
plt.title('Distribution of user activity')

plt.legend(loc="upper right")
plt.tight_layout()
plt.savefig("M:\\activity_dist.png") 



# distribution of song popularity
song_popularity = (
	triplets_matches
	.groupBy("SONG_ID")
	.agg({"COUNT":"sum"})
	.select(
		F.col("SONG_ID"),
		F.col("sum(COUNT)").alias("TOTAL_COUNT")
	)
	.orderBy('TOTAL_COUNT', ascending=False)
)

song_popularity.show(5,False)
#+------------------+-----------+
#|SONG_ID           |TOTAL_COUNT|
#+------------------+-----------+
#|SOBONKR12A58A7A7E0|726885     |
#|SOSXLTC12AF72A7F54|527893     |
#|SOEGIYH12A6D4FC0E3|389880     |
#|SOAXGDH12A8C13F8A1|356533     |
#|SONYKOW12AB01849C9|292642     |
#+------------------+-----------+



# Visualize the distribution of song popularity

song_popular = song_popularity.toPandas()


plt.figure(figsize=(18, 8), dpi=80)
plt.subplot(1, 1, 1)

plt.hist(song_popular.TOTAL_COUNT, bins = 300, color = 'steelblue', edgecolor = 'k', label = 'Distribution of song popularity')
plt.xlabel('Total Play Count')
plt.ylabel('Count of Songs')
plt.title('Distribution of song popularity')

plt.legend(loc="upper right")
plt.tight_layout()
plt.savefig("M:\\song_popularity.png") 


#(d) 

import numpy as np


N = active_user.toPandas().quantile(0.25)
print(N)

#TOTAL_COUNT    32.0
#Name: 0.25, dtype: float64

M = song_popularity.toPandas().quantile(0.25)
print(M)

#TOTAL_COUNT    8.0
#Name: 0.25, dtype: float64


user_less = (
	active_user
	.where(
		active_user.TOTAL_COUNT < 32
	)
	.orderBy('TOTAL_COUNT', ascending=False)
)

user_less.show(5)
#+--------------------+-----------+
#|             USER_ID|TOTAL_COUNT|
#+--------------------+-----------+
#|902ec26d46b80f750...|         31|
#|f6767b867b8cbd596...|         31|
#|2cab17056bda991ff...|         31|
#|f7e257f72a59fb9e2...|         31|
#|0b6e359ef6e4d879c...|         31|
#+--------------------+-----------+



user_less.count() # 254739




song_less = (
	song_popularity
	.where(
		song_popularity.TOTAL_COUNT < 8
	)
	.orderBy('TOTAL_COUNT', ascending=False)
)

song_less.show(5)

#+------------------+-----------+
#|           SONG_ID|TOTAL_COUNT|
#+------------------+-----------+
#|SOOFOEY12AF729DD40|          7|
#|SOGOAHQ12AF72A4A56|          7|
#|SOGEKVP12A8C139C56|          7|
#|SOWVHHP12A8C134107|          7|
#|SOMWSMW12A8C13F8BA|          7|
#+------------------+-----------+


song_less.count() # 90750



user_song = (
	triplets_matches
	.join(
		user_less,
		on = "USER_ID",
		how = "left_anti"
	)
	.join(
		song_less,
		on = "SONG_ID",
		how = "left_anti"
	)
)

user_song.count() #  41978747



	
#(e)
sum_number = triplets_matches.agg({"COUNT":"sum"}).collect()[0]
total_plays = sum_number["sum(COUNT)"] 
print(total_plays) # 131312858


def split_Data(dataset,seed):
	for i in range(seed):
		(a, b) = dataset.randomSplit([0.7, 0.3],i)


		fractions = (
		    b
		    .select("USER_ID")
		    .distinct()
		    .withColumn("fraction", F.lit(0.25))
		    .rdd
		    .collectAsMap()
		)


		test = (
		    dataset
		    .stat
		    .sampleBy("USER_ID", fractions)
		)


		test_plays =test.agg({"COUNT":"sum"}).collect()[0]
		total_test_plays = test_plays["sum(COUNT)"]

		if 0.3 * total_plays >total_test_plays > 0.2 * total_plays:
			break
		else:
			continue

	training = (
		dataset
		.join(
		test,
      	on = ["SONG_ID","USER_ID"],
      	how = "left_anti")
	)

	return training, test

trainingData,testData = split_Data(user_song, 10)





# Q2

#(a)
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline


user_stringIdx = StringIndexer(inputCol = "USER_ID", outputCol = "USER_INDEX")
song_stringIdx = StringIndexer(inputCol = "SONG_ID", outputCol = "SONG_INDEX")

pipeline1 = Pipeline(stages=[user_stringIdx,song_stringIdx])

# Fit the pipeline to training 
pipelineFit = pipeline1.fit(user_song)
dataset1 = pipelineFit.transform(user_song)


trainingData1,testData1 = split_Data(dataset1, 10)


als = ALS(maxIter=5, regParam=0.01, implicitPrefs = True, userCol="USER_INDEX", itemCol="SONG_INDEX", ratingCol="COUNT")
ALSModel = als.fit(trainingData1)


# Generate top 10 recommendations for 10 SELECTED users

users = testData1.select(["USER_INDEX"]).distinct().limit(5)
users.cache()



userRecs = ALSModel.recommendForUserSubset(users, 10)
userRecs.cache()
userRecs.show(5,False)

#+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
#|USER_INDEX|recommendations                                                                                                                                                                            |
#+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
#|6575      |[[10, 0.36875355], [31, 0.3682614], [91, 0.32585025], [42, 0.31243947], [105, 0.31233838], [94, 0.30951855], [95, 0.30716902], [52, 0.29921487], [82, 0.29818088], [6, 0.29085243]]        |
#|251775    |[[31, 0.30811918], [141, 0.23697475], [59, 0.19655953], [85, 0.19566129], [90, 0.17769632], [62, 0.17086054], [139, 0.16889596], [76, 0.1675975], [83, 0.1624386], [119, 0.1578668]]       |
#|370316    |[[31, 0.20638654], [141, 0.16759032], [85, 0.16335368], [62, 0.124417745], [59, 0.11877337], [83, 0.11612449], [76, 0.11461151], [333, 0.10954098], [119, 0.099708945], [139, 0.099168144]]|
#|4784      |[[31, 0.6720965], [85, 0.5774308], [52, 0.5640661], [141, 0.52617323], [62, 0.4972302], [0, 0.47666457], [17, 0.46911415], [57, 0.46587792], [4, 0.4279694], [333, 0.42653036]]            |
#|202815    |[[17, 0.62614405], [57, 0.60602796], [4, 0.59239095], [83, 0.5881185], [131, 0.5542552], [175, 0.5126759], [212, 0.50955105], [31, 0.5082299], [113, 0.47570002], [51, 0.44544482]]        |
#+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

# Users actually played 

actual_played = (
	dataset1
	.where(dataset1.USER_INDEX.isin([row.USER_INDEX for row in users.select(["USER_INDEX"]).collect()])
	)
)



# (c)
from pyspark.sql import Window
from pyspark.mllib.evaluation import RankingMetrics


predictions = ALSModel.transform(testData1)
predictions.show(5,False)
#+------------------+--------------------+-----+----------+----------+----------+
#|           SONG_ID|             USER_ID|COUNT|USER_INDEX|SONG_INDEX|prediction|
#+------------------+--------------------+-----+----------+----------+----------+
#|SOHTKMO12AB01843B0|71b73a931076a4e2c...|    2|    1111.0|      12.0|0.45784798|
#|SOHTKMO12AB01843B0|12eb2aaf2b88b944b...|    2|    1140.0|      12.0|0.45242503|
#|SOHTKMO12AB01843B0|33fe3afeaa73f1fe1...|    2|    1942.0|      12.0| 0.6361546|
#|SOHTKMO12AB01843B0|4c13ca9fc4a07da19...|    1|    2384.0|      12.0|0.33053803|
#|SOHTKMO12AB01843B0|2be94fe8976330172...|   17|    3368.0|      12.0| 0.7165724|
#+------------------+--------------------+-----+----------+----------+----------+


# Get the prediction list of each user
w1 = Window.partitionBy("USER_INDEX").orderBy(F.col("prediction").desc())

prediction_list = (
	predictions
	.withColumn("pred_list", F.collect_list("SONG_INDEX").over(w1))
	.groupBy("USER_INDEX")
	.agg(F.max("pred_list").alias("pred_list"))
)


# Get actual played songs list
w2 = Window.partitionBy("USER_INDEX").orderBy(F.col("COUNT").desc())

played = (
	dataset1
	.withColumn("actual_played", F.collect_list("SONG_INDEX").over(w2))
	.groupBy("USER_INDEX")
	.agg(F.max("actual_played").alias("actual_played"))
)


# Join the predicted songs list and the actual played songs list
predictionAndLabels = (
	prediction_list
	.select(["USER_INDEX","pred_list"])
	.join(
		played
		.select(["USER_INDEX","actual_played"]),
		on = "USER_INDEX",
		how = "left"
	)
	.select(["pred_list","actual_played"])
)
predictionAndLabels.show(5)
#+--------------------+--------------------+
#|           pred_list|       actual_played|
#+--------------------+--------------------+
#|[130.0, 291.0, 28...|[141386.0, 5430.0...|
#|[10.0, 2.0, 12.0,...|[46321.0, 5345.0,...|
#|[87.0, 1233.0, 50...|[61979.0, 49772.0...|
#|[141.0, 153.0, 42...|[46662.0, 6892.0,...|
#|[5.0, 133.0, 6.0,...|[6490.0, 3178.0, ...|
#+--------------------+--------------------+



# Create the ranking metrics
metrics = RankingMetrics(predictionAndLabels.rdd)




metrics.precisionAt(5)
# 0.9151640769944318


metrics.ndcgAt(10)
# 0.8194191765714083



metrics.meanAveragePrecision
#0.251494240714216




# Q3
metadata = (
    spark.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("hdfs:///data/msd/main/summary/metadata.csv.gz")
)

metadata.show(5,False)

#+----------------+-----------------+-------------------+-------------------+------------------+---------------+-----------------+----------------+------------------------------------+----------------+---------------+-----+----------------+-------------------+------------------------------------+------------------+------------------+------------------+-----------------+----------------+
#|analyzer_version|artist_7digitalid|artist_familiarity |artist_hotttnesss  |artist_id         |artist_latitude|artist_location  |artist_longitude|artist_mbid                         |artist_name     |artist_playmeid|genre|idx_artist_terms|idx_similar_artists|release                             |release_7digitalid|song_hotttnesss   |song_id           |title            |track_7digitalid|
#+----------------+-----------------+-------------------+-------------------+------------------+---------------+-----------------+----------------+------------------------------------+----------------+---------------+-----+----------------+-------------------+------------------------------------+------------------+------------------+------------------+-----------------+----------------+
#|null            |4069             |0.6498221002008776 |0.3940318927141434 |ARYZTJS1187B98C555|null           |null             |null            |357ff05d-848a-44cf-b608-cb34b5701ae5|Faster Pussy cat|44895          |null |0               |0                  |Monster Ballads X-Mas               |633681            |0.5428987432910862|SOQMMHC12AB0180CB8|Silent Night     |7032331         |
#|null            |113480           |0.4396039666767154 |0.3569921077564064 |ARMVN3U1187FB3A1EB|null           |null             |null            |8d7ef530-a6fd-4f8f-b2e2-74aec765e0f9|Karkkiautomaatti|-1             |null |0               |0                  |Karkuteillä                         |145266            |0.2998774882739778|SOVFVAK12A8C1350D9|Tanssi vaan      |1514808         |
#|null            |63531            |0.6436805720579895 |0.4375038365946544 |ARGEKB01187FB50750|55.8578        |Glasgow, Scotland|-4.24251        |3d403d44-36ce-465c-ad43-ae877e65adc4|Hudson Mohawke  |-1             |null |0               |0                  |Butter                              |625706            |0.6178709693948196|SOGTUKN12AB017F4F1|No One Could Ever|6945353         |
#|null            |65051            |0.44850115965646636|0.37234906851712235|ARNWYLR1187B9B2F9C|null           |null             |null            |12be7648-7094-495f-90e6-df4189d68615|Yerba Brava     |34000          |null |0               |0                  |De Culo                             |199368            |null              |SOBNYVR12A8C13558C|Si Vos Querés    |2168257         |
#|null            |158279           |0.0                |0.0                |AREQDTE1269FB37231|null           |null             |null            |null                                |Der Mystic      |-1             |null |0               |0                  |Rene Ablaze Presents Winter Sessions|209038            |null              |SOHSBXH12A8C13B0DF|Tangle Of Aspens |2264873         |
#+----------------+-----------------+-------------------+-------------------+------------------+---------------+-----------------+----------------+------------------------------------+----------------+---------------+-----+----------------+-------------------+------------------------------------+------------------+------------------+------------------+-----------------+----------------+

