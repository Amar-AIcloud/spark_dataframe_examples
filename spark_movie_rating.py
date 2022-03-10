'''1.From the given movies.csv and ratings.csv datasets find out the top 20 movies with the 
highest average user ratings from those movies that received at least 10 user reviews. 
Show the following data in the output: Movie Id, Movie Name, Average Rating & Total 
Number of Ratings. 
'''
#code - using dataframe method
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, avg, count , round

data_path1 = "C:\\PySpark\\data_git\\movielens\\movies.csv"
data_path2 = "C:\\PySpark\\data_git\\movielens\\ratings.csv"

spark = SparkSession \
    .builder \
    .appName("Spark Assignment sql") \
    .config("spark.master", "local") \
    .getOrCreate()
    
moviesdf = spark.read.format("csv")\
                    .option("header","true")\
                    .load(data_path1)
ratingsdf = spark.read.format("csv")\
                    .option("header","true")\
                    .load(data_path2)
moviesdf.show(5)     
ratingsdf.show(5)

summaryDf = ratingsdf \
            .groupBy("movieId") \
            .agg(count("rating").alias("ratingCount"), round(avg("rating"),2).alias("ratingAvg")) \
            .filter("ratingCount > 10") \
            .orderBy(desc("ratingAvg")) \
            .limit(20)
              
summaryDf.show(5)
    
joinStr = summaryDf["movieId"] == moviesdf["movieId"]
    
summaryDf2 = summaryDf.join(moviesdf, joinStr) \
                .drop(summaryDf["movieId"]) \
                .select("movieId", ("title"), "ratingCount", "ratingAvg") \
                .orderBy(desc("ratingAvg")) \
                .coalesce(1)

summaryDf2.show(5)

output = summaryDf2.selectExpr("movieId as MovieID","title as Movie_Name",
                               "ratingAvg as AverageRating","ratingCount as TotalnumRating")
output.show(5)

spark.stop()