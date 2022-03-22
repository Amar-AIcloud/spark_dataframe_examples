# Apply your own schema to the dataset


#code
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, avg, count , round

data_path1 = "C:\\PySpark\\data_git\\movielens\\moviesNoHeader.csv"
data_path2 = "C:\\PySpark\\data_git\\movielens\\ratingsNoHeader.csv"

spark = SparkSession \
    .builder \
    .appName("pyspark Assignment sql") \
    .config("spark.master", "local") \
    .getOrCreate()
   
######################################################  
# custom schema on a dataframe
######################################################  

from pyspark.sql.types import StructField, StructType, StringType, IntegerType,FloatType,TimestampType

movieSchema = StructType([
  StructField("Movieid", IntegerType(), False),
  StructField("Movie_Name", StringType(), False),
  StructField("Genere", StringType(), True)
])

df = spark.read.format("csv").schema(movieSchema).load(data_path1)
df.show(5)


ratingSchema = StructType([
  StructField("Userid", IntegerType(), False),
  StructField("Movieid", IntegerType(), False),
  StructField("Rating", FloatType(), True),
  StructField('Timestamp', TimestampType(), True)
])

df2 = spark.read.format("csv").schema(ratingSchema).load(data_path2)
df2.show(5)

print("\n")
df2.printSchema()

summaryDf = df2 \
            .groupBy("Movieid") \
            .agg(count("Rating").alias("TotalnumRating"), round(avg("Rating"),2).alias("AverageRating")) \
            .filter("TotalnumRating > 10") \
            .orderBy(desc("AverageRating")) \
            .limit(20)
                  
joinStr = summaryDf["Movieid"] == df["Movieid"]
    
summaryDf2 = summaryDf.join(df, joinStr) \
                .drop(summaryDf["Movieid"]) \
                .select("Movieid", "Movie_Name", "TotalnumRating", "AverageRating") \
                .orderBy(desc("AverageRating")) \
                .coalesce(1)



summaryDf2.show(5)

summaryDf2.printSchema()                    

#stop spak Session   
spark.stop()                 