from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name, collect_list, concat_ws,
    col, lower, regexp_replace
)
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
import numpy as np


# 1. Start Spark
spark = SparkSession.builder.appName("TFIDF_Book_Similarity").getOrCreate()

# 2. Load and combine book text

books_df = spark.read.text("/home/hp/books/D184MB/*.txt") \
    .withColumn("file_name", input_file_name()) \
    .withColumnRenamed("value", "line")

books_text_df = books_df.groupBy("file_name") \
    .agg(concat_ws(" ", collect_list("line")).alias("text"))


# 3. Text preprocessing

clean_df = books_text_df.withColumn(
    "clean_text",
    regexp_replace(lower(col("text")), "[^a-zA-Z ]", " ")
)

tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
words_df = tokenizer.transform(clean_df)

remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
filtered_df = remover.transform(words_df)

# 4. TF-IDF computation

hashingTF = HashingTF(
    inputCol="filtered_words",
    outputCol="raw_features",
    numFeatures=10000
)
tf_df = hashingTF.transform(filtered_df)

idf = IDF(inputCol="raw_features", outputCol="tfidf_features")
idf_model = idf.fit(tf_df)
tfidf_df = idf_model.transform(tf_df)


# 5. Cosine similarity

def cosine_similarity(v1, v2):
    v1 = np.array(v1.toArray())
    v2 = np.array(v2.toArray())
    return float(np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2)))

cosine_udf = udf(cosine_similarity, DoubleType())

# Reference book
ref_vector = tfidf_df.filter(col("file_name").contains("10.txt")) \
    .select("tfidf_features") \
    .first()[0]

similarity_df = tfidf_df.withColumn(
    "similarity",
    cosine_udf(col("tfidf_features"), ref_vector)
)


# 6. Top 5 similar books

similarity_df.select("file_name", "similarity") \
    .orderBy(col("similarity").desc()) \
    .show(6, truncate=False)
spark.stop()
