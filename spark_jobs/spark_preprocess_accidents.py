from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, avg, lit, regexp_replace, lpad, to_timestamp, hour, row_number
)
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

def main():
    try:
        spark = (
            SparkSession.builder
                .appName("PreprocessAccidents")
                .master("spark://spark:7077")
                .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
                .config("spark.hadoop.fs.s3a.access.key", "admin")
                .config("spark.hadoop.fs.s3a.secret.key", "password")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "4g")
                .config("spark.sql.shuffle.partitions", "50")
                .getOrCreate()
        )

        # 1) Read raw Parquet
        df = spark.read.parquet("s3a://accidents/raw/")
        print(f"Loaded {df.count()} rows from s3a://accidents/raw/")

        # 2) Impute lat/long by department-level mean then global mean
        w_dep = Window.partitionBy("dep")
        df = df.withColumn(
                "lat",
                when(col("lat").isNull(), avg("lat").over(w_dep))
                .otherwise(col("lat"))
            ) \
               .withColumn(
                "long",
                when(col("long").isNull(), avg("long").over(w_dep))
                .otherwise(col("long"))
            )
        g = df.select(avg("lat").alias("g_lat"), avg("long").alias("g_long")).first()
        df = df.fillna({"lat": g["g_lat"], "long": g["g_long"]})

        # 3) Impute gps by department mode then global mode
        gps_counts = df.groupBy("dep","gps").count()
        w_gps = Window.partitionBy("dep").orderBy(col("count").desc())
        dep_gps_mode = (
            gps_counts
            .withColumn("rn", row_number().over(w_gps))
            .filter(col("rn")==1)
            .select("dep", col("gps").alias("mode_gps"))
        )
        df = df.join(dep_gps_mode, on="dep", how="left") \
               .withColumn(
                 "gps",
                 when(col("gps").isNull(), col("mode_gps"))
                 .otherwise(col("gps"))
               ).drop("mode_gps")
        global_gps = df.groupBy("gps") \
                       .count() \
                       .orderBy(col("count").desc()) \
                       .first()["gps"] or "Unknown"
        df = df.fillna({"gps": global_gps})

        # 4) Fill free-text / code columns
        df = df.withColumn(
                "adr",
                when(col("adr").isNull(), lit("adresse_inconnue"))
                .otherwise(col("adr"))
            ) \
               .withColumn(
                "com",
                when(col("com").isNull(), lit("00000"))
                .otherwise(col("com"))
            )
        top_dep = df.groupBy("dep").count().orderBy(col("count").desc()).first()["dep"] or "Unknown"
        df = df.withColumn(
                "dep",
                when(col("dep").isNull(), lit(top_dep))
                .otherwise(col("dep"))
            )

        # Cache DataFrame to reduce shuffle overhead
        df.cache()

        # 5) Feature-engineer hour of day from hrmn
        df = df.withColumn(
                "hrmn_clean",
                lpad(regexp_replace(col("hrmn").cast("string"), "\\D", ""), 4, "0")
            ) \
               .withColumn("hour", hour(to_timestamp("hrmn_clean", "HHmm"))) \
               .drop("hrmn_clean")

        # 6) One-hot encode the small-cardinality categorical columns
        cat_cols = ["lum","agg","int","atm","col"]
        indexers = [
            StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
            for c in cat_cols
        ]
        encoders = [
            OneHotEncoder(inputCols=[f"{c}_idx"], outputCols=[f"{c}_ohe"])
            for c in cat_cols
        ]
        pipeline = Pipeline(stages=indexers + encoders)
        df = pipeline.fit(df).transform(df).drop(*[f"{c}_idx" for c in cat_cols])

        # 7) Write cleaned, partitioned data
        df.write \
          .mode("overwrite") \
          .partitionBy("an","dep") \
          .parquet("s3a://processed/accidents_processed/")

        total = df.count()
        print(f"✅ Finished: wrote {total:,} rows to s3a://processed/accidents_processed/")
        df.unpersist()
        spark.stop()

    except Exception as e:
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()