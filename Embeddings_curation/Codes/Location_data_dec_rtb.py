import sys
sys.path.append('/home/azureuser/prathyusha/Kearney/prathyusha')
from utils import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = instantiate_spark_sedona("100g", "10g")

spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")

spark.conf.set("spark.sql.parquet.mergeSchema", "False")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")


loc = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet("abfss://propheus-data-staging@propheusdatabay.dfs.core.windows.net/kearney/propheus-rtb-data/sample-data/*")
)

loc = loc.withColumn("timestamp_ist", to_timestamp(col("timestamp") / 1000))

buil = spark.read.parquet('s3a://propheus-data-staging/Thailand/Gee_Buildings/overture_tiles_merged_buildings.parquet')

buil_geo = buil.select(
    col("id_a"),
    expr("ST_GeomFromWKT(geom_source_1)").alias("buil_geom")
).repartition(200)

spark.conf.set("spark.sql.adaptive.enabled", "true")

spark.conf.set("sedona.join.gridtype", "kdbtree")
spark.conf.set("sedona.join.numpartition", "800")

spark.conf.set("sedona.global.index", "true")
spark.conf.set("sedona.global.indextype", "rtree")


loc_with_date = loc.withColumn(
    "event_date",
    to_date(col("timestamp_ist"))
)

# date_counts = (
#     loc_with_date
#     .groupBy("event_date")
#     .agg(count("*").alias("rows"))
#     .orderBy(col("rows").asc())   # smallest days first
# )

# date_counts.show()   # optional sanity check

# dates = [r.event_date for r in date_counts.select("event_date").collect()]

largest_date = (
    loc_with_date
    .groupBy("event_date")
    .count()
    .orderBy(col("count").desc())
    .select("event_date")
    .first()["event_date"]
)



# print(f"Processing {len(dates)} days")


# for d in dates:

#     print(f"Processing {d}")

#     loc_day = (
#         loc_with_date.drop("consent_string")
#         .filter(col("event_date") == d)
#         .withColumn("point_geom", expr("ST_Point(longitude, latitude)"))
#         .repartition(300)
#     )

#     joined_day = loc_day.join(
#         buil_geo,
#         expr("ST_Within(point_geom, buil_geom)"),
#         "inner"
#     ).drop("point_geom")

#     # print(joined_day.count())


#     joined_day = joined_day.repartition(200) 

#     joined_day.write.mode("overwrite").parquet(f'abfss://propheus-data-science@propheusdatabay.dfs.core.windows.net/Thailand/Buildings_w_rtb_dec_loc_data/date={d}')
#     rows = joined_day.count()

#     print(f"✅ Written day {d} data successfully — {rows} rows")    

# print("✅ All days processed")


d = largest_date
print(f"Processing big day: {d}")

# Filter day once (important — don't redo this 12 times)
loc_day_base = (
    loc_with_date
    .drop("consent_string")
    .filter(col("event_date") == d)
)

# Add batch number 1..12
w = Window.orderBy("timestamp_ist")   # or any stable column (id, timestamp, etc)

loc_day_batched = loc_day_base.withColumn(
    "batch_id",
    ntile(12).over(w)
)

# Process each batch separately
for b in range(1, 13):

    print(f"➡ Processing batch {b}/12")

    loc_batch = (
        loc_day_batched
        .filter(col("batch_id") == b)
        .withColumn("point_geom", expr("ST_Point(longitude, latitude)"))
        .repartition(200)
    )

    print(f"Batch row count : {loc_batch.count()}")

    joined_batch = loc_batch.join(
        buil_geo,
        expr("ST_Within(point_geom, buil_geom)"),
        "inner"
    ).drop("point_geom", "batch_id")

    joined_batch.write.mode("overwrite").parquet(
        f"abfss://propheus-data-science@propheusdatabay.dfs.core.windows.net/"
        f"Thailand/Buildings_w_rtb_dec_loc_data/date={d}/part={b}"
    )

    rows = joined_batch.count()
    print(f"✅ Batch {b} done — {rows} rows")

print("🔥 Huge day fully processed in chunks")


