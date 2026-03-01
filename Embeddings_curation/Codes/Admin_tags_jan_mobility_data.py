from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from tqdm import tqdm


from pyspark.sql.functions import *

ACCOUNT = "propheusdatabay.dfs.core.windows.net"

spark = (
    SparkSession.builder

    .config(f"fs.azure.account.auth.type.{ACCOUNT}", "OAuth")
    .config(
        f"fs.azure.account.oauth.provider.type.{ACCOUNT}",
        "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider"
    )
    .config(
        f"fs.azure.account.oauth2.msi.endpoint.{ACCOUNT}",
        "http://169.254.169.254/metadata/identity/oauth2/token"
    )
    .config(f"fs.azure.account.oauth2.msi.tenant.{ACCOUNT}", "3f50e9d5-8877-43f0-bc25-fcecafe59ee5")
    .config(f"fs.azure.account.oauth2.client.id.{ACCOUNT}", "322c41c6-38f5-4894-82a1-e955df89ff85")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "20g")
    .config("spark.executor.memoryOverhead", "8g")

    # Kryo (MANDATORY for Sedona)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator")

    # Shuffle tuning
    .config("spark.sql.shuffle.partitions", "2000")
    .config("spark.default.parallelism", "2000")

    # Adaptive execution
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")

    # VERY IMPORTANT
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # disable auto
    .config("spark.sql.parquet.enableVectorizedReader", "false")

    .getOrCreate()
)

SedonaRegistrator.registerAll(spark)

spark.conf.set("sedona.join.spatialPartitioning", "kdbtree")
spark.conf.set("sedona.join.index", "true")
spark.conf.set("sedona.join.indexType", "quadtree")

loc_jan_df = spark.read.option("recursiveFileLookup", "true").parquet('abfss://propheus-data-staging@propheusdatabay.dfs.core.windows.net/propheus-veraset-data/movement_v2/2026/*')

print(loc_jan_df.count())

loc_points = loc_jan_df.select("caid", "utc_timestamp", "latitude", "longitude").filter(
    col("latitude").isNotNull() & col("longitude").isNotNull()
    )

loc_points = loc_points.withColumn(
    "point_geom",
    expr("ST_Point(CAST(longitude AS DOUBLE), CAST(latitude AS DOUBLE))")
).drop('latitude', 'longitude')

loc_points = loc_points.withColumn("event_date",to_date(col("utc_timestamp")))

adm3_geom = spark.read.parquet('abfss://propheus-data-science@propheusdatabay.dfs.core.windows.net/Thailand/geometries_parquet/admin3_geom')
adm3 = adm3_geom.select("adm3_name", "adm2_name", "adm1_name", "adm3_pcode", "geometry")

adm3 = adm3.persist()
print(adm3.count())

days = (
    loc_points
    .select("event_date")
    .distinct()
    .orderBy("event_date")
    .collect()
)

day_list = [row["event_date"] for row in days]

print(f"\nTotal Days To Process: {len(day_list)}\n")

for day in tqdm(day_list, desc="Processing Days"):

    print(f"\nStarting day: {day}")

    day_df = loc_points.filter(col("event_date") == day)

    day_df = day_df.repartition(2000)

    joined = day_df.alias("p").join(
        adm3.alias("a"),
        expr("ST_Intersects(p.point_geom, a.geometry)")
    ).select(
        "p.caid",
        "p.utc_timestamp",
        "p.point_geom",
        "a.adm3_name",
        "a.adm2_name",
        "a.adm1_name",
        "a.adm3_pcode"
    )

    joined.write.mode("overwrite").parquet(f"abfss://propheus-data-science@propheusdatabay.dfs.core.windows.net/Thailand/Jan_location_data_w_admin_final/date={day}")
    print(f"Finished day: {day}")
