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
    .config("spark.executor.memory", "25g")
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

loc = spark.read.option("recursiveFileLookup", "true").parquet(f"abfss://propheus-data-science@propheusdatabay.dfs.core.windows.net/Thailand/Jan_location_data_w_admin_final/*")
loc = loc.withColumn("event_date",to_date(col("utc_timestamp")))

places_join = spark.read.option("recursiveFileLookup", "true").parquet("abfss://propheus-data-science@propheusdatabay.dfs.core.windows.net/Thailand/places_join/*")
places_join = places_join.select('id', 'geom_source_1', 'province_en', 'district_en', 'tambon_en', 'overture_places_id')


places_join = places_join.repartition(500, "province_en", "district_en", "tambon_en").persist()
places_join.count()

day_list = [
    r[0] for r in loc
    .select("event_date")
    .distinct()
    .orderBy("event_date")
    .toLocalIterator()
]

print(f"\nTotal Days: {len(day_list)}\n")


# --------------------------------------------------
# OUTER LOOP → DAY WISE
# --------------------------------------------------

for day in tqdm(day_list, desc="Processing Days"):

    print(f"\nStarting day: {day}")

    day_df = loc.filter(col("event_date") == day).persist()
    day_df.count()   # materialize once

    # ------------------------------------------
    # Get distinct adm1 for this day
    # ------------------------------------------

    adm1_list = [
        r["adm1_name"]
        for r in day_df
        .select("adm1_name")
        .distinct()
        .toLocalIterator()
    ]

    print(f"Total adm1 for {day}: {len(adm1_list)}")

    # ------------------------------------------
    # INNER LOOP → PROVINCE WISE
    # ------------------------------------------

    for adm1 in tqdm(adm1_list, desc=f"{day} provinces", leave=False):

        loc_subset = day_df.filter(
            col("adm1_name") == adm1
        )

        places_subset = places_join.filter(
            col("province_en") == adm1
        )

        if places_subset.limit(1).count() == 0:
            print(f"No buildings found for {day} for province {adm1}")
            continue

        joined = (
            loc_subset.alias("l")
            .join(
                places_subset.alias("p"),
                expr("ST_Intersects(l.point_geom, p.geom_source_1)")
            )
            .select(
                "l.caid",
                "l.utc_timestamp",
                "l.point_geom",
                "l.adm1_name",
                "l.adm2_name",
                "l.adm3_name",
                "p.id",
                "p.overture_places_id"
            )
        )

        joined.write.mode("overwrite").parquet(
            f"abfss://propheus-data-science@propheusdatabay.dfs.core.windows.net/Thailand/loc_places_join/date={day}/province={adm1}"
        )

    day_df.unpersist()

    print(f"Finished day: {day}")

