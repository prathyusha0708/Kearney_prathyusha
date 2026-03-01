from pyspark.sql.functions import *
from pyspark.sql.types import *
import geohash
import mercantile

def instantiate_spark_sedona(driver_mem = "10g", driver_maxResultsie = "2g", partitions = "200", maxPartitionBytes =  "256m"):
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession
    from sedona.spark import SedonaContext
    import os
    from dotenv import load_dotenv
    load_dotenv()
    
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
    
    # Configure Spark settings including driver memory and Azure authentication options.
    conf = SparkConf() \
        .set("spark.driver.memory", driver_mem) \
        .set("spark.driver.maxResultSize", driver_maxResultsie) \
        .set("spark.sql.shuffle.partitions", partitions) \
        .set("spark.sql.files.maxPartitionBytes", maxPartitionBytes) \
        .set("fs.azure.account.auth.type.propheusdatabay.dfs.core.windows.net", "OAuth") \
        .set("fs.azure.account.oauth.provider.type.propheusdatabay.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider") \
        .set("fs.azure.account.oauth2.msi.tenant", "3f50e9d5-8877-43f0-bc25-fcecafe59ee5") \
        .set("fs.azure.account.oauth2.client.id", "322c41c6-38f5-4894-82a1-e955df89ff85") \
        .set("fs.azure.account.oauth2.client.endpoint", "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-0201&resource=https://storage.azure.com/") \
        .set("fs.azure.account.oauth2.msi.endpoint", "http://169.254.169.254/metadata/identity/oauth2/token") \
        .set("fs.azure.account.oauth2.use.metadata.header", "true") \
        .set("sedona.global.index", "true") \
        .set("sedona.global.indextype", "rtree") \
        .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
        .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
        .set("spark.local.dir", "/dev/shm/spark-temp")
        
    
    # Create Spark Context
    sc = SparkContext(conf=conf)
    
    # Create SparkSession
    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .appName("SedonaApp") \
        .getOrCreate()
    
    # Initialize Sedona Context (this registers Sedona's SQL functions and spatial types)
    spark = SedonaContext.create(spark)
    
    # Confirm by printing Spark and Sedona contexts
    print("Spark Session and SedonaContext have been successfully initiated.")
    return spark

@udf(StringType())
def quadkey_to_wkt_geometry(qk):
    import mercantile
    from shapely.geometry import shape
    if not qk:
        return None
    try:

        tile_feature = mercantile.feature(mercantile.quadkey_to_tile(qk))
        return shape(tile_feature['geometry']).wkt
    except:
        return None
@udf(StringType())
def lat_lon_to_quadkey(lat: float, lon: float, zoom: int) -> str:
    tile = mercantile.tile(lon, lat, zoom)
    quadkey = mercantile.quadkey(tile)
    return f"{quadkey}"

@udf(StringType())
def geohash_encode(lat, lon, precision):
    if lat is None or lon is None:
        return None
    return geohash.encode(lat, lon, precision)