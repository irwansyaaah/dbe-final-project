import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys

# Inisialisasi Spark
sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(pyspark.SparkConf().setAppName("Dibimbing"))
)
sparkcontext.setLogLevel("WARN")
spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# Path CSV (bisa dari argumen atau default)
csv_path = sys.argv[1] if len(sys.argv) > 1 else "/data/smoking.csv"

# Definisikan schema sesuai kolom CSV
smoking_schema = StructType([
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("marital_status", StringType(), True),
    StructField("highest_qualification", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("ethnicity", StringType(), True),
    StructField("gross_income", StringType(), True),
    StructField("region", StringType(), True),
    StructField("smoke", StringType(), True),
    StructField("amt_weekends", StringType(), True),
    StructField("amt_weekdays", StringType(), True),
    StructField("type", StringType(), True),
])

# Baca CSV ke DataFrame
df = spark.read.csv(
    csv_path,
    header=True,
    schema=smoking_schema,
    mode="DROPMALFORMED"
)

# Optional: Ganti 'NA', 'Refused', 'Unknown' jadi None/null
for col in df.columns:
    df = df.replace('NA', None, subset=[col])
    df = df.replace('Refused', None, subset=[col])
    df = df.replace('Unknown', None, subset=[col])

# Konfigurasi koneksi Postgres
pg_url = "jdbc:postgresql://localhost:5433/postgres"  # Ganti sesuai kebutuhan
pg_properties = {
    "user": "user",
    "password": "password",
    "driver": "org.postgresql.Driver"
}
table_name = "smoking_staging"  # Ganti sesuai kebutuhan

# Tulis ke Postgres
df.write.jdbc(
    url=pg_url,
    table=table_name,
    mode="append",  # atau "overwrite"
    properties=pg_properties
)

spark.stop()