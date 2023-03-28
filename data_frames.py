import pyspark
from pyspark.sql import SparkSession
import findspark
findspark.init()


spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.driver.extraClassPath", "C:/Program Files/PostgreSQL/15/driver/postgresql-42.6.0.jar") \
    .getOrCreate()



df_actor = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/postgres") \
    .option("dbtable", "actor") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_address = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/postgres") \
    .option("dbtable", "address") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_category = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/postgres") \
    .option("dbtable", "category") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_city = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/postgres") \
    .option("dbtable", "city") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_country = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/postgres") \
    .option("dbtable", "country") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_customer = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/postgres") \
    .option("dbtable", "customer") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_film = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/postgres") \
    .option("dbtable", "film") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_film_actor = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/postgres") \
    .option("dbtable", "film_actor") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_film_category = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/postgres") \
    .option("dbtable", "film_category") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_inventory = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/postgres") \
    .option("dbtable", "inventory") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_language = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/postgres") \
    .option("dbtable", "language") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_payment = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/postgres") \
    .option("dbtable", "payment") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_rental = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/postgres") \
    .option("dbtable", "rental") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_staff = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/postgres") \
    .option("dbtable", "staff") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df_store = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5433/postgres") \
    .option("dbtable", "store") \
    .option("user", "postgres") \
    .option("password", "123456") \
    .option("driver", "org.postgresql.Driver") \
    .load()
