import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.cluster import Cluster

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        );
    """)
    print("Table created successfully!")

def insert_data(session, **kwargs):
    print("Inserting data...")
    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, registered_date, phone, picture)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")
    except Exception as e:
        logging.error(f'Could not insert data due to {e}')

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,com.datastax.spark:spark-cassandra-connector_2.12:3.1.0,org.apache.kafka:kafka-clients:2.8.1') \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn

def connect_to_kafka(spark):
    try:
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "users_created") \
            .option("startingOffsets", "earliest") \
            .load()
        logging.info("Connected to Kafka successfully!")
        return kafka_df
    except Exception as e:
        logging.error(f"Failed to connect to Kafka: {e}")
        return None

def process_kafka_df(kafka_df):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("address", StringType(), True),
        StructField("post_code", StringType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        StructField("registered_date", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("picture", StringType(), True)
    ])

    processed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    
    return processed_df

def write_to_cassandra(df, session):
    try:
        query = df.writeStream \
            .foreachBatch(lambda batch_df, batch_id: batch_df.foreach(lambda row: insert_data(session, **row.asDict()))) \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .start()
        query.awaitTermination()
    except Exception as e:
        logging.error(f"Failed to write to Cassandra: {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    spark = create_spark_connection()
    if spark:
        kafka_df = connect_to_kafka(spark)
        if kafka_df:
            cassandra_session = Cluster(['localhost']).connect('spark_streams')
            create_keyspace(cassandra_session)
            create_table(cassandra_session)
            processed_df = process_kafka_df(kafka_df)
            write_to_cassandra(processed_df, cassandra_session)
        else:
            logging.error("Unable to connect to Kafka.")
    else:
        logging.error("Unable to create Spark session.")
