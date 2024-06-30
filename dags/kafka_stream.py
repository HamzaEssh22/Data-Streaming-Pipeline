from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import uuid
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
import time  # Add this line to import time module

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_data():
    res = requests.get("https://randomuser.me/api/")
    res.raise_for_status()
    res = res.json()
    res = res['results'][0]
    return res

def format_data(res):
    location = res['location']
    data = {
        'id': str(uuid.uuid4()),
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'gender': res['gender'],
        'address': f"{location['street']['number']} {location['street']['name']}, "
                   f"{location['city']}, {location['state']}, {location['country']}",
        'post_code': location['postcode'],
        'email': res['email'],
        'username': res['login']['username'],
        'dob': res['dob']['date'],
        'registered_date': res['registered']['date'],
        'phone': res['phone'],
        'picture': res['picture']['medium']
    }
    return data

def stream_data():
    kafka_server = 'broker:29092'
    kafka_topic = 'users_created'

    producer = KafkaProducer(
        bootstrap_servers=[kafka_server],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        max_block_ms=5000
    )
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: # 1 minute
            break
        try:
            res = get_data()
            formatted_data = format_data(res)

            producer.send(kafka_topic, formatted_data)
            logger.info("Sent data to Kafka")
        except Exception as e:
            logger.error(f'An error occurred: {e}')
            continue

        time.sleep(1)

    producer.close()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 17)  # Adjusted start_date
}

with DAG('user_created',
         default_args=default_args,
         schedule_interval='*/1 * * * *',  # Run every minute
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
