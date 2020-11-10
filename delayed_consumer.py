from time import sleep
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime, timedelta


RETRY_DELAY = timedelta(minutes=1)
BOOTSTRAP_SERVER = 'localhost:9092'
TOPIC_NAME = 'app_RETRY1'


consumer = KafkaConsumer(TOPIC_NAME, group_id='retry1', bootstrap_servers=BOOTSTRAP_SERVER)
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)

for msg in consumer:
    msg_date = datetime.fromtimestamp(msg.timestamp / 1000)
    wait_until = msg_date + RETRY_DELAY

    # update number of retries
    headers = dict(msg.headers)
    retries = int(headers.get('retries', b'0').decode())
    retries += 1
    headers['retries'] = str(retries).encode()

    print('waiting to retry')
    if wait_until > datetime.now():
        consumer.pause()
        sleep((wait_until - datetime.now()).total_seconds())
        consumer.resume()

    print('retrying')
    producer.send(msg.topic, msg.value, headers=[('retries', b'1')])
