from kafka import KafkaConsumer, KafkaProducer


BOOTSTRAP_SERVER = 'localhost:9092'
TOPIC_NAME = 'app_RETRY1'

consumer = KafkaConsumer('foobar', group_id='my_favorite_group', bootstrap_servers=BOOTSTRAP_SERVER)
producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)

for msg in consumer:
    print(msg)
    headers = dict(msg.headers)
    retries = int(headers.get('retries', b'0').decode())
    if retries == 0:
        print('Sending to retry')
        producer.send('foobar_RETRY1', msg.value, headers=msg.headers)
    else:
        print('success')
