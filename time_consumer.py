import kafka
from time import sleep, strftime, gmtime
from os import environ
from prometheus_client import start_http_server, Counter

try:
    server = environ['KAFKA_SERVER']
    if len(server) < 7:
        raise Exception
except:
    print('Variable KAFKA_SERVER is not set', flush=True)
    exit()

def create_topic(server, topic_name = 'output'):
    kafka_connector = kafka.admin.KafkaAdminClient( bootstrap_servers= server)
    kafka_topic = kafka.admin.NewTopic(
        name=topic_name,
        num_partitions=1,
        replication_factor=1
    )
    kafka_connector.create_topics(new_topics=[kafka_topic], validate_only=False)

def check_input_topic(server, topic_name = 'input'):
    kafka_connector = kafka.KafkaConsumer(bootstrap_servers = server)

    while True:
        set_of_topics = kafka_connector.topics()
        if topic_name in set_of_topics:
            break
        else:
            print('Topic "{}" is not exist.' .format(topic_name), flush=True)
            sleep(60)


def check_output_topic(server, topic_name = 'output'):
    kafka_connector = kafka.KafkaConsumer(bootstrap_servers = server)
    set_of_topics = kafka_connector.topics()
    if topic_name not in set_of_topics:
        create_topic(server)
        print('Topic "{}" has been created.' .format(topic_name), flush=True)

def kafka_process(server, topic_name = 'input'):
    consumer = kafka.KafkaConsumer(
        topic_name,
        bootstrap_servers = server,
        auto_offset_reset = 'earliest',
        enable_auto_commit = True,
        group_id  = 'test-consumer-group'
    )
    for i in consumer:
        epoch_time = float(i.value.decode())
        write_the_data(server, epoch_time)
        prometheus_counts.inc()
    return list_of_epoch_time


def write_to_topic(server, rfc3339_time, topic_name = 'output'):
    producer = kafka.KafkaProducer(bootstrap_servers=server)
    producer.send(topic_name, key = b'rfc3339', value = rfc3339_time.encode())


def time_converor(epoch_time):
    rfc3339_time = strftime('%Y-%m-%dT%H:%M:%S%z', gmtime(epoch_time))
    return rfc3339_time


def write_the_data(server, epoch_time):
    rfc3339_time = time_converor(epoch_time)
    print(rfc3339_time, flush=True)
    write_to_topic(server, rfc3339_time)


def start_the_process(server):
    check_input_topic(server)
    check_output_topic(server)
    kafka_process(server)

start_http_server(5000)
prometheus_counts = Counter('consumer_convert', 'time converted')

while True:
    try:
        start_the_process(server)
    except Exception as err:
        print(err, flush=True)
        sleep(60)
