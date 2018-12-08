import pytest

from datetime import datetime, timedelta
from multiprocessing import Process
from random import random
import json
import time

from pyedj.ingestion.router import Router

from mqtt_client import MqttClient

# Start mqtt broker
# sudo docker run -it -h mqtt_broker --name mqtt_broker --rm -p 1883:1883 -p 9001:9001 eclipse-mosquitto


def get_msgs(start_time, interval_sec, num):
    msgs = []

    for i in range(num):
        pir_num = random() * 100
        temp_num = random() * 100
        timestamp = start_time + timedelta(seconds=(i*interval_sec))
        msg = {
                'timestamp': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                'pir': pir_num,
                'temp': temp_num}
        msgs.append(msg)

    return msgs


def send_mqtt_msgs(service_info, msgs, interval_sec):
    client = MqttClient()
    client.connect(service_info['host'], service_info['protocol']['port'], 60)

    for msg in msgs:
        client.publish(service_info['protocol']['publish']['default_topic'], json.dumps(msg))
        time.sleep(interval_sec)


def spawn_test_client(service_info, interval_sec, num_msgs):
    msgs = get_msgs(datetime.utcnow(), interval_sec, num_msgs)
    p = Process(target=send_mqtt_msgs, args=(service_info, msgs, interval_sec))
    p.start()
    p.join()


def get_mqtt_info():
    service_info = {}
    service_info['name'] = 'MultiSensor'
    service_info['debug'] = True
    service_info['host'] = '172.17.0.2'
    service_info['protocol'] = get_net_protocol()
    service_info['deserializer'] = {}
    service_info['deserializer']['type'] = 'json'
    service_info['schema'] = get_schema()

    return service_info


def get_net_protocol():
    protocol = {}

    protocol['type'] = 'mqtt'
    protocol['port'] = 1883
    protocol['publish'] = {}
    protocol['publish']['default_topic'] = 'test'
    protocol['subscribe'] = {}
    protocol['subscribe']['topics'] = ['test']
    protocol['subscribe']['type'] = 'unblocking'

    return protocol


def get_schema():
    schema = {}
    schema['fields'] = {}

    schema['fields']['timestamp'] = {
        'type': 'timestamp',
        'default_value': datetime.utcnow(),
        'type_check': True,
        'parser': '%Y-%m-%d %H:%M:%S'
    }

    schema['fields']['pir'] = {
        'type': 'float',
        'default_value': 0.0,
        'type_check': True
    }

    schema['fields']['temp'] = {
        'type': 'float',
        'default_value': 0.0,
        'type_check': True
    }

    return schema


def get_stream_info(name, handle):
    stream_info = {}
    stream_info['name'] = name
    stream_info['debug'] = True
    stream_info['handle'] = handle
    stream_info['store'] = {}
    stream_info['store']['type'] = 'in_memory'
    stream_info['enricher'] = {}
    stream_info['enricher']['type'] = 'simple'

    return stream_info


def test_simple_storage():
    print()
    service_info = get_mqtt_info()
    pir_stream_info = get_stream_info('PirSensor', 'pir')
    temp_stream_info = get_stream_info('TempSensor', 'temp')
    stream_infos = [pir_stream_info, temp_stream_info]

    router = Router()
    route_names = router.create_routes(service_info, stream_infos)

    for r in route_names:
        print('Route created: {}'.format(r))

    router.start_all()

    spawn_test_client(service_info, 3, 3)
