import json
import time
import argparse
from random import random
from pprint import pprint
from datetime import datetime

from mqtt_client import MqttClient


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--server', help='Mqtt server', required=True)
    parser.add_argument('-p', '--port', help='Mqtt port', required=True)
    parser.add_argument('-t', '--topic', help='Mqtt topic', required=True)
    args = parser.parse_args()

    return args


def main():
    args = get_arguments()
    server = args.server
    port = int(args.port)
    topic = args.topic

    print('Parameters')
    print(f'Server: {server}')
    print(f'Port: {port}')
    print(f'Topic: {topic}')
    print()

    client = MqttClient()
    client.connect(server, port, 60)

    while True:
        timestamp = datetime.utcnow()
        num = random() * 100
        msg = {
                'timestamp': timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                'value': num}

        print(f'Publishing: {msg}')
        client.publish(topic, json.dumps(msg))
        time.sleep(3)


if __name__ == '__main__':
    main()