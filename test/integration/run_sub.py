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
    client.run(server, port, [topic])


if __name__ == '__main__':
    main()