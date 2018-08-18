from pprint import pprint

from pyedj.ingestion.protocols.mqtt import Mqtt


print('Starting edge...')
service_info = {'host': 'localhost',
                'port': 1883,
                'publish': {
                    'default_topic': 'test/'
                },
                'subscribe': {
                    'topics': ['ingestion/test/']
                }}

print('Service info:')
pprint(service_info, indent=4)

mqtt = Mqtt()

print('Connecting to server...')
mqtt.connect(service_info)

print('Starting event loop...')
mqtt.receive_msgs()