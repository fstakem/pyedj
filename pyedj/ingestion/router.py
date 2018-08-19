from collections import namedtuple

import pyedj.ingestion.protocols


class RouteError(Exception):
    pass

Route = namedtuple('Route', 'name, on, subscriber, stream')


class Router(object):
    supported_protocols = ['mqtt']

    def __init__(self, routes=None):
        self.routes = {}

    def create_route(self, name, service_info, stream):
        if name in self.routes:
            raise RouteError(f'Route name({name}) is already being used')

        subscriber = Router.create_subscriber(service_info)
        route = Route(name, False, subscriber, stream)
        route.subscriber.connect()
        self.routes[name] = route

    @classmethod
    def create_subscriber(cls, service_info):
        protocol = service_info['protocol']

        if protocol not in cls.supported_protocols:
            raise RouteError(f'Cannot create route with protocol({protocol})')

        mod = getattr(pyedj.ingestion.protocols, protocol)
        klass_name = ''.join([p.capitalize() for p in protocol.split('_')])
        klass = getattr(mod, klass_name)
        subscriber = klass(service_info)

        return subscriber

    def remove_route(self, name):
        if name not in self.routes.keys():
            raise RouteError('Route name is not defined')

        route = self.routes.pop(name)
        route.subscriber.disconnect()

        return route

    def get_route(self, name):
        if name not in self.routes.keys():
            raise RouteError('Route name is not defined')

        return self.routes[name]

    def num_routes(self):
        return len(self.routes)

    def start(self, name):
        if name not in self.routes.keys():
            raise RouteError('Route name is not defined')

        self.routes[name].subscriber.start()

    def stop(self, name):
        if name not in self.routes.keys():
            raise RouteError('Route name is not defined')

        self.routes[name].subscriber.stop()

    def start_all(self):
        for name, route in self.routes:
            route.start()

    def stop_all(self):
        for name, route in self.routes:
            route.stop()

    def __iter__(self):
        for k, v in self.routes:
            yield k, v