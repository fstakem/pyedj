from collections import namedtuple
from importlib import import_module

from pyedj.compute.stream import Stream


class RouteError(Exception):
    pass


Route = namedtuple('Route', 'name, on, endpoint, stream')


class Router(object):
    supported_protocols = ['mqtt']

    def __init__(self, routes=None):
        self.routes = {}

    def create_routes(self, service_info, stream_infos):
        names = [service_info['name'] + '_' + s['handle'] for s in stream_infos]

        for n in names:
            if n in self.routes:
                raise RouteError(f'Route name({n}) is already being used')

        endpoint = self.get_endpoint(service_info['name'])

        if not endpoint:
            endpoint = Router.create_endpoint(service_info)

        for n, s in zip(names, stream_infos):
            stream = Stream(s)
            endpoint.add_stream(stream)
            route = Route(n, False, endpoint, stream)
            self.routes[n] = route

        if not endpoint.is_connected():
            endpoint.connect()

        return names

    def get_endpoint(self, name):
        for n, v in self.routes.items():
            if v.endpoint.name == name:
                return v.endpoint

        return None

    @classmethod
    def create_endpoint(cls, service_info):
        protocol = service_info['protocol']['type']

        if protocol not in cls.supported_protocols:
            raise RouteError(f'Cannot create route with protocol({protocol})')

        mod = import_module('pyedj.ingestion.protocols.' + protocol)
        klass_name = ''.join([p.capitalize() for p in protocol.split('_')])
        klass = getattr(mod, klass_name)
        endpoint = klass(service_info)

        return endpoint

    def remove_route(self, name):
        if name not in self.routes.keys():
            raise RouteError('Route name is not defined')

        route = self.routes.pop(name)
        route.endpoint.disconnect()

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

        route = self.routes[name]
        route.endpoint.start()

    def stop(self, name):
        if name not in self.routes.keys():
            raise RouteError('Route name is not defined')

        self.routes[name].endpoint.stop()

    def start_all(self):
        for name, route in self.routes.items():
            route.endpoint.start()

    def stop_all(self):
        for name, route in self.routes.items():
            route.endpoint.stop()

    def __iter__(self):
        for k, v in self.routes.items():
            yield k, v