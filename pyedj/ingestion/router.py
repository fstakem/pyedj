from collections import namedtuple


Route = namedtuple('Route', 'name ingestor stream')


class RouteError(Exception):
    pass


class Router(object):

    def __init__(self, routes):
        self.routes = routes

    def add_route(self, route):
        if route.name in self.routes.keys():
            raise RouteError('Route key is already defined')

        self.routes[route.name] = route

    def remove_route(self, route_name):
        if route_name in self.routes.keys():
            raise RouteError('Route key is not defined')

        _ = self.routes.pop(route_name)

    def get_route(self, route_name):
        if route_name in self.routes.keys():
            raise RouteError('Route key is not defined')

        route = self.routes.pop(route_name)

        return route

    def num_routes(self):
        return len(self.routes)