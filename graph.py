from pystream import Stream
from typing import Type
from pprint import pprint


class ProtoVertex:
    def __init__(self):
        self._id: int = None
        self.incoming_edges: list[Type[ProtoEdge]] = []
        self.outgoing_edges: list[Type[ProtoEdge]] = []

    def __eq__(self, other):
        return self._id == other._id

    def __hash__(self):
        return hash(self._id)


class ProtoEdge:
    def __init__(
        self, source_vertex: Type[ProtoVertex], destination_vertex: Type[ProtoVertex]
    ):
        self._id: int = None
        self.source_vertex = source_vertex
        self.destination_vertex = destination_vertex

    def __eq__(self, other):
        return self._id == other._id

    def __hash__(self):
        return hash(self._id)


class Actor(ProtoVertex):
    def __init__(self, firstname: str, lastname: str):
        super().__init__()
        self.firstname = firstname
        self.lastname = lastname

    def __repr__(self):
        return f"<{self.__class__.__qualname__} id={self._id!r}, firstname={self.firstname!r}, lastname={self.lastname!r}>"


class Movie(ProtoVertex):
    def __init__(self, name: str, minutes: int):
        super().__init__()
        self.name = name
        self.minutes = minutes

    def __repr__(self):
        return f"<{self.__class__.__qualname__} id={self._id!r}, name={self.name!r}, minutes={self.minutes!r}>"


class Acts(ProtoEdge):
    def __init__(
        self, source_vertex: Type[ProtoVertex], destination_vertex: Type[ProtoVertex]
    ):
        super().__init__(source_vertex, destination_vertex)

    def __repr__(self):
        return f"<{self.__class__.__qualname__} id={self._id!r}, source_vertex={self.source_vertex!r}, destination_vertex={self.destination_vertex!r}>"


class Directs(ProtoEdge):
    def __init__(
        self, source_vertex: Type[ProtoVertex], destination_vertex: Type[ProtoVertex]
    ):
        super().__init__(source_vertex, destination_vertex)

    def __repr__(self):
        return f"<{self.__class__.__qualname__} id={self._id!r}, source_vertex={self.source_vertex!r}, destination_vertex={self.destination_vertex!r}>"


class Produces(ProtoEdge):
    def __init__(
        self, source_vertex: Type[ProtoVertex], destination_vertex: Type[ProtoVertex]
    ):
        super().__init__(source_vertex, destination_vertex)

    def __repr__(self):
        return f"<{self.__class__.__qualname__} id={self._id!r}, source_vertex={self.source_vertex!r}, destination_vertex={self.destination_vertex!r}>"


class Graph:
    def __init__(self):
        self.vertices: list[Type[ProtoVertex]] = []
        self.edges: list[Type[ProtoEdge]] = []

        self.auto_id = 0

    def new_id(self):
        self.auto_id += 1
        return self.auto_id

    def __repr__(self):
        return f"<{self.__class__.__qualname__} vertices={self.vertices} edges={self.edges}>"

    def add_vertices(self, vertices: list[Type[ProtoVertex]]):
        return [self.add_vertex(vertex) for vertex in vertices]

    def add_vertex(self, vertex: Type[ProtoVertex]):
        if vertex._id is None:
            vertex._id = self.new_id()

        elif vertex._id in map(lambda v: v._id, self.vertices):
            raise ValueError(f"vertex with id={vertex._id} already exists")

        self.vertices.append(vertex)

        return vertex

    def add_edges(self, edges: list[Type[ProtoEdge]]):
        return [self.add_edge(edge) for edge in edges]

    def add_edge(self, edge: Type[ProtoEdge]):
        if edge._id is None:
            edge._id = self.new_id()

        elif edge._id in map(lambda e: e._id, self.edges):
            raise ValueError(f"edge with id={edge._id} already exists")

        if edge.source_vertex not in self.vertices:
            raise ValueError(
                f"vertex with id={edge.source_vertex} not found in graph")
        if edge.destination_vertex not in self.vertices:
            raise ValueError(
                f"vertex with id={edge.destination_vertex} not found in graph"
            )

        edge.source_vertex.outgoing_edges.append(edge)
        edge.destination_vertex.incoming_edges.append(edge)
        self.edges.append(edge)

        return edge


def main():
    graph = Graph()

    a1 = graph.add_vertex(Actor("f1", "l1"))
    a2 = graph.add_vertex(Actor("f2", "l2"))
    m1 = graph.add_vertex(Movie("m1", 210))
    m2 = graph.add_vertex(Movie("m2", 150))

    graph.add_edge(Directs(a1, m1))
    graph.add_edge(Produces(a1, m1))
    graph.add_edge(Acts(a1, m1))
    graph.add_edge(Acts(a2, m1))
    graph.add_edge(Acts(a2, m2))

    print("\n\n\n")
    pprint(graph)
    print("\n\n\n")

    res = (
        Stream(graph.vertices)
        .filter(lambda v: type(v) == Actor)
        .filter(lambda a: a.firstname == "f2")
        .flatmap(lambda v: v.outgoing_edges)
        .map(lambda e: e.destination_vertex)
        .distinct()
        .collect(list)
    )

    pprint(res)


if __name__ == "__main__":
    main()

    print("done")
