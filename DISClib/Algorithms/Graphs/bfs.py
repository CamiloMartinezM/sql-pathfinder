"""
 * Copyright 2020, Departamento de sistemas y Computación,
 * Universidad de Los Andes
 *
 * Desarrollado para el curso ISIS1225 - Estructuras de Datos y Algoritmos
 *
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contribución de:
 *
 * Dario Correal
 *
 """

import config
from DISClib.ADT import graph as g
from DISClib.ADT import list as lt
from DISClib.ADT import map as map
from DISClib.ADT import queue, stack
from DISClib.Utils import error as error

assert config


def BreadthFisrtSearch(graph, source):
    """
    Genera un recorrido BFS sobre el grafo graph
    Args:
        graph:  El grafo a recorrer
        source: Vertice de inicio del recorrido.
    Returns:
        Una estructura para determinar los vertices
        conectados a source
    Raises:
        Exception
    """
    try:
        search = {"source": source, "visited": None}
        search["visited"] = map.newMap(
            numelements=g.numVertices(graph),
            maptype="PROBING",
            comparefunction=graph["comparefunction"],
        )
        map.put(
            search["visited"],
            source,
            {"marked": True, "edgeTo": None, "distTo": 0, "edge": None},
        )
        bfsVertex(search, graph, source)
        return search
    except Exception as exp:
        error.reraise(exp, "bfs:BFS")


def bfsVertex(search, graph, source):
    """
    Funcion auxiliar para calcular un recorrido BFS
    Args:
        search: Estructura para almacenar el recorrido
        vertex: Vertice de inicio del recorrido.
    Returns:
        Una estructura para determinar los vertices
        conectados a source
    Raises:
        Exception
    """
    try:
        adjsqueue = queue.newQueue()
        queue.enqueue(adjsqueue, source)
        visitededges = set()
        while not (queue.isEmpty(adjsqueue)):
            vertex = queue.dequeue(adjsqueue)
            visited_v = map.get(search["visited"], vertex)["value"]
            vertex, _ = unhashVertices(vertex)
            _, visited_v = unhashVertices(visited_v)
            adjslst = g.adjacents(graph, vertex)
            adjsedges = g.adjacentEdges(graph, vertex)
            for w, e in zip(lt.iterator(adjslst), lt.iterator(adjsedges)):
                assert e["vertexB"] == w
                visited_w = map.get(search["visited"], hashVertices(w, vertex))
                if visited_w is None or not edgeWasVisited(visitededges, e):
                    dist_to_w = visited_v["distTo"] + 1
                    visited_w = {
                        "marked": True,
                        "edgeTo": vertex,
                        "distTo": dist_to_w,
                        "edge": e,
                    }
                    visitededges = addVisitedEdge(visitededges, e)
                    map.put(search["visited"], hashVertices(w, vertex), visited_w)
                    # map.put(search["visited"], hashVertices(vertex, w, e), visited_w)
                    queue.enqueue(adjsqueue, hashVertices(w, vertex))
        return search
    except Exception as exp:
        error.reraise(exp, "bfs:bfsVertex")


def addVisitedEdge(visited, edge):
    visited.add(hashEdge(edge))
    return visited


def hashEdge(edge):
    return edge["vertexA"] + edge["vertexB"] + edge["name"]


def edgeWasVisited(visited, edge):
    return hashEdge(edge) in visited

def hashVertices(vertexa, vertexb):
    return vertexa + ";" + vertexb

def unhashVertices(_hash):
    if ";" in _hash:
        return _hash.split(";")
    else:
        return (_hash, _hash)

def hasPathTo(search, vertex):
    """
    Indica si existe un camino entre el vertice source
    y el vertice vertex
    Args:
        search: Estructura de recorrido BFS
        vertex: Vertice destino
    Returns:
        True si existe un camino entre source y vertex
    Raises:
        Exception
    """
    try:
        element = map.get(search["visited"], vertex)
        if element and element["value"]["marked"] is True:
            return True
        return False
    except Exception as exp:
        error.reraise(exp, "bfs:hasPathto")


def pathTo(search, vertex):
    """
    Retorna el camino entre el vertices source y el
    vertice vertex
    Args:
        search: La estructura con el recorrido
        vertex: Vertice de destingo
    Returns:
        Una pila con el camino entre el vertices source y el
        vertice vertex
    Raises:
        Exception
    """
    try:
        #if hasPathTo(search, vertex) is False:
        #    return None
        path = stack.newStack()
        edges = stack.newStack()
        # first_edge = map.get(search["visited"], vertex)["value"]["edge"]
        possible_paths = partial_get(search, vertex)
        # stack.push(edges, first_edge)
        while vertex != search["source"]:
            stack.push(path, vertex)
            # vertex = map.get(search["visited"], vertex)["value"]["edgeTo"]
            possible_vertices = partial_get(search, vertex)
            for possible_vertex in possible_vertices:
                _, vertex = unhashVertices(possible_vertex['key'])

            # edge = map.get(search["visited"], vertex)["value"]["edge"]
            # stack.push(edges, edge)
        stack.push(path, search["source"])
        return path, edges
    except Exception as exp:
        error.reraise(exp, "bfs:pathto")

def partial_get(search, vertex):
    # for visited in search['visited']['']
    found = []
    for visited in search['visited']['table']['elements']:
        if visited['key'] and visited['key'].startswith(vertex + ";"):
            found.append(visited)
    return found