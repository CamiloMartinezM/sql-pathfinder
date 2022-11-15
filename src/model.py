"""
 * Copyright 2022, Camilo Martínez M.
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
 """
import config as cf

assert cf

from DISClib.ADT import graph as gr
from DISClib.ADT import stack
from DISClib.Algorithms.Graphs import bfs, dfs
from DISClib.DataStructures import singlelinkedlist


def new_database():
    return gr.newGraph()


def insert_relationship(database, table_a, table_b, name="", reversed_name=""):
    return gr.addEdge(
        database, table_a, table_b, name=name, reversed_name=reversed_name
    )


def insert_table(database, table):
    if not table_exists(database, table):
        return gr.insertVertex(graph=database, vertex=table)
    return gr


def table_exists(database, table):
    return gr.containsVertex(graph=database, vertex=table)


def show_relationships(database, table):
    return singlelinkedlist.iterator(gr.adjacentEdges(graph=database, vertex=table))


def path_to(database, table_source, table_destination, algorithm: str = "BFS"):
    if algorithm.upper() == "BFS":
        paths = bfs.BreadthFisrtSearch(graph=database, source=table_source)
        return bfs.pathTo(paths, table_destination)
    elif algorithm.upper() == "DFS":
        paths = dfs.DepthFirstSearch(graph=database, source=table_source)
        return dfs.pathTo(paths, table_destination)
    else:
        raise Exception(
            "Algorithm {} does not exist. It must be BFS or DFS".format(algorithm)
        )


def pop(s):
    return stack.pop(s)


def isempty(s):
    return stack.isEmpty(s)


def number_of_tables(database):
    return gr.numVertices(database)


def number_of_relationships(database):
    return gr.numEdges(database)


def relationships(database):
    edges = gr.edges(database)
    return singlelinkedlist.iterator(edges)
    # return edges
