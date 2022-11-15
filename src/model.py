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

import networkx as nx


class Database:
    def __init__(self):
        self.database = nx.Graph()

    def insert_relationship(
        self, source: str, target: str, src_fk: str = "", target_fk: str = ""
    ) -> None:
        self.database.add_edge(source, target, src_fk=src_fk, target_fk=target_fk)

    def insert_table(self, table: str, attrs=None) -> None:
        if attrs:
            self.database.add_node(table, **attrs)
        else:
            self.database.add_node(table)

    def paths(
        self,
        source: str,
        target: str,
        cutoff: int = None,
        include_tables: list = [],
        exclude_tables: list = [],
    ) -> list:
        paths = []
        for path in nx.all_simple_paths(
            self.database, source=source, target=target, cutoff=cutoff
        ):
            if Database.path_includes(
                set(path), include_tables
            ) and not Database.path_includes(path, exclude_tables, any_=True):
                paths.append(path)
        return map(nx.utils.pairwise, paths)

    @staticmethod
    def paths_passes_filters(paths, filter_: dict, boolean_operation: str = "AND"):
        for filter_criteria in filter_.items():
            func, wanted_value = filter_criteria
            passes_current = Database._path_passes_filter(paths, func, wanted_value)
            if not passes_current and boolean_operation == "AND":
                return False
            elif passes_current and boolean_operation == "OR":
                return True
        return True

    @staticmethod
    def _path_passes_filter(
        path: list, func, wanted_value, all_for_true: bool = False
    ) -> bool:
        for node in path:
            if (
                (func == "=" and node != wanted_value)
                or (func == "!=" and node == wanted_value)
                or (func == "in" and wanted_value not in node)
                or (
                    func not in ("=", "!=", "in")
                    and not getattr(node, func)(wanted_value)
                )
            ) and all_for_true:
                return False
        return True

    def get_tables(self, filter_: dict) -> list:
        """
        Parameters
        ----------
        filter_ : list
            Filter criteria list where each item is a tuple where the first 
            item is a function to apply to the node and the second item is
            the expected value.

        Returns
        -------
        list
            Nodes that match the given criteria.
        """
        filtered = self.database.nodes()
        for filter_criteria in filter_:
            func, wanted_value = filter_criteria
            filtered = self._filter_tables(filtered, func, wanted_value)
        return filtered

    @staticmethod
    def _filter_tables(tables: list, func, wanted_value) -> list:
        """
        Parameters
        ----------
        tasks: list
            List of tables to filter.
        func :
            A function to apply to the attribute name of the current table to
            see if it matches the stop criteria.
        stop_value :
            Stop criteria; the value determines when the algorithm will stop
            and decide that the current table must be in the resulting list of
            filtered tables.
            
        Returns
        -------
        list
            Filtered tables.
        """
        filtered = []
        for table in tables:
            if not (
                (func == "=" and table != wanted_value)
                or (func == "!=" and table == wanted_value)
                or (func == "in" and wanted_value not in table)
                or (
                    func not in ("=", "!=", "in")
                    and not getattr(table, func)(wanted_value)
                )
            ):
                filtered.append(table)
        return filtered

    @staticmethod
    def path_includes(path: list, tables: list, any_: bool = False) -> bool:
        for table in tables:
            if table not in path and not any_:
                return False
            elif table in path and any_:
                return True
        return True if not any_ else False

    def is_empty(self) -> bool:
        return self.database.number_of_nodes() == 0

    def number_of_tables(self) -> int:
        return self.database.number_of_nodes()

    def number_of_relationships(self) -> int:
        return self.database.number_of_edges()
