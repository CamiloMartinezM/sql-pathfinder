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
        self.edges_attrs = None

    def insert_relationship(
        self, source: str, target: str, src_fk: str = "", target_fk: str = ""
    ) -> None:
        self.database.add_edge(source, target, src_fk=src_fk, target_fk=target_fk)

    def insert_table(self, table: str, attrs=None) -> None:
        if attrs:
            self.database.add_node(table, **attrs)
        else:
            self.database.add_node(table)

    def load_table_attrs(self, attrs_list: list[tuple[str, dict]]) -> None:
        for curr_attr in attrs_list:
            table, attrs = curr_attr
            if table in self.database:
                self.database.nodes[table].update(attrs)

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
            if Database._check_filter(node, func, wanted_value) and all_for_true:
                return False
        return True

    def get_tables(self, filter_: dict, tables: list = []) -> list:
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
        filtered = self.database.nodes() if not tables else tables 
        for filter_criteria in filter_:
            func, wanted_value = filter_criteria
            filtered = self._filter_tables(filtered, func, wanted_value)
        return filtered

    def filter_tables_per_attr(self, tables: list, filter_: dict[str, tuple]) -> list:
        filtered = []
        for table in tables:
            if table in self.database:
                if table == "GCCOM_INSURANCE_EXCH_RATE":
                    print("hola")

                for attr, filter_criteria in filter_.items():
                    func, wanted_value = filter_criteria
                    if attr in self.database.nodes[table].keys():
                        if not self._check_filter(self.database.nodes[table][attr], func, wanted_value):
                            break
                else:
                    filtered.append(table)

        return filtered

    @staticmethod
    def _check_filter(to_check, func, wanted_value) -> list:
        """
        Parameters
        ----------
        to_check: any object
            Value to check if it passes the filter criteria
        func :
            A function to apply to the given value.
        wanted_value :
            Stop criteria; the value determines if the given value matches
            the filter criteria.

        Returns
        -------
        bool
            True if it matches the filter criteria.
        """
        if not (
            (func == "=" and to_check != wanted_value)
            or (func == "!=" and to_check == wanted_value)
            or (func == "in" and type(wanted_value) is not list and wanted_value not in to_check)
            or (func == "in" and type(wanted_value) is list and to_check not in wanted_value)
            or (func == "not in" and wanted_value in to_check)
            or (
                func not in ("=", "!=", "in", "not in")
                and not getattr(to_check, func)(wanted_value)
            )
        ):
            return True
        return False

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
            if Database._check_filter(table, func, wanted_value):
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

    def build_select_query(self, path: list) -> str:
        if not self.edges_attrs or not self.is_empty():
            edge_attrs = self.database.edges(data=True)
            self.edges_attrs = {
                (node1, node2): attrs for node1, node2, attrs in edge_attrs
            }
            self.edges_attrs.update(
                {(node2, node1): attrs for node1, node2, attrs in edge_attrs}
            )
        else:
            raise Exception("Unable to build the query. The database might be empty")

        query = ""
        aliases = []
        for i, edge in enumerate(path):
            table1, table2 = edge
            src_fk, target_fk = (
                self.edges_attrs[edge]["src_fk"],
                self.edges_attrs[edge]["target_fk"],
            )
            start_with_select = True if i == 0 else False
            query_part, aliases = Database._join_tables(
                table1,
                table2,
                src_fk,
                target_fk,
                exclude_aliases=aliases,
                start_with_select=start_with_select,
            )
            query += query_part

        return query

    @staticmethod
    def _join_tables(
        table1: str,
        table2: str,
        table1_key: str,
        table2_key,
        join_type: str = "INNER",
        exclude_aliases: list = [],
        start_with_select: bool = False,
    ) -> str:
        t1_alias, exclude_aliases = Database._table_alias(table1, exclude_aliases)
        t2_alias, exclude_aliases = Database._table_alias(table2, exclude_aliases)
        query = ""
        if start_with_select:
            query += "SELECT *\nFROM {} AS {}\n".format(table1, t1_alias)

        query += "{} JOIN {} AS {} ON {}.{} = {}.{}\n".format(
            join_type, table2, t2_alias, t1_alias, table1_key, t2_alias, table2_key
        )
        return query, exclude_aliases

    @staticmethod
    def _table_alias(table: str, exclude_aliases: list = []) -> str:
        possible_alias = (
            "".join([p[0] for p in table.split("_")]) if "_" in table else table[0]
        )
        i = 1
        while possible_alias in exclude_aliases:
            if i > 1:
                possible_alias = possible_alias[:-1] + str(i)
            else:
                possible_alias += str(i)
            i += 1
        exclude_aliases.append(possible_alias)
        return possible_alias, exclude_aliases

    def is_empty(self) -> bool:
        return self.database.number_of_nodes() == 0

    def number_of_tables(self) -> int:
        return self.database.number_of_nodes()

    def number_of_relationships(self) -> int:
        return self.database.number_of_edges()

    def get_table_attrs(self, table: str) -> dict:
        return (
            self.database.nodes[table]
            if table in self.database
            else None
        )
