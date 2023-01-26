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

import model
from DISClib.Utils.functions import file_to_dict


def load_database_from_file(
    path: str, include_only: list = [], allow_self_reference: bool = True
):
    database = model.Database()
    with open(path, "r") as f:
        line = f.readline()
        line = f.readline()
        while line != "" and line is not None:
            (_, table, column, referenced_table, referenced_column,) = line.split(",")
            table = table.strip()
            column = column.strip()
            referenced_table = referenced_table.strip()
            referenced_column = referenced_column.strip()

            # The current column is primary key of the current table
            if referenced_column == "" and referenced_table == "":
                line = f.readline()
                continue

            # If only a couple of tables are to be added to the database
            if include_only and (
                table not in include_only or referenced_table not in include_only
            ):
                line = f.readline()
                continue

            # Remove the relationships of tables pointing to themselves
            if not allow_self_reference and referenced_table == table:
                line = f.readline()
                continue

            # If not, then it must be a foreign key
            database.insert_table(table)
            database.insert_table(referenced_table)
            database.insert_relationship(
                table, referenced_table, src_fk=column, target_fk=referenced_column,
            )
            line = f.readline()

    return database

d = load_database_from_file(cf.DATA_PATH / "Tables.csv", allow_self_reference=False,)
attrs = file_to_dict(cf.DATA_PATH / "Table_attrs.csv", key_column="table", delimiter=",", ignore_columns=["schema_name"])

import networkx as nx

nodes = nx.shortest_path(d.database,'GCCD_RELATIONSHIP').keys()

print(nodes)
print("")

print(d.number_of_tables())
exclude_tables = d.get_tables(filter_=[("in", "GCGT")])

for path in d.paths(
    "GCCOM_FARE_CM_PARAMETER",
    "GCCOM_FARE",
    cutoff=5,
    exclude_tables=exclude_tables,
    include_tables=["GCCOM_FARED_CONCEPT", "GCCOM_FARE_PERIOD"]
):
    path = list(path)
    # print(d.build_select_query(path))
    # print("")

# print(d.database.edges(data=True))

print("")
print(d.build_select_query(path))

print(attrs)