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


def load_database_from_file(
    path: str, include_only: list = [], allow_self_reference: bool = True
):
    database = model.new_database()
    with open(path, "r") as f:
        line = f.readline()
        line = f.readline()
        while line != "" and line is not None:
            (
                _,
                table,
                column,
                referenced_table,
                referenced_column,
            ) = line.split(",")
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
            model.insert_table(database, table)
            model.insert_table(database, referenced_table)
            rel_name = _relationship_name_from(column, referenced_column)
            rev_rel_name = _relationship_name_from(referenced_column, column)
            model.insert_relationship(
                database,
                table,
                referenced_table,
                name=rel_name,
                reversed_name=rev_rel_name,
            )
            line = f.readline()

    return database


def _relationship_name_from(column_1, column_2):
    return ";".join([column_1, column_2])


"""
    include_only=[
        "GCCOM_PAYMENT_FORM",
        "GCCOM_CONTRACTED_SERVICE",
        "GCCOM_FARE",
        "GCCOM_BILLING_SERVICE",
        "GCCD_RELATIONSHIP",
    ],
"""

d = load_database_from_file(
    cf.DATA_PATH / "Tables.csv",
    allow_self_reference=False,
)
r = model.show_relationships(d, "GCCD_RELATIONSHIP")

for l in r:
    print(l)

tables, relationships = model.path_to(
    d, "GCCD_RELATIONSHIP", "GCCOM_FARE", algorithm="BFS"
)

while not model.isempty(tables):
    print(model.pop(tables))
    print(model.pop(relationships))

"""
D = model.new_database()
D = model.insert_table(D, "GCCOM_PAYMENT_FORM")
D = model.insert_table(D, "GCCOM_CONTRACTED_SERVICE")
D = model.insert_table(D, "GCCOM_SECTOR_SUPPLY")
D = model.insert_relationship(
    D,
    "GCCOM_PAYMENT_FORM",
    "GCCOM_CONTRACTED_SERVICE",
    name="ID_PAYMENT_FORM;ID_PAYMENT_FORM",
    reversed_name="ID_PAYMENT_FORM;ID_PAYMENT_FORM"
)
D = model.insert_relationship(
    D,
    "GCCOM_SECTOR_SUPPLY",
    "GCCOM_CONTRACTED_SERVICE",
    name="ID_SECTOR_SUPPLY;ID_SECTOR_SUPPLY",
    reversed_name="ID_SECTOR_SUPPLY;ID_SECTOR_SUPPLY"
)

# print(model.relationships(D))
# for relationship in model.relationships(D):
#     print(relationship)

tables, relationships = model.path_to(D, "GCCOM_PAYMENT_FORM", "GCCOM_SECTOR_SUPPLY")

while not model.isempty(tables):
    print(model.pop(tables))
    print(model.pop(relationships))

# for table, relationship in zip(model.iterator(tables), model.iterator(relationships)):
#    print(table, relationship)
"""
