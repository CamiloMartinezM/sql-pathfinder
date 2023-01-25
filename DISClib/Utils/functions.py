def file_to_dict(
    path: str,
    delimiter: str = ";",
    key_column: str,
    ignore_first_row: bool = True,
    ignore_columns: list = [],
) -> dict:
    with open(path, "r") as f:
        if ignore_first_row:
            line = f.readline()
        line = f.readline()
        while line != "" and line is not None:
            columns = line.split(delimiter)
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
                table,
                referenced_table,
                src_fk=column,
                target_fk=referenced_column,
            )
            line = f.readline()

    return database
