def file_to_datastruct(
    path: str,
    key_column: str | int,
    delimiter: str,
    firstrow_as_column_names: bool = True,
    ignore_columns: list[str | int] = [],
    datastruct: str = "dict"
) -> dict:
    file_as_dict = {}
    with open(path, "r") as f:
        columns = []
        if firstrow_as_column_names:
            columns = [column.strip() for column in f.readline().split(delimiter)]

        line = f.readline()
        if not columns:
            columns = [i for i in range(len(f.readline().split(delimiter)))]
        
        if not set(ignore_columns).issubset(columns):
            raise Exception(f"`ignore_columns={ignore_columns}` value has unrecognized columns")

        if key_column not in columns:
            raise Exception(f"`key_column={key_column}` not in list of `columns={columns}`")

        while line != "" and line is not None:
            values = [value.strip() for value in line.split(delimiter)]
            column_values = {column: value for column, value in zip(columns, values) if column not in ignore_columns}
            key_column_value = column_values.pop(key_column)
            file_as_dict[key_column_value] = column_values
            line = f.readline()
    
    if datastruct == "list": 
        file_as_dict = [[{key: value}] for key, value in file_as_dict.items()]
    
    if datastruct == "tuple":
        file_as_dict = [(key, value) for key, value in file_as_dict.items()]

    return file_as_dict
