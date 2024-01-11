import argparse
import atexit
import glob
import multiprocessing as mpg
import os
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow as pa
from tableauhyperapi import HyperProcess, Telemetry, Connection, CreateMode, NOT_NULLABLE, NULLABLE, SqlType, \
TableDefinition, Inserter, escape_name, escape_string_literal, HyperException, TableName, SchemaName

def get_parquet_schema(path):
    dt_schema = {}
    par = pq.read_metadata(path)
    no_of_column = par.num_columns
    schema = par.schema
    for i in range(no_of_column):
        col_schema = schema.column(i)
        dt_schema[col_schema.name] =  col_schema.physical_type if col_schema.logical_type.type == 'NONE' else  col_schema.logical_type.type
    return dt_schema

def get_table_definition_from_parquet(path, output_schema='Extract', output_table='Extract'):
    dt_type_map = {'INT32':SqlType.int(), 
                    'INT64':SqlType.big_int(),
                    'INT96':SqlType.timestamp_tz(),
                    'DOUBLE':SqlType.double(), 
                    'FLOAT32':SqlType.double(), 
                    'FLOAT64':SqlType.double(), 
                    'FLOAT':SqlType.double(),
                    'STRING':SqlType.text(), 
                    'DATE':SqlType.date(),}
    
    dt = get_parquet_schema(path)
    lt_tb_def = [TableDefinition.Column(k, dt_type_map[v], NULLABLE) for k, v in dt.items()]
    table_defintion = TableDefinition(table_name=TableName(output_schema,output_table), columns=lt_tb_def)
    return table_defintion

def clean_parquet(input_path, output_path=None):
    """to cleanse each partitioned parquet file to be tableau hyper-compatible format"""
    input_folder_name = os.path.dirname(input_path)
    input_file_name = os.path.basename(input_path)
    if output_path is None:
        temp_folder = os.path.join(input_folder_name, 'cleansed_parquet' )
        try:
            os.makedirs(temp_folder)
        except FileExistsError:
            pass
        output_path = os.path.join(temp_folder, input_file_name)

    table = pq.read_table(input_path)
    # get_parquet_schema(p_path)
    j = 0
    for i, (col_name, type_) in enumerate(zip(table.schema.names, table.schema.types)):
        if pa.types.is_decimal(type_):
            j += 1
            #print(j)
            try:
                table = table.set_column(i, col_name, pc.cast(table.column(col_name), pa.float64()))
            except KeyError:
                print('keyerror')
    pq.write_table(table, output_path, use_deprecated_int96_timestamps=True)
    return output_path, temp_folder

def single_parquet_to_hyper(parquet_path, output_path=None, schema='Extract', table='Extract'):
    print('converting single parquet to single hyper')
    parquet_path, temp_folder = clean_parquet(parquet_path)
    if pq.read_metadata(parquet_path).num_rows == 0:
        return None, temp_folder
    table_definition = get_table_definition_from_parquet(parquet_path, schema, table)

    if not output_path:
        output_path = parquet_path.replace('.parquet','.hyper')
    path_to_database = output_path

    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=False) as hyper:
        connection_parameters = {"lc_time": "en_US"}

        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        #create_mode=CreateMode.CREATE_IF_NOT_EXISTS,
                        create_mode=CreateMode.CREATE_AND_REPLACE,
                        parameters=connection_parameters) as connection:
            connection.catalog.create_schema(schema)
            connection.catalog.create_table_if_not_exists(table_definition=table_definition)
            count_in_table_definition = connection.execute_command(command=f"COPY {table_definition.table_name} from {escape_string_literal(parquet_path)} with (format parquet)")
            #print(f"+{count_in_table_definition}->{table_definition.table_name}\t", end='')
    return output_path, temp_folder

def multiprocess_parquet(lt_arg):
    p = mpg.Pool()
    with mpg.Pool() as pool:
        result = pool.starmap(single_parquet_to_hyper, lt_arg)
    atexit.register(p.close)
    return [i for i in result if i[0] ] #if i is not None

def union_hyper(input_files, output_path, input_schema='Extract', input_table='Extract', output_schema='Extract', output_table='Extract',rm_input=False, sql=None):
    print('unioning hypers')
    if os.path.exists(output_path):
        os.remove(output_path)
    output_table_name = TableName(output_schema, output_table)
    input_table_name = TableName(input_schema, input_table)
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, 'unionfiles_efficient') as hyper:
        with Connection(hyper.endpoint) as connection:
            for i, file in enumerate(input_files):
                connection.catalog.attach_database(file, alias=f"input{i}")
            connection.catalog.create_database(output_path)
            connection.catalog.attach_database(output_path, alias="output")
            connection.catalog.create_schema(SchemaName("output", output_table_name.schema_name))
            if sql:
                union_query = ' UNION ALL\n'.join([f'(\n{sql}\n)'.replace('<schema.table>', f'"input{i}".{input_table_name}') for i in range(len(input_files))])
            else:       
                union_query = ' UNION ALL\n'.join(f"""SELECT * FROM "input{i}".{input_table_name}""" for i in range(len(input_files)))
            #print(union_query)
            create_table_sql = f'CREATE TABLE "output".{output_table_name} AS \n{union_query}'
            connection.execute_command(create_table_sql)

    if rm_input:
        for i in input_files:
            if os.path.exists(i):
                os.remove(i)

def get_parquet_files(folder_path):
    parquet_files = []
    search_pattern = os.path.join(folder_path, '*.parquet')
    parquet_files = glob.glob(search_pattern)
    return parquet_files

def main(parquet_path, output_path=None):
    """parquet file/folder to hyper"""
    is_folder = True
    if '.parquet'.upper() in os.path.basename(parquet_path).upper():
        is_folder = False
    print('Input path is folder?', is_folder)
    if is_folder:
        lt_arg = [(i, None, 'Extract', 'Extract') for i in get_parquet_files(parquet_path)]
        lt_hyper_file = multiprocess_parquet(lt_arg)
        union_hyper( [i[0] for i in lt_hyper_file], output_path)
    else:
        single_parquet_to_hyper(parquet_path, output_path)
    print('P2H process completed.')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert a folder/file of parquet to a single, unioned hyper files')
    parser.add_argument('input', type=str, help='Path to the input parquet file/folder')
    parser.add_argument('output', type=str, help='Path to the output hyper file')
    args = parser.parse_args()
    input = args.input
    output = args.output

    main(input, output)