import psycopg2
from pathlib import Path
import pandas as pd
import psycopg2.extras as pg_extras
"""
 database_description = [
 table1: {
    columns : [
    {col1: typeA},
    {col2: typeB}
    ]
 },
 table2: {
    columns : [
    {col1: typeA},
    {col2: typeB}
    ]
 }
]
"""
sql_insert_scripts = """
                        INSERT IN 
"""
sql_create_scripts = {
        "accounts": """
                        CREATE TABLE IF NOT EXISTS accounts (
                        customer_id INT NOT NULL,
                        first_name VARCHAR(255) NOT NULL,
                        last_name VARCHAR(255) NOT NULL,
                        address_1 VARCHAR(255) NOT NULL,
                        address_2 VARCHAR(255) ,
                        city VARCHAR(255) NOT NULL,
                        state VARCHAR(255) NOT NULL,
                        zip_code INT NOT NULL,
                        join_date VARCHAR(255) NOT NULL,
                        PRIMARY KEY (customer_id))
                    """
,
        "products": """
                        CREATE TABLE IF NOT EXISTS products (
                        product_id INT NOT NULL,
                        product_code INT NOT NULL UNIQUE,
                        product_description VARCHAR(255) NOT NULL UNIQUE,
                        PRIMARY KEY (product_id))
                    """
    ,
        "transactions": """
                            CREATE TABLE IF NOT EXISTS transactions (
                            transaction_id VARCHAR(255) NOT NULL,
                            transaction_date VARCHAR(255) NOT NULL,
                            product_id INT NOT NULL,
                            product_code INT NOT NULL,
                            product_description VARCHAR(255) NOT NULL,
                            quantity INT NOT NULL,
                            account_id INT NOT NULL,
                            PRIMARY KEY (transaction_id),
                            FOREIGN KEY (account_id) REFERENCES accounts (customer_id),
                            FOREIGN KEY (product_code) REFERENCES products (product_code),
                            FOREIGN KEY (product_description) REFERENCES products (product_description))
                        """
    }


def map_data_type(dtype):
    # print(f"mapping data type {dtype}")
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):  
        return "FLOAT"
    elif pd.api.types.is_datetime64_dtype(dtype):  
        return "DATE"
    else:  
        return "VARCHAR(255)"


# TODO: Determine foreign keys from given tables
def check_foreign_keys(tables):
    foreign_keys = []
    for table_name, table_df in tables:
        for other_table_name, other_table_df in tables:
            if table_name != other_table_name:
                for col in table_df.columns:
                    if col in other_table_df.columns:
                        if table_df[col].isin(other_table_df[col]).all():
                            foreign_keys.append({
                                "table": table_name,
                                "column": col,
                                "references_table": other_table_name,
                                "references_column": col
                            })

    return foreign_keys

            

def analyse_csv_file(file):
    try:
        csv_data = pd.read_csv(file)
        table_name = Path(file).stem
        print(f"File {file} analyced completely.\nTable Name: {table_name}\n")
        return table_name, csv_data
    except Exception as error:
        print(f"An error occurred reading file {file}:\n{error}")


# TODO: generate a general create table dll
def generate_ddl(tables, foreign_keys):
    ddl_statements = []
    for table_name, table_df in tables:
        columns = []
        for column, dtype in table_df.dtypes.items():
            sql_type = map_data_type(dtype)
            nullable = "NOT NULL" if table_df[column].isnull().sum() == 0 else ""
            columns.append(f"{column.strip()} {sql_type} {nullable}")

        fk_constraints = [
            f"    FOREIGN KEY ({fk['column']}) REFERENCES {fk['references_table']}({fk['references_column']})"
            for fk in foreign_keys if fk["table"] == table_name
        ]

        sql_create_statement = (
            f"CREATE TABLE {table_name} (\n" +
            ",\n".join(columns + fk_constraints) +
            f",\nPRIMARY KEY ({table_df.columns[0]. strip()}))\n"
        )


        ddl_statements.append(sql_create_statement)

    return ddl_statements

# TODO: create index on each table
def create_index(cursor, table_name, columns):
    print(f"Creating index: {table_name}")
    try:
        # cursor.execute(f"CREATE UNIQUE INDEX {column}_idx ON {table_name}({column});")
        print(f"CREATE UNIQUE INDEX {columns}_idx ON {table_name}({columns});")
    except Exception as error:
        print(f"Could not execute the query: {error}")

def insert_into_table(cursor, table_name, table_df):
    print(f"Inserting into: {table_name}")
    try:
        columns = ','.join(list(table_df.columns))
        values =[ tuple(x) for x in table_df.to_numpy() ]
        query = "INSERT INTO %s(%s) VALUES %%s" %(table_name, columns)
        pg_extras.execute_values(cursor, query, values)
    except Exception as error:
        print(f"Could not execute the query: {error}")

def create_table(cursor, sql_statement):
    try: 
        cursor.execute(sql_statement)
    except Exception as error:
        print(f"Could not execute the query: {error}")


def main():
    host = "postgres"
    database = "postgresdb"
    user = "postgres"
    pas = "postgres"

    base_dir = Path("data")
    
        
    tables = []
   
    cur = None
    conn = None
    try:
        # pass
        conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
        print("Database connected sucesfully!!!\n")
        cur = conn.cursor()   
    except Exception as error:
        print(f"Could not connect to postgres database: {error}")
    else:
        for file in base_dir.iterdir():
            table_name, table_df = analyse_csv_file(file)
            sql_script = sql_create_scripts[table_name].strip()
            print(f"Creating table: {table_name}")
            create_table(cur, sql_script)
            insert_into_table(cur, table_name, table_df)
            


            
        conn.commit()  # Commit the transaction
        cur.close()
        conn.close()
        print('closing connection')


if __name__ == "__main__":
    main()
