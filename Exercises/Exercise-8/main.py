import duckdb
from dataclasses import dataclass, field


@dataclass 
class TableConfiguration:
    path: str
    table_name: str
    schema: dict
    kwargs: dict

table_cfg = TableConfiguration(
    path="data/Electric_Vehicle_Population_Data.csv",
    table_name="e_vehicle_population",
    schema={
        "VIN (1-10)": "VARCHAR",
        "County": "VARCHAR",
        "City": "VARCHAR",
        "State": "VARCHAR",
        "Postal Code": "BIGINT",
        "Model Year": "BIGINT",
        "Make": "VARCHAR",
        "Model": "VARCHAR",
        "Electric Vehicle Type": "VARCHAR",
        "Clean Alternative Fuel Vehicle (CAFV) Eligibility": "VARCHAR",
        "Electric Range": "BIGINT",
        "Base MSRP": "BIGINT",
        "Legislative District": "BIGINT",
        "DOL Vehicle ID": "BIGINT",
        "Vehicle Location": "VARCHAR",
        "Electric Utility": "VARCHAR",
        "2020 Census Tract": "BIGINT",
    },
    kwargs={"header": True}
)

def create_table_from_csv(table_cfg: TableConfiguration, connection: duckdb.DuckDBPyConnection = duckdb):
    csv_data = connection.read_csv(table_cfg.path, columns=table_cfg.schema, **table_cfg.kwargs)
    connection.sql(f"CREATE TABLE {table_cfg.table_name} AS SELECT * FROM csv_data")

def query_electric_vehicles_per_city() -> str:
    """
    Count the number of electic vehicles per city.
    """
    return """
    SELECT city, COUNT(*) AS num_of_vehicles
    FROM e_vehicle_population
    GROUP BY city;
    """

def query_top_3_most_popular_electric_vehicles() -> str:
    """
    Find the top 3 most popular electric vehicles
    """
    return """
    SELECT Make, Model, Count(*) AS count
    FROM e_vehicle_population
    GROUP BY Make, Model
    ORDER BY count DESC
    LIMIT 3;
    """


def query_the_most_popular_electric_vehicles_in_each_postal_code() -> str:
    """
    Find the most popular electric vehicles in each postal code
    """
    return """
    -- Find the number of electrical vehicles for each in model per postal code
    WITH vehicle_count AS(
    SELECT Make, Model, "Postal Code", COUNT(*) AS count
    FROM e_vehicle_population
    GROUP BY Make, Model, "Postal Code"
    )

    SELECT Make, Model, "Postal Code", count
    FROM (
    SELECT Make, Model, "Postal Code", count,
    RANK() OVER (PARTITION BY "Postal Code" ORDER BY count DESC) AS rank
    FROM vehicle_count
    )
    WHERE rank = 1;
    """
def main():
    create_table_from_csv(table_cfg)
    list_of_queries = [
        query_electric_vehicles_per_city(),
        query_top_3_most_popular_electric_vehicles(),
        query_the_most_popular_electric_vehicles_in_each_postal_code(),
    ]

    for query in list_of_queries:
        duckdb.sql(query).show()


# Create table from csv file -> electric-car.csv



if __name__ == "__main__":
    main()
