import pandas as pd
from db import connection
from db_utils import DBUtils

def main():

    bp_entities = pd.read_csv("./standardization/entities.csv")
    std_entities = pd.read_csv("./standardization/entities-standardized.csv")
    new_entities = bp_entities[-bp_entities.name.isin(std_entities.name)]

    if len(new_entities) > 0:
        print("The following entities do not exist yet in entities-standardized.csv")
        print(new_entities)
        print("Press CTRL+C to cancel and match+add them yourself to entities-standardized.csv")
        _ = input("or press ENTER to proceed and look them up (or create) in the database: ")

    std_entities = pd.concat([std_entities, new_entities]).reset_index(drop=True)
    std_entities.loc[
        std_entities.standardized_name.isnull(), "standardized_name"
    ] = std_entities.name

    db = DBUtils(connection.cursor())

    for _, row in std_entities[std_entities.db_entity_id.isnull()].iterrows():
        std_entities.loc[
            std_entities.standardized_name == row["standardized_name"], "db_entity_id"
        ] = db.get_or_create_entity(row["standardized_name"])

    db_entity_id_by_bp_name = dict(zip(std_entities.name, std_entities.db_entity_id))

    # Inserting the dataset
    db_dataset_id = db.upsert_dataset(
        name="BP Statistical Review of Global Energy",
        namespace="bpstatreview_2020",
        user_id=35
    )

    #Inserting the source
    db_source_id = db.upsert_source(
        name="BP Statistical Review of Global Energy (2020)",
        description="",
        dataset_id=db_dataset_id
    )

    # Inserting variables
    variables = pd.read_csv("output/variables.csv")

    for _, variable in variables.iterrows():

        print("Inserting variable: %s" % variable["name"])
        db_variable_id = db.upsert_variable(
            name=variable["name"],
            code=None,
            unit=variable["unit"],
            short_unit=None,
            source_id=db_source_id,
            dataset_id=db_dataset_id,
            description=variable["notes"] if pd.notnull(variable["notes"]) else ""
        )

        data_values = pd.read_csv("./output/datapoints/datapoints_%d.csv" % variable.id)

        values = [(
            float(row["value"]),
            int(row["year"]),
            db_entity_id_by_bp_name[row["country"]],
            db_variable_id
        ) for _, row in data_values.iterrows()]

        print("Inserting values…")
        db.upsert_many("""
            INSERT INTO data_values (value, year, entityId, variableId)
            VALUES (%s, %s, %s, %s)
        """, values)
        print("Inserted %d values for variable" % len(values))

if __name__ == "__main__":
    main()
