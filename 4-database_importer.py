import pandas as pd
from db import connection
from db_utils import DBUtils

def main():

    entities = pd.read_csv("./standardization/entities-standardized.csv")

    with connection as cnx:

        db = DBUtils(cnx)

        all_entities = entities.copy()
        new_entities = all_entities[all_entities["db_entity_id"].isnull()]

        for _, entity in new_entities.iterrows():
            entity_id = entity.name
            entity_name = entity["name"]
            db_entity_id = db.get_or_create_entity(entity_name)
            all_entities.loc[entity_id, "db_entity_id"] = db_entity_id

        db_entity_id_by_name = {
            row["name"]: int(row["db_entity_id"])
            for _, row in all_entities.iterrows()
        }

        # Inserting the dataset
        db_dataset_id = db.upsert_dataset(
            name="BP Statistical Review of Global Energy",
            namespace="bpstatreview_2019",
            user_id=35
        )

        #Inserting the source
        db_source_id = db.upsert_source(
            name="BP Statistical Review of Global Energy (2019)",
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
                db_entity_id_by_name[row["country"]],
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
