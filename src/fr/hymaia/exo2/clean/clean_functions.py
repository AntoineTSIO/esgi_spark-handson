from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def add_departement_column(df):
    extract_departement_udf = F.udf(lambda zip_code: str(zip_code)[:2], StringType())

    result_df = df.withColumn("departement",
                              F.when((df["zip"] >= 20000) & (df["zip"] <= 20190), "2A")
                              .when((df["zip"] > 20190) & (df["zip"] < 21000), "2B")
                              .otherwise(extract_departement_udf(df["zip"])))

    return result_df


def join_dataframes(clients_df, villes_df):
    result_df = clients_df.join(villes_df, clients_df["zip"] == villes_df["zip"]).select(
        clients_df["name"],
        clients_df["age"],
        villes_df["zip"],
        villes_df["city"]
    )
    return result_df