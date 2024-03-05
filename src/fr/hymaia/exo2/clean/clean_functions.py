from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def add_departement_column(df):
    extract_departement_udf = F.udf(lambda zip_code: str(zip_code)[:2], StringType())

    result_df = df.withColumn("departement",
                              F.when((df["zip"] >= 20000) & (df["zip"] <= 20190), "2A")
                              .when((df["zip"] > 20190) & (df["zip"] < 21000), "2B")
                              .when((df["zip"] >= 97100) & (df["zip"] <= 97199), "971")
                              .when((df["zip"] >= 97200) & (df["zip"] <= 97299), "972")
                              .when((df["zip"] >= 97300) & (df["zip"] <= 97399), "973")
                              .when((df["zip"] >= 97400) & (df["zip"] <= 97499), "974")
                              .when((df["zip"] >= 97600) & (df["zip"] <= 97699), "976")
                              .when((df["zip"] >= 1000) & (df["zip"] <= 1999), "01")
                              .when((df["zip"] >= 2000) & (df["zip"] <= 2999), "02")
                              .when((df["zip"] >= 3000) & (df["zip"] <= 3999), "03")
                              .when((df["zip"] >= 4000) & (df["zip"] <= 4999), "04")
                              .when((df["zip"] >= 5000) & (df["zip"] <= 5999), "05")
                              .when((df["zip"] >= 6000) & (df["zip"] <= 6999), "06")
                              .when((df["zip"] >= 7000) & (df["zip"] <= 7999), "07")
                              .when((df["zip"] >= 8000) & (df["zip"] <= 8999), "08")
                              .when((df["zip"] >= 9000) & (df["zip"] <= 9999), "09")
                              .otherwise(extract_departement_udf(df["zip"])))

    result_df = result_df.filter(df["zip"] >= 1000).filter(df["zip"] <= 97699)

    result_df = result_df.filter(~result_df["departement"].isin("97"))

    return result_df


def join_dataframes(clients_df, villes_df):
    result_df = clients_df.join(villes_df, "zip")
    result_df = result_df.select("name", "age", "zip", "city")
    return result_df


def filter_major_clients(df):
    filtered_df = df.filter(df["age"] >= 18)
    return filtered_df
