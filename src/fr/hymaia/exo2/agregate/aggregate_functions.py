from pyspark.sql.functions import desc, count


def departement_count(df):
    # Calculer le nombre de personnes par département
    result_df = df.groupBy("departement").agg(count("*").alias("nb_people"))

    # Trier le résultat par nombre de personnes décroissant et par ordre alphabétique du département
    result_df = result_df.orderBy(desc("nb_people"), "departement")

    return result_df