import pyspark.pandas as ps

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("arbres_de_paris") \
    .config("spark.sql.ansi.enabled", "false") \
    .getOrCreate()
sc = spark.sparkContext

df_spark = ps.read_csv("arbresremarquablesparis.csv", sep=";")

df_spark['Année de plantation'] = ps.to_numeric(df_spark['Année de plantation'], errors="coerce")

print("Le plus vieil arbre a été planté en :", end=" ")
print(df_spark['Année de plantation'].min())

print("Le circonference de plus grand arbre est de :", end=" ")
print(df_spark.loc[df_spark['hauteur en m'].argmax(), 'circonference en cm'], "cm")

print("La hauteur moyenne des arbres est de :", end=" ")
print(df_spark['hauteur en m'].mean(), "m")

print("L'arrondissement du plus grand arbre est le :", end=" ")
print(df_spark.loc[df_spark['hauteur en m'].argmax(), 'arrondissement3'])

print("Nombre d'arbres situés au Cimetière du Père Lachaise :", end=" ")
print(len(df_spark.loc[df_spark['Site'] == "Cimetière du Père Lachaise"]))

sc.stop()
