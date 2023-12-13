from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

# Initialisation de la session Spark
spark = SparkSession.builder.appName("Top Players Analysis").getOrCreate()

# Lecture du fichier CSV
df = spark.read.csv("sample_agg_match_stats_0.csv", header=True, inferSchema=True)

# remove null values
df = df.dropna()


# Définition de la fonction de calcul du score
def calculate_score(assists, damage, kills, rank):
    score = 50 * assists + damage + 100 * kills
    if rank == 1:
        score += 1000
    else:
        score += 1000 - ((rank-1) * 10)
    return score

# Enregistrer la fonction UDF
score_udf = udf(calculate_score, IntegerType())

df = df.withColumn("score", score_udf(col("player_assists"), col("player_dmg"), col("player_kills"), col("team_placement")))

# Trouver les 10 meilleurs joueurs
top_players = df.groupBy("player_name").sum("score").orderBy("sum(score)", ascending=False).limit(10)

top_players.show()

# Fermeture de la session Spark
spark.stop()
