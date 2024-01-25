from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, udf
from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import IntegerType
import time

def mesure_performance(df, with_persistence):
    if with_persistence:
        df.persist(StorageLevel.DISK_ONLY_3)

    score_udf = udf(calculate_score, IntegerType())

    times = []
    for _ in range(150):
        start_time = time.time()

        tmp = df.withColumn("score", score_udf(col("player_assists"), col("player_dmg"), col("player_kills"),
                                              col("team_placement")))

        # Trouver les 10 meilleurs joueurs
        top_players = tmp.groupBy("player_name").sum("score").orderBy("sum(score)", ascending=False).limit(10)

        end_time = time.time()
        times.append(end_time - start_time)

    if with_persistence:
        df.unpersist()

    return times





def calculate_score(assists, damage, kills, rank):
    score = 50 * assists + damage + 100 * kills
    if rank == 1:
        score += 1000
    else:
        score += 1000 - ((rank - 1) * 10)
    return score


# Initialisation de Spark
spark = SparkSession.builder.appName("Analyse des Données de Jeu Avancée").getOrCreate()

# Chargement des données
df = spark.read.csv("agg_match_stats_0.csv", header=True, inferSchema=True)
# Calculer le nombre de parties par joueur
df_player_games = df.groupBy("player_name").agg(count("*").alias("number_of_games"))

# Joindre avec le DataFrame original
df_joined = df.join(df_player_games, "player_name")

# Filtrer les joueurs ayant joué au moins 4 parties et avec un nom non null
df_filtered = df_joined.filter(col("number_of_games") >= 4).filter(col("player_name").isNotNull())

# Mesure du temps avec persistance
times_with_persist = mesure_performance(df_filtered, with_persistence=True)

# Mesure du temps sans persistance
times_without_persist = mesure_performance(df_filtered, with_persistence=False)

print("Temps avec persistance: " + str(sum(times_with_persist) / len(times_with_persist)))
print("Temps sans persistance: " + str(sum(times_without_persist) / len(times_without_persist)))
