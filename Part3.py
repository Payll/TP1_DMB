from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

# 1
spark = SparkSession.builder.appName("Analyse des Données de Jeu").getOrCreate()
df = spark.read.csv("sample_agg_match_stats_0.csv", header=True, inferSchema=True)
df = df.dropna() # 6
df.printSchema()

# 2. Sélection du nom du joueur et du nombre d'éliminations
df_selected = df.select(col("player_name"), col("player_kills"))

# 3. Calcul de la moyenne des éliminations et du nombre de parties par joueur
df_avg_kills = df_selected.groupBy("player_name").agg(
    avg("player_kills").alias("average_kills"),
    count("player_name").alias("number_of_games")
)

# 4. Obtenir les 10 meilleurs joueurs selon les éliminations
top_players = df_avg_kills.sort(col("average_kills").desc())

# 5. Filtrer les joueurs ayant au moins 4 parties
filtered_players = top_players.filter(col("number_of_games") >= 4).limit(10)


# Affichage des résultats pour vérification
filtered_players.show()  # Afficher les joueurs filtrés
