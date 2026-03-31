from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, broadcast
from pyspark.sql import DataFrame

import os

def load_data(spark: SparkSession):
    match_details = spark.read.csv("data/match_details.csv", header=True, inferSchema=True)
    matches = spark.read.csv("data/matches.csv", header=True, inferSchema=True)
    medals_matches_players = spark.read.csv("data/medals_matches_players.csv", header=True, inferSchema=True)
    medals = spark.read.csv("data/medals.csv", header=True, inferSchema=True)
    maps = spark.read.csv("data/maps.csv", header=True, inferSchema=True)

    return match_details, matches, medals_matches_players, medals, maps

def write_bucketed_data(spark: SparkSession , match_details: DataFrame, matches: DataFrame, medals_matches_players: DataFrame):
    match_details.write.mode("overwrite") \
    .bucketBy(16, "match_id") \
    .option("path", "output/bucketed_tables/bucketed_match_details") \
    .saveAsTable("bucketed_match_details")


    matches.write.mode("overwrite") \
    .bucketBy(16, "match_id") \
    .option("path", "output/bucketed_tables/bucketed_matches") \
    .saveAsTable("bucketed_matches") 


    medals_matches_players.write.mode("overwrite") \
    .bucketBy(16, "match_id") \
    .option("path", "output/bucketed_tables/bucketed_medals_matches_players") \
    .saveAsTable("bucketed_medals_matches_players") 

    bucket_match_details = spark.table("bucketed_match_details")
    bucket_matches = spark.table("bucketed_matches")
    bucket_medals_matches_players = spark.table("bucketed_medals_matches_players")

    return bucket_match_details, bucket_matches, bucket_medals_matches_players

def join_bucketed_data(spark: SparkSession , bucket_match_details: DataFrame, bucket_matches: DataFrame, bucket_medals_matches_players: DataFrame, medals: DataFrame, maps: DataFrame):
    bucketed_joined_data = bucket_match_details.join(bucket_matches, on="match_id", how="inner") \
    .join(bucket_medals_matches_players, on="match_id", how="inner") \
    .join(broadcast(medals.withColumnsRenamed({"name": "medal_name", "description": "medal_description"})), on = "medal_id", how = "inner") \
    .join(broadcast(maps.withColumnsRenamed({"name": "map_name", "description": "map_description"})), on = "mapid", how = "inner") \
    
    return bucketed_joined_data

def most_kills_per_game(bucketed_match_details: DataFrame):

    agg_most_kills_per_game = bucketed_match_details.groupBy("player_gamertag") \
    .agg(avg("player_total_kills")) \
    .withColumnRenamed("avg(player_total_kills)", "avg_kills") \
    .orderBy(col("avg_kills").desc()) 

    return agg_most_kills_per_game


def most_played_playlist(bucketed_matches: DataFrame):

    agg_most_played_playlist = bucketed_matches.groupBy("playlist_id") \
    .agg(count("match_id")) \
    .withColumnRenamed("count(match_id)", "num_played") \
    .orderBy(col("num_played").desc()) 

    return agg_most_played_playlist

def most_played_map(bucketed_matches: DataFrame):

    agg_most_played_map = bucketed_matches.groupBy("mapid") \
    .agg(count("match_id")) \
    .withColumnRenamed("count(match_id)", "num_played") \
    .orderBy(col("num_played").desc()) 

    return agg_most_played_map

def most_killing_spree(bucketed_joined_data: DataFrame):
    agg_most_killing_spree = bucketed_joined_data.filter(col("classification") == "KillingSpree") \
    .groupBy("map_name").agg(count("medal_id")) \
    .withColumnRenamed("count(medal_id)", "num_medals") \
    .orderBy(col("num_medals").desc())

    return agg_most_killing_spree


def sort_experiment(df: DataFrame, low_card_col: str, high_card_col: str, output_path: str, name: str):

    df.sortWithinPartitions(col(low_card_col)) \
        .write.mode("overwrite") \
        .parquet(f"{output_path}/{name}/low_cardinality") 

    df.sortWithinPartitions(col(high_card_col)) \
        .write.mode("overwrite") \
        .parquet(f"{output_path}/{name}/high_cardinality") 


def get_folder_size(path: str) -> int:
    total = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            total += os.path.getsize(filepath)
    return total

def main():
    spark = SparkSession.builder \
    .master("local") \
    .appName("HaloAnalysis") \
    .config("spark.sql.warehouse.dir", "output/bucketed_tables") \
    .getOrCreate()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    match_details, matches, medals_matches_players, medals, maps = load_data(spark)
    bucket_match_details, bucket_matches, bucket_medals_matches_players = write_bucketed_data(spark, match_details, matches, medals_matches_players)
    bucketed_joined_data = join_bucketed_data(spark, bucket_match_details, bucket_matches, bucket_medals_matches_players, medals, maps)

    bucketed_joined_data.explain()

    agg_kills_per_game = most_kills_per_game(bucket_match_details)
    agg_kills_per_game.show(10)
    agg_played_playlist = most_played_playlist(bucket_matches)
    agg_played_playlist.show(10)
    agg_played_map = most_played_map(bucket_matches)
    agg_played_map.show(10)
    agg_killing_spree = most_killing_spree(bucketed_joined_data)
    agg_killing_spree.show(10)

    sort_experiment(bucket_matches, "is_team_game", "match_id", "output/sorted_tables/", "team_game_sorting")
    sort_experiment(bucket_medals_matches_players, "count", "match_id", "output/sorted_tables/", "medal_id_sorting")
    print("""--------------------------------Team Game Sorting Experiment--------------------------------""")
    print(f"Size of low cardinality: {get_folder_size('output/sorted_tables/team_game_sorting/low_cardinality')}")
    print(f"Size of high cardinality: {get_folder_size('output/sorted_tables/team_game_sorting/high_cardinality')}")
    print("""--------------------------------Count Sorting Experiment--------------------------------""")
    print(f"Size of low cardinality: {get_folder_size('output/sorted_tables/medal_id_sorting/low_cardinality')}")
    print(f"Size of high cardinality: {get_folder_size('output/sorted_tables/medal_id_sorting/high_cardinality')}")


if __name__ == "__main__":
    main()


