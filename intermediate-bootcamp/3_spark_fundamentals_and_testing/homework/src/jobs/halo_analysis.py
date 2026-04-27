"""Halo Spark job: bucket joins + simple aggregation + partitioning experiment."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, broadcast
from pyspark.sql import DataFrame

import os

def load_data(spark: SparkSession):
    """Load all raw CSV inputs into DataFrames."""
    match_details = spark.read.csv("data/match_details.csv", header=True, inferSchema=True)
    matches = spark.read.csv("data/matches.csv", header=True, inferSchema=True)
    medals_matches_players = spark.read.csv("data/medals_matches_players.csv", header=True, inferSchema=True)
    medals = spark.read.csv("data/medals.csv", header=True, inferSchema=True)
    maps = spark.read.csv("data/maps.csv", header=True, inferSchema=True)

    return match_details, matches, medals_matches_players, medals, maps

def write_bucketed_data(spark: SparkSession , match_details: DataFrame, matches: DataFrame, medals_matches_players: DataFrame):
    """Persist big tables as bucketed Spark tables and read them back."""
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
    """Join the bucketed fact tables and broadcast the small dimension tables."""
    bucketed_joined_data = bucket_match_details.join(bucket_matches, on="match_id", how="inner") \
    .join(bucket_medals_matches_players, on="match_id", how="inner") \
    .join(broadcast(medals.select(col("medal_id"),col("name").alias("medal_name"),col("description").alias("medal_description"),col("classification"))), on = "medal_id", how = "inner") \
    .join(broadcast(maps.select(col("mapid"), col("name").alias("map_name"), col("description").alias("map_description"))), on = "mapid", how = "inner") \
    
    return bucketed_joined_data

def most_kills_per_game(bucketed_match_details: DataFrame):
    """Compute average kills per game by player."""

    agg_most_kills_per_game = bucketed_match_details.groupBy("player_gamertag") \
    .agg(avg("player_total_kills")) \
    .withColumnRenamed("avg(player_total_kills)", "avg_kills") \
    .orderBy(col("avg_kills").desc()) 

    return agg_most_kills_per_game


def most_played_playlist(bucket_matches: DataFrame):
    """Find the most played playlist (by playlist_id)."""

    agg_most_played_playlist = bucket_matches.groupBy("playlist_id") \
    .agg(count("match_id")) \
    .withColumnRenamed("count(match_id)", "num_played") \
    .orderBy(col("num_played").desc()) 

    return agg_most_played_playlist

def most_played_map(bucketed_matches: DataFrame , maps: DataFrame):
    """Find the most played map (by map_name)."""

    agg_most_played_map = bucketed_matches.join(broadcast(maps.select(col("mapid"), col("name").alias("map_name"), col("description").alias("map_description"))), on = "mapid", how = "inner") \
    .groupBy("map_name") \
    .agg(count("match_id")) \
    .withColumnRenamed("count(match_id)", "num_played") \
    .orderBy(col("num_played").desc()) 

    return agg_most_played_map

def most_killing_spree(bucket_medals_matches_players: DataFrame, medals: DataFrame, bucket_matches: DataFrame, maps: DataFrame):
    """Count Killing Spree medals per map without joining match_details."""
    # Keep this path lean: joining match_details here multiplies medal rows.
    agg_most_killing_spree = bucket_medals_matches_players \
    .join(broadcast(medals.select(col("medal_id"), col("classification"))), on="medal_id", how="inner") \
    .filter(col("classification") == "KillingSpree") \
    .join(bucket_matches.select(col("match_id"), col("mapid")), on="match_id", how="inner") \
    .join(broadcast(maps.select(col("mapid"), col("name").alias("map_name"))), on="mapid", how="inner") \
    .groupBy("map_name").agg(count("medal_id")) \
    .withColumnRenamed("count(medal_id)", "num_medals") \
    .orderBy(col("num_medals").desc())

    return agg_most_killing_spree



def partition_experiment(df: DataFrame, low_card_col: str, high_card_col: str, output_path: str, name: str):
    """Compare partitioning strategies for low vs high-cardinality columns."""
    df.write.mode("overwrite") \
    .partitionBy(low_card_col) \
    .parquet(f"{output_path}/{name}/low_cardinality") 

    # Same partition key, but locally sorted before write for compression comparison.
    df.repartition(low_card_col) \
        .sortWithinPartitions(col(low_card_col)) \
        .write.mode("overwrite") \
        .partitionBy(low_card_col) \
        .parquet(f"{output_path}/{name}/low_cardinality_sorted") 

    df.repartition(high_card_col) \
        .write.mode("overwrite") \
        .partitionBy(high_card_col) \
        .parquet(f"{output_path}/{name}/high_cardinality") 

def get_folder_size(path: str) -> int:
    """Compute total size of a folder (bytes)."""
    total = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            total += os.path.getsize(filepath)
    return total

def main():
    """Entry point: build Spark session, run joins, aggregations, and experiments."""
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
    agg_played_map = most_played_map(bucket_matches, maps)
    agg_played_map.show(10)
    agg_killing_spree = most_killing_spree(bucket_medals_matches_players, medals, bucket_matches, maps)
    agg_killing_spree.show(10)

    partition_experiment(bucket_matches, "playlist_id", "game_mode", "output/sorted_tables/", "playlist_id_partitioning")
    partition_experiment(bucket_matches, "mapid", "game_mode", "output/sorted_tables/", "mapid_partitioning")

    print("""--------------------------------Playlist ID Partitioning Experiment--------------------------------""")
    print(f"Size of low cardinality: {get_folder_size('output/sorted_tables/playlist_id_partitioning/low_cardinality')}")
    print(f"Size of low cardinality sorted: {get_folder_size('output/sorted_tables/playlist_id_partitioning/low_cardinality_sorted')}")
    print(f"Size of high cardinality: {get_folder_size('output/sorted_tables/playlist_id_partitioning/high_cardinality')}")

    print("""--------------------------------Map ID Partitioning Experiment--------------------------------""")
    print(f"Size of low cardinality: {get_folder_size('output/sorted_tables/mapid_partitioning/low_cardinality')}")
    print(f"Size of low cardinality sorted: {get_folder_size('output/sorted_tables/mapid_partitioning/low_cardinality_sorted')}")
    print(f"Size of high cardinality: {get_folder_size('output/sorted_tables/mapid_partitioning/high_cardinality')}")



if __name__ == "__main__":
    main()


