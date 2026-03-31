from pyspark.sql import SparkSession

query = """
    WITH teams_deduped AS (
    SELECT * ,
        ROW_NUMBER() OVER(PARTITION BY team_id ORDER BY team_id) as row_num
    FROM teams
)
SELECT team_id as identifier,
    'team' AS `type`,
    MAP(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', CAST(year_founded AS STRING)
    )AS properties
FROM teams_deduped
WHERE row_num = 1
"""

def do_team_vertex_transformation(spark,dataframe):
    dataframe.createOrReplaceTempView("teams")
    return spark.sql(query)



def main():
    spark = SparkSession.master("local").appName("team_vertex").getOrCreate()
    output_df = do_team_vertex_transformation(spark, spark.table("players"))
    output_df.write.mode("overwrite").insertInto("team_vertex")