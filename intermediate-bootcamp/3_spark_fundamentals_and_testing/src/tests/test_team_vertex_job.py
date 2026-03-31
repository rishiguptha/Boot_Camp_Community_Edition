from chispa.dataframe_comparer import *
from ..jobs.team_vertex_job import do_team_vertex_transformation
from collections import namedtuple

TeamVertex = namedtuple("TeamVertex", "identifier type properties")
Team = namedtuple("Team", "team_id abbreviation nickname city arena year_founded")


def test_vertex_generation(spark):
    input_data = [
        Team(team_id=1, abbreviation="BOS", nickname="Celtics", city="Boston", arena="TD Garden", year_founded=1946),
        Team(team_id=1, abbreviation="BOS", nickname="Bad Celtics", city="Boston", arena="TD Garden", year_founded=1946),
        Team(team_id=2, abbreviation="GSW", nickname="Warriors", city="San Francisco", arena="Chase Center", year_founded=1900),
        Team(team_id=3, abbreviation="LAL", nickname="Lakers", city="Los Angeles", arena="Staples Center", year_founded=1900),
    ]

    input_df = spark.createDataFrame(input_data)
    actual_df = do_team_vertex_transformation(spark, input_df)

    expected_output = [
        TeamVertex(identifier=1, type="team", properties={"abbreviation": "BOS", "nickname": "Celtics", "city": "Boston", "arena": "TD Garden", "year_founded": "1946"}),
        TeamVertex(identifier=2, type="team", properties={"abbreviation": "GSW", "nickname": "Warriors", "city": "San Francisco", "arena": "Chase Center", "year_founded": "1900"}),
        TeamVertex(identifier=3, type="team", properties={"abbreviation": "LAL", "nickname": "Lakers", "city": "Los Angeles", "arena": "Staples Center", "year_founded": "1900"}),
    ]

    expected_df = spark.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df,ignore_nullable=True)
