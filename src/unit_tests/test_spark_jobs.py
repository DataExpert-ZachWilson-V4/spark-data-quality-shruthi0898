from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from collections import namedtuple
from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2
import pytest

# Named tuple for job 1 test data
ActorFilm = namedtuple("ActorFilm", "actor film votes rating film_id year")
Actor = namedtuple("Actor", "actor_id actor current_year")

# Test job 1
def test_job_1(self, spark_session):
    # Define fake input data
    actor_films_data = [
        ActorFilm("Actor1", "Film1", 100, 8.0, 1, 2023),
        ActorFilm("Actor1", "Film2", 150, 7.5, 2, 2023),
        ActorFilm("Actor2", "Film3", 80, 6.5, 3, 2023),
        ActorFilm("Actor3", "Film4", 120, 8.5, 4, 2023)
    ]
    actors_data = [
        Actor(1, "Actor1", 2022),
        Actor(2, "Actor2", 2022),
        Actor(3, "Actor3", 2022)
    ]
    
    actor_films_df = spark_session.createDataFrame(actor_films_data)
    actors_df = spark_session.createDataFrame(actors_data)

    # Execute job
    output_table_name = "test_job1_output"
    result_df = job_1(spark_session, actor_films_df, actors_df, 2023, 2022, output_table_name)

    # Define expected output data
    expected_data = [
        ActorFilm("Actor1", "Film1", 100, 8.0, 1, 2023),
        ActorFilm("Actor1", "Film2", 150, 7.5, 2, 2023),
        ActorFilm("Actor2", "Film3", 80, 6.5, 3, 2023),
        ActorFilm("Actor3", "Film4", 120, 8.5, 4, 2023),
        ActorFilm("Actor1", "Film1", 100, 8.0, 1, 2022),
        ActorFilm("Actor1", "Film2", 150, 7.5, 2, 2022),
        ActorFilm("Actor2", "Film3", 80, 6.5, 3, 2022),
        ActorFilm("Actor3", "Film4", 120, 8.5, 4, 2022)
    ]
    expected_df = spark_session.createDataFrame(expected_data)

    # Perform assertions on the result DataFrame
    assert_df_equality(result_df, expected_df)
    
# Named tuple for job 2 test data
WebEvent = namedtuple("WebEvent", "user_id device_id event_time")
Device = namedtuple("Device", "device_id browser_type")
    
# Test job 2
def test_job_2(self, spark_session):
        # Define fake input data for job 2
        web_events_data = [
            WebEvent(1, 101, "2023-01-01 10:00:00"),
            WebEvent(1, 101, "2023-01-02 11:00:00"),
            WebEvent(2, 102, "2023-01-01 09:00:00"),
            WebEvent(3, 103, "2023-01-02 08:00:00"),
            WebEvent(3, 103, "2023-01-03 14:00:00")
        ]
        devices_data = [
            Device(101, "Chrome"),
            Device(102, "Firefox"),
            Device(103, "Safari")
        ]
        
        web_events_df = spark_session.createDataFrame(web_events_data)
        devices_df = spark_session.createDataFrame(devices_data)

        # Execute job 2
        result_df = job_2(spark_session, web_events_df, devices_df)

        # Define expected output data for job 2
        expected_data = [
            WebEvent(1, 101, "2023-01-02"),
            WebEvent(2, 102, "2023-01-01"),
            WebEvent(3, 103, "2023-01-03")
        ]
        expected_df = spark_session.createDataFrame(expected_data)

        # Perform assertions on the result DataFrame for job 2
        assert_df_equality(result_df, expected_df)

if __name__ == "__main__":
    pytest.main([__file__])