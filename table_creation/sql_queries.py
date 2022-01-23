### INSTRUCTIONS
# Fact Table
# songplays - records in log data associated with song plays i.e. records with page NextSong
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
# Dimension Tables
# users - users in the app
## user_id, first_name, last_name, gender, level
# songs - songs in music database
## song_id, title, artist_id, year, duration
# artists - artists in music database
## artist_id, name, location, latitude, longitude
# time - timestamps of records in songplays broken down into specific units
## start_time, hour, day, week, month, year, weekday

import sys
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from scripts.templating import fill_template

schemas={}
songplays={}
songplays["table_description"] = "records in log data associated with song plays i.e. records with page NextSong" 
songplays["songplay_id"] = "SERIAL_PRIMARY KEY"
songplays["start_time"] = "timestamptz_NOT NULL"
songplays["user_id"] = "int"
songplays["level"] = "varchar"
songplays["song_id"] = "varchar"
songplays["artist_id"] = "varchar"
songplays["session_id"] = "int"
songplays["location"] = "varchar"
songplays["user_agent"] = "varchar"

schemas["songplays"]=songplays

# --
users={}
users["table_description"] = "Users in the app" 
users["user_id"] = "int_PRIMARY KEY"
users["first_name"] = "varchar"
users["last_name"] = "varchar"
users["gender"] = "varchar"
users["level"] = "varchar"

schemas["users"]=users

# --
songs={}
songs["table_description"] = "Songs in music database" 
songs["song_id"]   = "varchar_PRIMARY KEY"
songs["title"]     = "varchar"
songs["artist_id"] = "varchar"
songs["year"]      = "int"
songs["duration"]  = "float"

schemas["songs"]=songs

# --
artists={}
artists["table_description"] = "Artists in music database" 
artists["artist_id"]  = "varchar_PRIMARY KEY"
artists["name"]       = "varchar"
artists["location"]   = "varchar"
artists["latitude"]   = "Decimal(8,6)"
artists["longitude"]  = "Decimal(9,6)"
#TODO: check lat/long type -- https://stackoverflow.com/questions/1196415/what-datatype-to-use-when-storing-latitude-and-longitude-data-in-sql-databases
schemas["artists"]=artists


# --
time={}
time["table_description"] = "Timestamps of records in songplays broken down into specific units" 
time["start_time"] = "timestamptz_PRIMARY KEY_NOT NULL"
time["hour"]       = "int"
time["day"]        = "int"
time["week"]       = "int"
time["month"]      = "int"
time["year"]       = "int"
time["weekday"]    = "int"

schemas["time"]=time

actions = ["drop_table", "create_table"]
schemas_names= ["songplays", "users", "songs", "artists", "time"]

execute_queries=[]
for action in actions:
    for table_name in schemas_names:
        execute_queries.append(fill_template(key=table_name, template=action, schemas_dict=schemas))

song_select="""
select
    song_id, artist_id
from 
    songs
inner join artists using (artist_id)
where
    songs.title=%s
    and artists.name=%s
    and songs.duration=%s
"""

songplay_table_insert= """
    Insert INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent 
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)

"""