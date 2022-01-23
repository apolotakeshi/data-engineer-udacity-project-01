import os
import glob
import psycopg2
import pandas as pd
import json
from scripts.templating import choose_template
from table_creation.create_tables import generate_uri
from table_creation.sql_queries import song_select, songplay_table_insert

def get_files(filepath, extension="*.json"):
    """
    - read all the files that contains a specific extension
    - return their full path
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, extension))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def filter_out_val(df, col_name, val):
    """
    - Receives DataFrame, column and value to be filter out
    - Returns a DataFrame
    """
    return df[df[col_name] != val]

def filter_out(df, filter_out_dict):
    """
    - Receives a DataFrame and a dict
    - uses the dict and for each key filter out that value
    -- filter_out_multiple_vals haven't been implemented yet (https://github.com/pandas-dev/pandas/pull/29636/)
    - Returns DataFrame after filters
    """
    df = df.copy()
    for col_name, val in filter_out_dict.items():
        tmp_df = filter_out_val(df, col_name, val)
        df = tmp_df
    return df


def df_cleanse(df, filter_cols, filter_out_dict={}, astype={}, rename_map=[], duplicates_considering=[]):
    """
    - Since Pandas hasn't implemented UPSERT yet, this method considers also this adjustment
    - It runs all cleanse in a dataframe before injection in a table
    - Returns DataFrame
    """
    
    df = df[filter_cols]
    df = filter_out(df, filter_out_dict)
    df = df.astype(astype)
    df = df.rename(columns=rename_map)
    if len(duplicates_considering) > 0:
        df = df.drop_duplicates(duplicates_considering)
    else:
        df = df.drop_duplicates()
    return df

def read_json(file):
    """
    - read a json file
    - Returns a pandas DataFrame
    """
    return pd.read_json(file, lines=True)

def to_str(input_list):
    """
    - converts a list into a single string 
    example: ['a','b'] -> '(a,b)'
    """
    return str(tuple(input_list)).replace("'","")

def prepare_columns(df, template_formatted="%({})s"):
    """
    - receives a dataframe and a format template
    - convert each of its columns into this template
    - Returns its columns_name into a string and a string formatted
    example df['a','b'] -> '(a,b)', '( %(a)s, %(b)s )' 
    """
    df_columns= df.columns
    flatten= []
    for elem in df_columns:
        flatten.append(template_formatted.format(elem))
    
    return to_str(df_columns), to_str(flatten)

def inject_template(df
        , database_table_name: str
        , conflict_rule: str =""
        , conflict_action="NOTHING"
        , template_name="insert_data"):
    """
    Requires
    df :
        pandas dataframe
    
    database_table_name :
        Name of SQL table.
    
    conn : SQLAlchemy connectable(engine/connection) or database string URI
        or sqlite3 DBAPI2 connection
        Using SQLAlchemy makes it possible to use any DB supported by that
        library.
        If a DBAPI2 object, only sqlite3 is supported.
    
    """
    
    template = choose_template(template_name)
    columns_names, columns_names_formatted = prepare_columns(df)
    
    d={}
    d["columns_names"] = columns_names
    d["columns_names_formatted"] = columns_names_formatted
    d["database_table_name"]= database_table_name
    d["conflict_rule"] = conflict_rule
    d["conflict_action"] = conflict_action
    
    return template.format(**d)

def inject_data(
        df
        , cur
        , conn
        , injection_rule
        , show_status_at = 250
    ):
    """
    Requires
    df
        pandas dataframe
        
    conn : SQLAlchemy connectable(engine/connection) or database string URI
        or sqlite3 DBAPI2 connection
        Using SQLAlchemy makes it possible to use any DB supported by that
        library.
        If a DBAPI2 object, only sqlite3 is supported.
    
    """
    
    
    injection_data_list = json.loads( df.to_json(orient="records") )
    
    print("Start processing the {}".format(injection_rule), flush=True)
    print("...", flush=True)

    for counter, injection_data in enumerate(injection_data_list):
        letter =""
        size = len(injection_data_list) -1
        
        cur.execute(injection_rule, injection_data)
        conn.commit()

        if counter == len(injection_data_list) -1:
            letter = "! DONE"
            print(flush=True)
        else: 
            if size < show_status_at:
                letter = "."
            else:
                if counter % show_status_at == 0:
                    letter = "."
        
        print(letter, end="", flush=True)

def process_song_file(conn, cur, find_path, show_status_at=250):
    """
    Pipeline to prepare the songs
    - retrieve
    - transform and clean data
    - insert them in the database
    """
    
    # open song file
    song_files = get_files(find_path)

    df = pd.concat(
                    map(
                        read_json
                        , song_files
                    )
                )

    songs_desired_columns= [
        "song_id"
        , "title"
        , "artist_id"
        , "year"
        , "duration"
    ]

    ## Songs
    song_data = df[songs_desired_columns]\
    .drop_duplicates()

    # insert song record    
    inject_data(
                song_data
                , cur
                , conn
                , injection_rule= inject_template(
                                                    song_data
                                                    , "songs"
                                                    , conflict_rule="(song_id)"
                                                    , conflict_action="""NOTHING"""
                                                )
            )
    
    ## Artists
    artists_desired_columns= [
        "artist_id"
        , "artist_name"
        , "artist_location"
        , "artist_latitude"
        , "artist_longitude"
    ]

    artists_rename_map = {
        "artist_name"        : "name"
        , "artist_location"  : "location"
        , "artist_latitude"  : "latitude"
        , "artist_longitude" : "longitude"
    }

    artist_data = df[artists_desired_columns]\
        .rename(columns=artists_rename_map)\
        .drop_duplicates()
    
    # insert artist record
    inject_data(
        artist_data
        , cur
        , conn
        , injection_rule= inject_template(
                artist_data
                                            , "artists"
                                            , conflict_rule="(artist_id)"
                                            , conflict_action="""NOTHING"""
                                        )
        , show_status_at=show_status_at
    )


def process_log_file(conn, cur, find_path, show_status_at=250):
    """
    Pipeline to prepare the log access
    - retrieve information
    - transform and clean data
    - insert them in the database
    -- due the amount of data, there is a tracker letting users 
    know about progress
    """
    
    # open log file
    log_files = get_files(find_path)

    log_df = pd.concat(
                        map(
                            read_json
                            , log_files
                        )
                    )

    log_df['time'] = pd.to_datetime(log_df['ts'], unit='ms', utc=True).astype(str)
    log_df['hour'] = pd.to_datetime(log_df['ts'], unit='ms', utc=True).dt.hour.astype(str)
    log_df['day'] = pd.to_datetime(log_df['ts'], unit='ms', utc=True).dt.day.astype(str)

    ## warning of deprecation
    # log_df['week'] = pd.to_datetime(log_df['ts'], unit='ms', utc=True).dt.week.astype(str)

    log_df['week'] = pd.to_datetime(log_df['ts'], unit='ms', utc=True).dt.isocalendar().week.astype(str)
    log_df['month'] = pd.to_datetime(log_df['ts'], unit='ms', utc=True).dt.month.astype(str)
    log_df['year'] = pd.to_datetime(log_df['ts'], unit='ms', utc=True).dt.year.astype(str)
    log_df['weekday'] = pd.to_datetime(log_df['ts'], unit='ms', utc=True).dt.weekday.astype(str)



    # filter by NextSong action
    time_df = log_df[log_df["page"] == "NextSong"].copy()

    times_desired_columns = [
        "time", "hour", "day", "week", "month", "year", "weekday"
    ]


    time_rename_map = {
        "time"               : "start_time"
    }

    times_df = df_cleanse(
        df=time_df
        , filter_cols=times_desired_columns
        # , filter_out_dict= {"userId":""}
        # , astype= {"userId": int}
        , rename_map= time_rename_map
        # , duplicates_considering=["start_time"]
    )

    
    # insert time data records
    inject_data(
        times_df
        , cur
        , conn
        , injection_rule= inject_template(
                                            times_df
                                            , "time"
                                            , conflict_rule="(start_time)"
                                            , conflict_action="""NOTHING"""
                                        )
    )
    ## Users

    users_desired_columns= [
        "userId"
        , "firstName"
        , "lastName"
        , "gender"
        , "level"
        
    ]

    users_rename_map = {
        "userId" : "user_id"
        , "firstName"        : "first_name"
        , "lastName"         : "last_name"
    }

    # load user table
    users_df = df_cleanse(
        df=log_df
        , filter_cols=users_desired_columns
        , filter_out_dict= {"userId":""}
        , astype= {"userId": int}
        , rename_map= users_rename_map
        , duplicates_considering=["user_id"]
    )   

    # insert user records
    inject_data(
        users_df
        , cur
        , conn
        , injection_rule= inject_template(
                                            users_df
                                            , "users"
                                            , conflict_rule="(user_id)"
                                            , conflict_action=""" UPDATE SET level = EXCLUDED.level"""
                                        )
    )

    songplays_desired_columns = [
        "time",
        "userId",
        "level",
        # songid,
        # artistid,
        "sessionId",
        "location",
        "userAgent",
        "song",
        "artist",
        "length"
    ]

    songplays_df = df_cleanse(
        df=log_df
        , filter_cols=songplays_desired_columns
        # , filter_out_dict= {"userId":""}
        # , astype= {"userId": int}
        , rename_map= {}
        # , duplicates_considering=["start_time"]
    ).reset_index(drop=True)


    # insert songplay records

    for index, row in songplays_df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        if songid != artistid:
            print(songid, artistid)

        # insert songplay record
        songplay_data = (
            #check what to use here, maybe index? using now auto_incremental approach. a.k.a. serial
            row.time,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent
        )
        
        if row.userId == '':
            pass
        else:
            cur.execute(songplay_table_insert, songplay_data)
            conn.commit()

        if index % show_status_at == 0:
            print("index", songplay_data)

def main():
    """
    Pipeline to trigger all the etl
    - prepare songs
    - prepare logs
    """

    conn = psycopg2.connect(
            generate_uri(override_env={"DATABASE_NAME":"sparkifydb"})
        )
    cur = conn.cursor()

    process_song_file(conn, cur, find_path='data/song_data')
    
    process_log_file(conn, cur, find_path='data/log_data')
    
    conn.close()


if __name__ == "__main__":
    main()