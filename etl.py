import os
import glob
import psycopg2
import pandas as pd
from table_creation.create_tables import generate_uri
from table_creation.sql_queries import song_select, songplay_table_insert

from sqlalchemy import create_engine

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


def process_song_file(engine, find_path):
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
    song_data\
        .to_sql('songs', con=engine, if_exists='append',index=False)
    
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
    artist_data\
        .to_sql('artists', con=engine, if_exists='append',index=False)


def process_log_file(conn, cur, engine, find_path, show_status_at=250):
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

    log_df['time'] = pd.to_datetime(log_df['ts'], unit='ms', utc=True)
    log_df['hour'] = pd.to_datetime(log_df['ts'], unit='ms', utc=True).dt.hour
    log_df['day'] = pd.to_datetime(log_df['ts'], unit='ms', utc=True).dt.day

    ## warning of deprecation
    # log_df['week'] = pd.to_datetime(log_df['ts'], unit='ms', utc=True).dt.week

    log_df['week'] = pd.to_datetime(log_df['ts'], unit='ms', utc=True).dt.isocalendar().week
    log_df['month'] = pd.to_datetime(log_df['ts'], unit='ms', utc=True).dt.month
    log_df['year'] = pd.to_datetime(log_df['ts'], unit='ms', utc=True).dt.year
    log_df['weekday'] = pd.to_datetime(log_df['ts'], unit='ms', utc=True).dt.weekday


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
    times_df\
        .to_sql('time', con=engine, if_exists='append',index=False)

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
    users_df\
        .to_sql('users', con=engine, if_exists='append',index=False)

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
    engine = create_engine(generate_uri(template="postgres_url", override_env={"DATABASE_NAME":"sparkifydb"})) 

    process_song_file(engine, find_path='data/song_data')
    
    process_log_file(conn, cur, engine, find_path='data/log_data')
    
    conn.close()


if __name__ == "__main__":
    main()