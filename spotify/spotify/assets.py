import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
import spotipy.util as util
import os
import json
from dagster import FreshnessPolicy, asset
import pandas as pd
from spotify_utils import *
from db_utils import *

sp = get_spotify_token()
conn = get_connection()

### TOP 50 LISTS (SHORT, MEDIUM, LONG TERM)
@asset
def get_top_tracks() -> None:
    # spotify = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())
    token = util.prompt_for_user_token(scope='user-top-read')
    sp = spotipy.Spotify(auth=token)
    for time_range in ['short_term', 'medium_term', 'long_term']:
        results = sp.current_user_top_tracks(limit=50, offset=0, time_range=time_range)

        os.makedirs('data', exist_ok=True)
        # clear the json file
        open(f'data/top_tracks_{time_range}.json', 'w').close()
        with open(f'data/top_tracks_{time_range}.json', 'w') as f:
            json.dump(results, f)



@asset(
        deps=[get_top_tracks],
        freshness_policy=FreshnessPolicy(maximum_lag_minutes=60*6)
        )
def top_tracks_parsed() -> None:

    df_all = pd.DataFrame()

    for time_range in ['short_term', 'medium_term', 'long_term']:
        with open(f'data/top_tracks_{time_range}.json', 'r') as f:
            results = json.load(f)

            df = df = pd.DataFrame(columns=['track_id', 'rank'])

            for rank, result in enumerate(results['items']):
                track_id = result['id']

                df.loc[len(df)] = [track_id, rank+1]

            df['time_range'] = time_range

            df_all = pd.concat([df_all, df])

    df_all.to_csv(f'data/top_tracks_parsed.csv', index=False)


'''
Add an asset that will write the results from the top_tracks_parsed.csv file to the postgres database
Is part of an SCD so stage it first so update statement can run properly
- Destination: source.fact_top_50_stage
'''

### RECENTLY PLAYED
@asset
def get_recently_played() -> None:
    token = util.prompt_for_user_token(scope='user-read-recently-played')
    sp = spotipy.Spotify(auth=token)

    results = sp.current_user_recently_played(limit=50)

    os.makedirs('data', exist_ok=True)
    # clear the json file
    open('data/recently_played.json', 'w').close()
    with open('data/recently_played.json', 'w') as f:
        json.dump(results, f)


@asset(deps=[get_recently_played])
def recently_played_parsed() -> None:

    with open('data/recently_played.json', 'r') as f:
        results = json.load(f)

    df = pd.DataFrame(columns=['track_id', 'played_at'])

    for result in results['items']:
        track_id = result['track']['id']
        played_at = pd.to_datetime(result['played_at'])

        df.loc[len(df)] = [track_id, played_at]

    df['played_at'] = df['played_at'].dt.tz_convert('America/Chicago') \
                                     .dt.tz_localize(None) \
                                     .dt.strftime('%Y-%m-%d %H:%M:%S')
    
    df.to_csv('data/recently_played_parsed.csv', index=False)


'''
Add an asset that will write the results from the recently_played_parsed.csv file to the postgres database
- Destination: source.fact_recently_played
'''


@asset(deps=[top_tracks_parsed, recently_played_parsed])
def track_details() -> None:

    sp = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())

    top_df = pd.read_csv('data/top_tracks_parsed.csv')
    recent_df = pd.read_csv('data/recently_played_parsed.csv')

    track_list = list(set(top_df['track_id'].tolist() + recent_df['track_id'].tolist()))
    
    track_df_all = pd.DataFrame()
    feature_df_all = pd.DataFrame()
    track_df = pd.DataFrame(columns=['track_id', 'artist_id', 'artist_ids', 'name', 'duration_ms', 'explicit', 'popularity', 'album_id'])
    feature_df = pd.DataFrame(columns=['track_id', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness',
                                       'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature'])


    chunk_size = 50

    for i in range(0, len(track_list), chunk_size):
        track_results = sp.tracks(track_list[i:i+chunk_size])
        for result in track_results['tracks']:
            track_id = result['id']
            artist_ids = [artist['id'] for artist in result['artists']]
            artist_id = result['artists'][0]['id']
            name = result['name']
            duration_ms = result['duration_ms']
            explicit = result['explicit']
            popularity = result['popularity']
            album_id = result['album']['id']
        
            track_df.loc[len(track_df)] = [track_id, artist_id, artist_ids, name, duration_ms, explicit, popularity, album_id]

        track_df_all = pd.concat([track_df_all, track_df])

        feature_results = sp.audio_features(track_list[i:i+chunk_size])
        for result in feature_results:
            track_id = result['id']
            danceability = result['danceability']
            energy = result['energy']
            key = result['key']
            loudness = result['loudness']
            mode = result['mode']
            speechiness = result['speechiness']
            acousticness = result['acousticness']
            instrumentalness = result['instrumentalness']
            liveness = result['liveness']
            valence = result['valence']
            tempo = result['tempo']
            time_signature = result['time_signature']

            feature_df.loc[len(feature_df)] = [track_id, danceability, energy, key, loudness, mode, speechiness, acousticness,
                                               instrumentalness, liveness, valence, tempo, time_signature]
            
        feature_df_all = pd.concat([feature_df_all, feature_df])

    track_df_all = track_df_all.set_index('track_id')
    feature_df_all = feature_df_all.set_index('track_id')

    track_details_df = track_df_all.join(feature_df_all)

    track_details_df = track_details_df[~track_details_df.index.duplicated(keep='first')]

    track_details_df.to_csv('data/track_details.csv')



@asset(deps=[track_details])
def artist_details() -> None:
    sp = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())

    df = pd.read_csv('data/track_details.csv')

    artist_list = df['artist_id'].dropna().unique().tolist()

    chunk_size = 50

    artist_df = pd.DataFrame(columns=['artist_id', 'name', 'genres', 'popularity'])
    artist_df_all = pd.DataFrame()

    for i in range(0, len(artist_list), chunk_size):
        artist_results = sp.artists(artist_list[i:i+chunk_size])
        for result in artist_results['artists']:
            artist_id = result['id']
            name = result['name']
            genres = result['genres']
            popularity = result['popularity']

            artist_df.loc[len(artist_df)] = [artist_id, name, genres, popularity]

        artist_df_all = pd.concat([artist_df_all, artist_df])

    artist_df_all.to_csv('data/artist_details.csv', index=False)


@asset(deps=[track_details])
def album_details() -> None:
    sp = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials())

    df = pd.read_csv('data/track_details.csv')

    album_list = df['album_id'].dropna().unique().tolist()

    chunk_size = 20

    album_df = pd.DataFrame(columns=['album_id', 'name', 'popularity', 'release_date', 'total_tracks', 'track_ids'])
    album_df_all = pd.DataFrame()

    for i in range(0, len(album_list), chunk_size):
        album_results = sp.albums(album_list[i:i+chunk_size])
        for result in album_results['albums']:
            album_id = result['id']
            name = result['name']
            popularity = result['popularity']
            release_date = pd.to_datetime(result['release_date'])
            total_tracks = result['total_tracks']
            track_ids = [track['id'] for track in result['tracks']['items']]

            album_df.loc[len(album_df)] = [album_id, name, popularity, release_date, total_tracks, track_ids]

        album_df_all = pd.concat([album_df_all, album_df])

    album_df_all.to_csv('data/album_details.csv', index=False)


@asset(deps=[track_details])
def write_tracks() -> None:
    return 1