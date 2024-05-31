
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
import spotipy.util as util
import pandas as pd
from datetime import datetime

def get_spotify_token():
    scopes = ['user-top-read', 'user-read-recently-played']

    sp_oauth = SpotifyOAuth(scope=scopes)
    token_info = sp_oauth.validate_token()
    try:
        token = token_info['access_token']
    except:
        # if not token_info:
        auth_url = sp_oauth.get_authorize_url()

        print(auth_url)

        response = input('Paste the above link into your browser, then paste the redirect url here: ')

        code = sp_oauth.parse_response_code(response)
        token_info = sp_oauth.get_access_token(code)

        token = token_info['access_token']

    sp = spotipy.Spotify(auth=token)

    return sp

def get_top_tracks(sp, n=50, offset=0, time_range='long_term'):
    '''
    Pull the user top N most played tracks
    '''

    results = sp.current_user_top_tracks(limit=n, offset=offset, time_range=time_range)

    return parse_top_tracks(results, time_range)

def parse_top_tracks(results, time_range):
    '''
    parse the track object from api response and return a dataframe of the results
    - results are returned in rank order (confirmed on https://www.statsforspotify.com/track/top?timeRange=short_term)
    '''

    df = pd.DataFrame(columns=['track_id', 'rank', 'is_current'])

    for rank, result in enumerate(results['items']):
        track_id = result['id']

        df.loc[len(df)] = [track_id, rank+1, True]

    df['time_range'] = time_range

    return df

def get_recently_played(sp, n=50):
    '''
    get the recently played tracks
    '''

    return parse_recently_played(sp.current_user_recently_played(limit=n))

def parse_recently_played(results):
    '''
    parse the recently played tracks to get info and play time
    '''

    df = pd.DataFrame(columns=['track_id', 'played_at'])

    for result in results['items']:
        track_id = result['track']['id']
        played_at = pd.to_datetime(result['played_at'])

        df.loc[len(df)] = [track_id, played_at]

    df['played_at'] = df['played_at'].dt.tz_convert('America/Chicago') \
                                     .dt.tz_localize(None) \
                                     .dt.strftime('%Y-%m-%d %H:%M:%S')

    return df

def get_audio_features(sp, tracks, chunk_size=100):
    '''
    pull the audio features for a track - tracks can be a single track (str) or a list of tracks

    max limit = 100
    '''

    df = pd.DataFrame()

    for i in range(0, len(tracks), chunk_size):
        tmp = parse_audio_features(sp.audio_features(tracks[i:i+chunk_size]))
        df = pd.concat([df, tmp])

    # af = sp.audio_features(tracks)

    return df

def parse_audio_features(results):
    '''
    parse the individual audio features for the track
    '''

    df = pd.DataFrame(columns=['track_id', 'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness',
                               'instrumentalness', 'liveness', 'valence', 'tempo', 'time_signature'])

    for result in results:

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

        df.loc[len(df)] = [track_id, danceability, energy, key, loudness, mode, speechiness, acousticness,
                           instrumentalness, liveness, valence, tempo, time_signature]

    return df

def append_audio_features(sp, df):
    '''
    append track audio features to an existing dataframe of tracks
    '''

    if not df.index.name:
        df = df.set_index('track_id')
    af = get_audio_features(sp, df.index.tolist())
    af_df = af.set_index('track_id')

    return df.join(af_df)

def get_track_info(sp, df, chunk_size=50):
    '''
    get all the track information for the specified list of tracks (max 50)
    '''

    # add a check here to see if the track already exists in dim_track
    # no need to pull if it already exists (will they ever change?)

    tracks = df['track_id'].dropna().unique().tolist()

    df = pd.DataFrame()

    for i in range(0, len(tracks), chunk_size):
        tmp = parse_track_info(sp, sp.tracks(tracks[i:i+chunk_size]))
        df = pd.concat([df, tmp])

    return df

def parse_track_info(sp, results):
    '''
    parse all the data for each track and add the audio features
    '''

    df = pd.DataFrame(columns=['track_id', 'artist_id', 'artist_ids', 'name', 'duration_ms', 'explicit', 'popularity', 'album_id'])

    for result in results['tracks']:
        track_id = result['id']
        artist_ids = [artist['id'] for artist in result['artists']]
        artist_id = result['artists'][0]['id']
        name = result['name']
        duration_ms = result['duration_ms']
        explicit = result['explicit']
        popularity = result['popularity']
        album_id = result['album']['id']

        df.loc[len(df)] = [track_id, artist_id, artist_ids, name, duration_ms, explicit, popularity, album_id]

    df = append_audio_features(sp, df)

    return df

def get_artist_info(sp, df, chunk_size=50):
    '''
    get the unique artists from a dataframe and pull their info (max 50)
    '''

    # add check here to see if artist already exists in dim_artist
    artists = df['artist_id'].dropna().unique().tolist()

    df = pd.DataFrame()

    for i in range(0, len(artists), chunk_size):
        tmp = parse_artist_info(sp.artists(artists[i:i+chunk_size]))
        df = pd.concat([df, tmp])

    return df.set_index('artist_id')

def parse_artist_info(results):
    '''
    parse the results of the artists info pull
    '''

    df = pd.DataFrame(columns=['artist_id', 'name', 'genres', 'popularity'])

    for result in results['artists']:
        artist_id = result['id']
        name = result['name']
        genres = result['genres']
        popularity = result['popularity']

        df.loc[len(df)] = [artist_id, name, genres, popularity]

    return df

def get_album_info(sp, df, chunk_size=20):
    '''
    get the unique albums from a dataframe and pull their info

    maximum size of albums list is 20
    '''

    albums = df['album_id'].dropna().unique().tolist()

    df = pd.DataFrame()

    for i in range(0, len(albums), chunk_size):
        tmp = parse_album_info(sp.albums(albums[i:i+chunk_size]))
        df = pd.concat([df, tmp])


    return df.set_index('album_id')

def parse_album_info(results):
    '''
    parse the results of the albums info pull
    '''

    df = pd.DataFrame(columns=['album_id', 'name', 'popularity', 'release_date', 'total_tracks', 'track_ids'])

    for result in results['albums']:
        album_id = result['id']
        name = result['name']
        popularity = result['popularity']
        release_date = pd.to_datetime(result['release_date'])
        total_tracks = result['total_tracks']
        track_ids = [track['id'] for track in result['tracks']['items']]

        df.loc[len(df)] = [album_id, name, popularity, release_date, total_tracks, track_ids]

    return df
