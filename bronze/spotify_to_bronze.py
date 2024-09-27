# Databricks notebook source
# MAGIC %pip install spotipy async_timeout

# COMMAND ----------

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
from pyspark.sql import functions as F

# COMMAND ----------

def read_secrets(file_path):
    secrets = {}
    with open(file_path, 'r') as file:
        for line in file:
            if '=' in line:
                key, value = line.strip().split('=', 1)
                secrets[key] = value
    return secrets

# Usage
secrets = read_secrets('secrets.txt')
client_id = secrets.get('client_id')
client_secret = secrets.get('client_secret')

# COMMAND ----------


auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(auth_manager=auth_manager)

country_codes = [
    'US', 'GB', 'DE', 'FR', 'BR', 'CA', 'AU', 'MX', 'IT', 'ES', 'NL', 'SE', 'NO', 'DK', 'FI', 'PL', 'PT', 'AR', 'CL', 'CO', 'PE', 'UG', 'UY'
]

def get_playlist_id_by_country(sp, country_code):
    query = f'Top 50 - {country_code}'
    results = sp.search(q=query, type='playlist', limit=1)
    playlists = results['playlists']['items']
    
    if playlists:
        return playlists[0]['id']
    else:
        return None

def get_tracks_from_playlist(sp, playlist_id, limit=50):
    results = sp.playlist_tracks(playlist_id, limit=limit)
    tracks = results['items']
    
    top_tracks = []
    for idx, item in enumerate(tracks):
        track = item['track']
        track_info = {
            'position': idx + 1, 
            'name': track['name'],
            'artist': ', '.join([artist['name'] for artist in track['artists']]),
            'album': track['album']['name'],
            'release_date': track['album']['release_date'],
            'spotify_url': track['external_urls']['spotify']
        }
        top_tracks.append(track_info)
    
    return top_tracks

for country_code in country_codes:
    print(f"\nFetching Top 50 Tracks for {country_code}...\n")
    playlist_id = get_playlist_id_by_country(sp, country_code)
    
    if playlist_id:
        top_tracks = get_tracks_from_playlist(sp, playlist_id)
        
        df = pd.DataFrame(top_tracks)
        
        spark_df = spark.createDataFrame(df)
        spark_df = spark_df.withColumn("day", F.current_date())
        spark_df.write.format("delta").mode("overwrite").option("overwriteSchema", "True").saveAsTable(f'treinamentodatabricks.bronze.spotify_top_50_{country_code.lower()}')
        
        print(f"Saved Top 50 Tracks for {country_code} to Delta table treinamentodatabricks.bronze.spotify_top_50_{country_code.lower()}")
    else:
        print(f"Could not find Top 50 playlist for {country_code}")

print("Completed saving all Top 50 tracks to Delta tables.")

