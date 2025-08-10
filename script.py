import os
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime

# ==== CONFIG ====
START_DATE = datetime(1996, 2, 7)
PLAYLIST_ID = os.getenv("SPOTIFY_PLAYLIST_ID")
DATA_FILE = "added_tracks.json"

# Spotify API credentials from GitHub Secrets
SPOTIPY_CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
SPOTIPY_CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
SPOTIPY_REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")
SPOTIFY_REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

# ==== AUTH FUNCTION ====
def get_spotify_client():
    auth_manager = SpotifyOAuth(
        client_id=SPOTIPY_CLIENT_ID,
        client_secret=SPOTIPY_CLIENT_SECRET,
        redirect_uri=SPOTIPY_REDIRECT_URI,
        scope="playlist-modify-public",
        cache_path=None
    )
    # Force refresh using stored refresh token
    token_info = auth_manager.refresh_access_token(SPOTIFY_REFRESH_TOKEN)
    return spotipy.Spotify(auth=token_info["access_token"])

sp = get_spotify_client()

# ==== LOAD ALREADY ADDED TRACKS ====
if os.path.exists(DATA_FILE):
    with open(DATA_FILE, "r") as f:
        added_tracks = json.load(f)
else:
    added_tracks = []

# ==== SCRAPE ALL UK NUMBER 1s SINCE 1996 ====
def get_all_number_ones(start_year=1996):
    all_songs = []
    current_year = datetime.now().year
    for year in range(start_year, current_year + 1):
        url = f"https://en.wikipedia.org/wiki/List_of_UK_Singles_Chart_number_ones_of_{year}"
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser")
        tables = soup.find_all("table", {"class": "wikitable"})
        for table in tables:
            df = pd.read_html(str(table))[0]
            for _, row in df.iterrows():
                try:
                    date_str = str(row[0])
                    song = str(row[1])
                    artist = str(row[2])
                    date_obj = datetime.strptime(
                        date_str.split("–")[0].strip(), "%d %B %Y"
                    )
                    if date_obj >= START_DATE:
                        all_songs.append(
                            {"date": date_obj, "song": song, "artist": artist}
                        )
                except Exception:
                    continue
    return sorted(all_songs, key=lambda x: x["date"])

# ==== SCRAPE LATEST UK NUMBER 1 ====
def get_latest_number_one():
    year = datetime.now().year
    url = f"https://en.wikipedia.org/wiki/List_of_UK_Singles_Chart_number_ones_of_{year}"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    tables = soup.find_all("table", {"class": "wikitable"})
    latest_song = None
    latest_date = None

    for table in tables:
        df = pd.read_html(str(table))[0]
        for _, row in df.iterrows():
            try:
                date_str = str(row[0])
                song = str(row[1])
                artist = str(row[2])
                date_obj = datetime.strptime(
                    date_str.split("–")[0].strip(), "%d %B %Y"
                )
                if latest_date is None or date_obj > latest_date:
                    latest_date = date_obj
                    latest_song = {"date": date_obj, "song": song, "artist": artist}
            except Exception:
                continue
    return latest_song

# ==== ADD TO SPOTIFY ====
def add_song_to_playlist(song, artist):
    query = f"track:{song} artist:{artist}"
    results = sp.search(q=query, type="track", limit=1)
    if results["tracks"]["items"]:
        track_id = results["tracks"]["items"][0]["id"]
        if track_id not in added_tracks:
            sp.playlist_add_items(PLAYLIST_ID, [track_id])
            added_tracks.append(track_id)
            print(f"✅ Added: {song} - {artist}")
        else:
            print(f"⏩ Already added: {song} - {artist}")
    else:
        print(f"❌ Not found: {song} - {artist}")

# ==== MAIN ====
if __name__ == "__main__":
    if not added_tracks:
        print("📀 First run detected — backfilling all Number 1s since 1996...")
        songs = get_all_number_ones(1996)
        for s in songs:
            add_song_to_playlist(s["song"], s["artist"])
    else:
        print("🔍 Checking latest Number 1...")
        latest = get_latest_number_one()
        if latest and latest["date"] >= START_DATE:
            add_song_to_playlist(latest["song"], latest["artist"])

    with open(DATA_FILE, "w") as f:
        json.dump(added_tracks, f)
