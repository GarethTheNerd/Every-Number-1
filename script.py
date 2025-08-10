import os
import re
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime
from io import StringIO
import warnings

# ==== CLEAN LOGS ====
warnings.simplefilter(action="ignore", category=FutureWarning)

# ==== CONFIG ====
START_DATE = datetime(1996, 2, 7)
PLAYLIST_ID = os.getenv("SPOTIFY_PLAYLIST_ID")
DATA_FILE = "added_tracks.json"
CACHE_FILE = "track_cache.json"
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

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
        scope="playlist-modify-public playlist-read-private",
        cache_path=None
    )
    token_info = auth_manager.refresh_access_token(SPOTIFY_REFRESH_TOKEN)
    return spotipy.Spotify(auth=token_info["access_token"])

sp = get_spotify_client()

# ==== LOAD LOCAL DATA ====
if os.path.exists(DATA_FILE):
    with open(DATA_FILE, "r") as f:
        added_tracks = json.load(f)
else:
    added_tracks = []

if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r") as f:
        track_cache = json.load(f)
else:
    track_cache = {}

# ==== LOAD TRACKS ALREADY IN SPOTIFY PLAYLIST ====
def get_existing_playlist_tracks():
    print("📋 Fetching existing playlist tracks from Spotify...")
    existing_ids = []
    results = sp.playlist_items(PLAYLIST_ID, fields="items.track.id,total,next", additional_types=["track"])
    while results:
        for item in results["items"]:
            if item["track"] and item["track"]["id"]:
                existing_ids.append(item["track"]["id"])
        if results.get("next"):
            results = sp.next(results)
        else:
            break
    print(f"✅ Found {len(existing_ids)} existing tracks in playlist")
    return existing_ids

existing_playlist_tracks = set(get_existing_playlist_tracks())

# ==== NORMALISE HEADERS ====
def normalise_header(header):
    return re.sub(r"[^a-z]", "", header.lower())

# ==== FLEXIBLE TABLE PARSER ====
def parse_wiki_table(table, all_songs):
    df = pd.read_html(StringIO(str(table)))[0]
    df.columns = [normalise_header(str(c)) for c in df.columns]  # normalise headers

    # Find columns by flexible matching
    date_col = next((c for c in df.columns if "week" in c or "date" in c), None)
    song_col = next((c for c in df.columns if "single" in c or "song" in c), None)
    artist_col = next((c for c in df.columns if "artist" in c), None)

    if not date_col or not song_col or not artist_col:
        print(f"⚠️ Skipping table - required columns not found. Headers: {df.columns.tolist()}")
        return

    for _, row in df.iterrows():
        try:
            date_str = str(row[date_col])
            song = str(row[song_col])
            artist = str(row[artist_col])
            date_obj = datetime.strptime(
                date_str.split("–")[0].strip(), "%d %B %Y"
            )
            if date_obj >= START_DATE:
                all_songs.append(
                    {"date": date_obj, "song": song, "artist": artist}
                )
        except Exception:
            continue

# ==== SCRAPE ALL UK NUMBER 1s FROM DECADE PAGES ====
def get_all_number_ones_from_decades():
    decade_urls = [
        "https://en.wikipedia.org/wiki/List_of_UK_singles_chart_number_ones_of_the_1990s",
        "https://en.wikipedia.org/wiki/List_of_UK_singles_chart_number_ones_of_the_2000s",
        "https://en.wikipedia.org/wiki/List_of_UK_singles_chart_number_ones_of_the_2010s",
        "https://en.wikipedia.org/wiki/List_of_UK_singles_chart_number_ones_of_the_2020s"
    ]

    if DEBUG:
        # Only scrape the 2020s for quick testing
        decade_urls = [
            "https://en.wikipedia.org/wiki/List_of_UK_singles_chart_number_ones_of_the_2020s"
        ]

    all_songs = []
    for url in decade_urls:
        print(f"📅 Scraping decade page: {url}")
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser")
        tables = soup.find_all("table", {"class": "wikitable"})
        if not tables:
            print(f"⚠️ No tables found on {url}")
        for t_index, table in enumerate(tables, start=1):
            print(f"   📊 Parsing table {t_index} of {len(tables)}...")
            parse_wiki_table(table, all_songs)

    # Sort chronologically
    return sorted(all_songs, key=lambda x: x["date"])

# ==== SCRAPE LATEST UK NUMBER 1 (from 2020s page) ====
def get_latest_number_one():
    url = "https://en.wikipedia.org/wiki/List_of_UK_singles_chart_number_ones_of_the_2020s"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")
    tables = soup.find_all("table", {"class": "wikitable"})
    latest_song = None
    latest_date = None

    for table in tables:
        df = pd.read_html(StringIO(str(table)))[0]
        df.columns = [normalise_header(str(c)) for c in df.columns]
        date_col = next((c for c in df.columns if "week" in c or "date" in c), None)
        song_col = next((c for c in df.columns if "single" in c or "song" in c), None)
        artist_col = next((c for c in df.columns if "artist" in c), None)

        if not date_col or not song_col or not artist_col:
            continue

        for _, row in df.iterrows():
            try:
                date_str = str(row[date_col])
                song = str(row[song_col])
                artist = str(row[artist_col])
                date_obj = datetime.strptime(
                    date_str.split("–")[0].strip(), "%d %B %Y"
                )
                if latest_date is None or date_obj > latest_date:
                    latest_date = date_obj
                    latest_song = {"date": date_obj, "song": song, "artist": artist}
            except Exception:
                continue
    return latest_song

# ==== ADD TO SPOTIFY WITH CACHING ====
def add_song_to_playlist(song, artist):
    key = f"{song} - {artist}"
    track_id = track_cache.get(key)

    if not track_id:
        query = f"track:{song} artist:{artist}"
        results = sp.search(q=query, type="track", limit=1)
        if results["tracks"]["items"]:
            track_id = results["tracks"]["items"][0]["id"]
            track_cache[key] = track_id
        else:
            print(f"❌ Not found: {song} - {artist}")
            return

    if DEBUG:
        print(f"🔍 Found match: {song} - {artist} (ID: {track_id})")

    if track_id not in added_tracks and track_id not in existing_playlist_tracks:
        if DEBUG:
            print(f"📝 Would add: {song} - {artist}")
        else:
            sp.playlist_add_items(PLAYLIST_ID, [track_id])
            added_tracks.append(track_id)
            print(f"✅ Added: {song} - {artist}")
    else:
        print(f"⏩ Already in playlist: {song} - {artist}")

# ==== REORDER PLAYLIST CHRONOLOGICALLY USING CACHE ====
def reorder_playlist_chronologically():
    print("🔄 Reordering playlist chronologically...")
    all_songs_sorted = get_all_number_ones_from_decades()
    track_ids_sorted = []

    for s in all_songs_sorted:
        key = f"{s['song']} - {s['artist']}"
        track_id = track_cache.get(key)
        if not track_id:
            results = sp.search(q=f"track:{s['song']} artist:{s['artist']}", type="track", limit=1)
            if results["tracks"]["items"]:
                track_id = results["tracks"]["items"][0]["id"]
                track_cache[key] = track_id
        if track_id:
            track_ids_sorted.append(track_id)

    if DEBUG:
        print(f"📝 Would reorder playlist to {len(track_ids_sorted)} tracks in chronological order")
        return

    sp.playlist_replace_items(PLAYLIST_ID, [])
    for i in range(0, len(track_ids_sorted), 100):
        sp.playlist_add_items(PLAYLIST_ID, track_ids_sorted[i:i+100])
    print("✅ Playlist reordered chronologically")

# ==== MAIN ====
if __name__ == "__main__":
    if not added_tracks:
        print("📀 First run detected — backfilling all Number 1s since 1996...")
        songs = get_all_number_ones_from_decades()
        print(f"✅ Found {len(songs)} songs to process")
        for idx, s in enumerate(songs, start=1):
            print(f"[{idx}/{len(songs)}] Processing: {s['song']} - {s['artist']}")
            add_song_to_playlist(s["song"], s["artist"])
        reorder_playlist_chronologically()
    else:
        print("🔍 Checking latest Number 1...")
        latest = get_latest_number_one()
        if latest and latest["date"] >= START_DATE:
            print(f"Latest chart-topper: {latest['song']} - {latest['artist']}")
            add_song_to_playlist(latest["song"], latest["artist"])
        reorder_playlist_chronologically()

    # Save caches
    with open(DATA_FILE, "w") as f:
        json.dump(added_tracks, f)
    with open(CACHE_FILE, "w") as f:
        json.dump(track_cache, f)
