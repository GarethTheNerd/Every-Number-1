import os
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
        scope="playlist-modify-public",
        cache_path=None
    )
    token_info = auth_manager.refresh_access_token(SPOTIFY_REFRESH_TOKEN)
    return spotipy.Spotify(auth=token_info["access_token"])

sp = get_spotify_client()

# ==== LOAD ALREADY ADDED TRACKS ====
if os.path.exists(DATA_FILE):
    with open(DATA_FILE, "r") as f:
        added_tracks = json.load(f)
else:
    added_tracks = []

# ==== SCRAPE ALL UK NUMBER 1s FROM DECADE PAGES ====
def get_all_number_ones_from_decades():
    decade_urls = [
        "https://en.wikipedia.org/wiki/List_of_UK_singles_chart_number_ones_of_the_1990s",
        "https://en.wikipedia.org/wiki/List_of_UK_singles_chart_number_ones_of_the_2000s",
        "https://en.wikipedia.org/wiki/List_of_UK_singles_chart_number_ones_of_the_2010s",
        "https://en.wikipedia.org/wiki/List_of_UK_singles_chart_number_ones_of_the_2020s"
