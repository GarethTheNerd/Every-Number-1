import os
import re
import json
import time
import sys
import requests
import pandas as pd
from bs4 import BeautifulSoup
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime
from io import StringIO
import warnings
from typing import Optional

# Optional: load .env for local dev if python-dotenv is installed
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# ==== CLEAN LOGS ====
warnings.simplefilter(action="ignore", category=FutureWarning)

# ==== CONFIG ====
START_DATE = datetime(1996, 2, 7)
PLAYLIST_ID = os.getenv("SPOTIFY_PLAYLIST_ID")
DATA_FILE = "added_tracks.json"
CACHE_FILE = "track_cache.json"
NOT_FOUND_FILE = "not_found.json"
DEBUG = os.getenv("DEBUG", "false").lower() == "true"
ACTION = os.getenv("ACTION", "").lower()  # "rebuild"/"reorder" or "clear"

# Spotify API credentials from GitHub Secrets
SPOTIPY_CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
SPOTIPY_CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")
SPOTIPY_REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")
SPOTIFY_REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

# ==== ENV VALIDATION & NORMALIZATION ====
def extract_playlist_id(value: str | None) -> str | None:
    """Accept raw ID, spotify:playlist:ID, or https URL and return bare ID.

    Returns None if it can't confidently extract an ID.
    """
    if not value:
        return None
    v = value.strip()
    # Match spotify URI or URL
    m = re.search(r"playlist[:/](?P<id>[a-zA-Z0-9]+)", v)
    if m:
        return m.group("id")
    # If already looks like an ID (22+ base62 chars), accept
    if re.fullmatch(r"[A-Za-z0-9]{22,}", v):
        return v
    return None

def validate_env() -> None:
    missing = []
    for name, val in [
        ("SPOTIPY_CLIENT_ID", SPOTIPY_CLIENT_ID),
        ("SPOTIPY_CLIENT_SECRET", SPOTIPY_CLIENT_SECRET),
        ("SPOTIPY_REDIRECT_URI", SPOTIPY_REDIRECT_URI),
        ("SPOTIFY_REFRESH_TOKEN", SPOTIFY_REFRESH_TOKEN),
    ]:
        if not val:
            missing.append(name)
    normalized_pid = extract_playlist_id(PLAYLIST_ID)
    if not normalized_pid:
        missing.append("SPOTIFY_PLAYLIST_ID (must be a playlist ID/URI/URL)")

    if missing:
        print("❌ Missing or invalid environment variables:")
        for m in missing:
            print(f"   - {m}")
        print("\nTip: Set these as GitHub Actions Secrets and map them to env in your workflow, e.g.\n"
              "env:\n  SPOTIPY_CLIENT_ID: ${{ secrets.SPOTIPY_CLIENT_ID }}\n  SPOTIPY_CLIENT_SECRET: ${{ secrets.SPOTIPY_CLIENT_SECRET }}\n  SPOTIPY_REDIRECT_URI: ${{ secrets.SPOTIPY_REDIRECT_URI }}\n  SPOTIFY_REFRESH_TOKEN: ${{ secrets.SPOTIFY_REFRESH_TOKEN }}\n  SPOTIFY_PLAYLIST_ID: ${{ secrets.SPOTIFY_PLAYLIST_ID }}\n")
        sys.exit(1)
    # Overwrite with normalized playlist ID for all downstream calls
    globals()["PLAYLIST_ID"] = normalized_pid  # type: ignore

# Validate early before hitting the API
validate_env()

# ==== AUTH FUNCTION ====
def get_spotify_client():
    auth_manager = SpotifyOAuth(
        client_id=SPOTIPY_CLIENT_ID,
        client_secret=SPOTIPY_CLIENT_SECRET,
        redirect_uri=SPOTIPY_REDIRECT_URI,
        scope="playlist-modify-public playlist-modify-private playlist-read-private",
        cache_path=None
    )
    token_info = auth_manager.refresh_access_token(SPOTIFY_REFRESH_TOKEN)
    return spotipy.Spotify(auth=token_info["access_token"], requests_timeout=30)

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
    return set(existing_ids)

existing_playlist_tracks = get_existing_playlist_tracks()
print(f"📋 Found {len(existing_playlist_tracks)} existing tracks in playlist")

# ==== NORMALISE HEADERS ====
def normalise_header(header):
    return re.sub(r"[^a-z]", "", header.lower())

# ==== CLEAN SONG & ARTIST ====
def clean_song_title(song):
    song = song.strip('"')
    song = re.sub(r"\[.*?\]|\(.*?\)", "", song)
    return song.strip()

def base_song_key(song: str) -> str:
    """Return a canonical key for de-dup decisions.

    - Lowercase
    - Remove bracketed text (already done by clean_song_title)
    - Remove year suffixes like '98 / ’98
    - Strip common version tags (remaster, edit, mix, version)
    - Collapse whitespace
    """
    t = clean_song_title(song).lower()
    # Remove apostrophe-year like '98 or ’98
    t = re.sub(r"\s*['’]\d{2}\b", "", t)
    # Remove common suffix tags
    tags = [
        r"\b\d{4}\s*remaster(?:ed)?\b",
        r"\bremaster(?:ed)?\s*\d{4}\b",
        r"\bremaster(?:ed)?\b",
        r"\bradio\s+edit\b",
        r"\bsingle\s+version\b",
        r"\boriginal\s+mix\b",
        r"\bmono\b",
        r"\bstereo\b",
        r"\bedit\b",
        r"\bmix\b",
        r"\bversion\b",
    ]
    for pat in tags:
        t = re.sub(pat, "", t, flags=re.IGNORECASE)
    # Remove trailing hyphenated qualifiers like " - 2011 remaster"
    t = re.sub(r"\s*-\s*$", "", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def clean_artist_name(artist):
    artist = re.split(r"\s+(featuring|feat\.|ft\.|with|&)\s+", artist, flags=re.IGNORECASE)[0]
    return artist.strip()

# ==== DATE PARSING HELPERS ====
def _extract_year(text: str) -> Optional[int]:
    m = re.search(r"(19|20)\d{2}", text)
    return int(m.group(0)) if m else None

def _first_date_segment(s: str) -> str:
    # Keep left part before ranges (en dash, hyphen, or 'to')
    seg = re.split(r"\s*(–|-|to)\s*", s, maxsplit=1)[0]
    return seg.strip()

def parse_date_with_fallback(date_text: str, fallback_year: Optional[int]) -> Optional[datetime]:
    # Remove any bracketed notes and trim
    t = re.sub(r"\[.*?\]|\(.*?\)", "", str(date_text)).strip()
    # Normalize Unicode spaces (e.g., NBSP) to regular spaces
    t = t.replace("\u00a0", " ").replace("\u2009", " ")
    # Some rows might be NaN/None
    if not t or t.lower() == "nan":
        return None
    # If the cell is just a year, skip (table subheaders)
    if re.fullmatch(r"(19|20)\d{2}", t):
        return None
    # Try to extract year from the text; else use fallback
    yy = _extract_year(t) or fallback_year
    # Keep only the first date segment before ranges
    left = _first_date_segment(t)
    left = left.replace("\u00a0", " ").replace("\u2009", " ")
    # Build candidate strings to parse (avoid duplicating year)
    candidates = []
    candidates.append(left)
    candidates.append(t)
    if yy and (_extract_year(left) is None) and (_extract_year(t) is None):
        candidates.append(f"{left} {yy}")
    # Try multiple formats via strptime
    fmts = ["%d %B %Y", "%d %b %Y", "%d %B", "%d %b"]
    for cand in candidates:
        for fmt in fmts:
            try:
                dt = datetime.strptime(cand, fmt)
                # If format lacked year, inject fallback year
                if fmt in ("%d %B", "%d %b"):
                    if yy is None:
                        continue
                    dt = dt.replace(year=yy)
                return dt
            except Exception:
                continue
    # Fallback to pandas parser (dayfirst) if available
    try:
        s = pd.Series(candidates)
        ds = pd.to_datetime(s, errors="coerce", dayfirst=True, utc=False)
        ds = ds.dropna()
        if not ds.empty:
            return ds.iloc[0].to_pydatetime()
    except Exception:
        pass
    return None

def base_artist_key(artist: str) -> str:
    """Canonicalize artist string for de-dup decisions.

    - Lowercase
    - Remove featured/with parts (handled by clean_artist_name)
    - Normalize separators (comma/and/&/with) to a unified delimiter
    - Keep only the first one or two primary names to avoid minor credit differences
    """
    a = clean_artist_name(artist).lower()
    # Normalize separators to comma
    a = re.sub(r"\s*&\s*|\s+and\s+|\s+with\s+", ",", a)
    # Split on commas and collapse whitespace
    parts = [p.strip() for p in a.split(",") if p.strip()]
    # Keep first two principal names to avoid variations
    if not parts:
        return a
    key = " & ".join(parts[:2])
    return key

# ==== FLEXIBLE TABLE PARSER ====
def parse_wiki_table(table, all_songs):
    # Try to infer base year from table caption if present
    try:
        caption = getattr(table, "find", lambda *a, **k: None)("caption")
        base_year = _extract_year(caption.get_text()) if caption else None
    except Exception:
        base_year = None
    # Fallback: look at the nearest previous heading (e.g., h2/h3/h4) that usually contains the year
    if base_year is None:
        try:
            prev_hdr = table.find_previous(["h2", "h3", "h4"])  # type: ignore[attr-defined]
            if prev_hdr is not None:
                base_year = _extract_year(prev_hdr.get_text())
        except Exception:
            pass

    df = pd.read_html(StringIO(str(table)))[0]
    df.columns = [normalise_header(str(c)) for c in df.columns]

    date_col = next((c for c in df.columns if "week" in c or "date" in c), None)
    song_col = next((c for c in df.columns if "single" in c or "song" in c), None)
    artist_col = next((c for c in df.columns if "artist" in c), None)

    if not date_col or not song_col or not artist_col:
        if DEBUG:
            print(f"   ⏩ Skipping table, headers={list(df.columns)} (date_col={date_col}, song_col={song_col}, artist_col={artist_col})")
        return

    if DEBUG:
        print(f"   🔎 Table headers={list(df.columns)} -> date={date_col}, song={song_col}, artist={artist_col}")

    current_year = base_year
    added = 0
    for _, row in df.iterrows():
        try:
            date_str = str(row[date_col])
            original_song = str(row[song_col])
            song = clean_song_title(original_song)
            artist = clean_artist_name(str(row[artist_col]))
            # Parse date, using current/base year as needed
            inferred_year = _extract_year(date_str) or current_year
            date_obj = parse_date_with_fallback(date_str, inferred_year)
            if date_obj is None:
                if DEBUG:
                    print(f"      ⚠️ Could not parse date '{date_str}' (inferred_year={inferred_year})")
                continue
            # Update year tracker if we encountered an explicit year
            if _extract_year(date_str):
                current_year = date_obj.year
            if date_obj >= START_DATE:
                all_songs.append({
                    "date": date_obj,
                    "song": song,
                    "artist": artist,
                    "original_artist": str(row[artist_col]),
                    "original_song": original_song,
                })
                added += 1
        except Exception:
            continue
    if DEBUG:
        print(f"   ✅ Added {added} rows from this table")

# ==== SCRAPE ALL UK NUMBER 1s FROM DECADE PAGES ====
def get_all_number_ones_from_decades():
    decade_urls = [
        "https://en.wikipedia.org/wiki/List_of_UK_singles_chart_number_ones_of_the_1990s",
        "https://en.wikipedia.org/wiki/List_of_UK_singles_chart_number_ones_of_the_2000s",
        "https://en.wikipedia.org/wiki/List_of_UK_singles_chart_number_ones_of_the_2010s",
        "https://en.wikipedia.org/wiki/List_of_UK_singles_chart_number_ones_of_the_2020s"
    ]
    if DEBUG:
        decade_urls = ["https://en.wikipedia.org/wiki/List_of_UK_singles_chart_number_ones_of_the_2020s"]

    all_songs = []
    for url in decade_urls:
        if DEBUG:
            print(f"🌐 Fetching: {url}")
        r = requests.get(
            url,
            headers={"User-Agent": "Every-Number-1/1.0 (+https://github.com/GarethTheNerd/Every-Number-1)"},
            timeout=30,
        )
        if r.status_code != 200 or not r.text:
            print(f"⚠️ Failed to fetch {url}: HTTP {r.status_code}")
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        tables = soup.find_all("table", {"class": "wikitable"})
        if DEBUG:
            print(f"   🧾 Found {len(tables)} tables")
        for table in tables:
            parse_wiki_table(table, all_songs)

    return sorted(all_songs, key=lambda x: x["date"])

# ==== SCRAPE LATEST UK NUMBER 1 ====
def get_latest_number_one():
    url = "https://en.wikipedia.org/wiki/List_of_UK_singles_chart_number_ones_of_the_2020s"
    r = requests.get(url, headers={"User-Agent": "Every-Number-1/1.0 (+https://github.com/GarethTheNerd/Every-Number-1)"}, timeout=30)
    if r.status_code != 200 or not r.text:
        print(f"⚠️ Failed to fetch {url}: HTTP {r.status_code}")
        return None
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

        # Track year progression across rows
        base_year = None
        cap = getattr(table, "find", lambda *a, **k: None)("caption")
        try:
            base_year = _extract_year(cap.get_text()) if cap else None
        except Exception:
            base_year = None
        current_year = base_year
        for _, row in df.iterrows():
            try:
                date_str = str(row[date_col])
                original_song = str(row[song_col])
                song = clean_song_title(original_song)
                artist = clean_artist_name(str(row[artist_col]))
                inferred_year = _extract_year(date_str) or current_year
                date_obj = parse_date_with_fallback(date_str, inferred_year)
                if date_obj is None:
                    continue
                if _extract_year(date_str):
                    current_year = date_obj.year
                if latest_date is None or date_obj > latest_date:
                    latest_date = date_obj
                    latest_song = {
                        "date": date_obj,
                        "song": song,
                        "artist": artist,
                        "original_artist": str(row[artist_col]),
                        "original_song": original_song,
                    }
            except Exception:
                continue
    return latest_song

# ==== 4-STEP FUZZY SEARCH ====
def search_spotify_track(song, artist, original_artist, chart_date: datetime | None):
    clean_song = clean_song_title(song)
    clean_artist = clean_artist_name(artist)
    chart_year = chart_date.year if chart_date else None

    # Build queries with year constraints to improve precision
    queries: list[tuple[str, str]] = []
    if chart_year:
        yr = chart_year
        yr_next = yr + 1
        queries.extend([
            (f'track:"{clean_song}" artist:"{clean_artist}" year:{yr}', "exact+year"),
            (f'track:"{clean_song}" artist:"{original_artist}" year:{yr}', "orig-artist+year"),
            (f'track:"{clean_song}" year:{yr}', "song+year"),
            (f'{clean_song} {clean_artist} year:{yr}', "kw+year"),
            (f'track:"{clean_song}" artist:"{clean_artist}" year:{yr}-{yr_next}', "exact+year-range"),
            (f'track:"{clean_song}" artist:"{original_artist}" year:{yr}-{yr_next}', "orig-artist+year-range"),
        ])
    # Fallbacks without year
    queries.extend([
        (f'track:"{clean_song}" artist:"{clean_artist}"', "exact"),
        (f'track:"{clean_song}"', "song only"),
        (f'{clean_song} {clean_artist}', "keywords"),
        (f'track:"{clean_song}" artist:"{original_artist}"', "orig-artist")
    ])

    def base_artist_set(a: str) -> set[str]:
        prim = base_artist_key(a)
        return set([p.strip() for p in prim.split("&") if p.strip()])

    desired_artist_set = base_artist_set(artist)
    desired_orig_set = base_artist_set(original_artist)
    desired_song_key = base_song_key(song)

    best = None
    best_score = -1

    for query, desc in queries:
        try:
            results = sp.search(q=query, type="track", limit=5, market="GB")
        except Exception:
            results = None
        items = (
            results.get("tracks", {}).get("items", [])
            if isinstance(results, dict)
            else []
        )
        for tr in items:
            try:
                title = tr.get("name", "")
                song_key = base_song_key(title)
                artists = [a.get("name", "") for a in tr.get("artists", [])]
                artist_join = " & ".join(artists[:2]) if artists else ""
                artist_set = base_artist_set(artist_join)
                album = tr.get("album", {})
                rel_date = album.get("release_date") or ""
                rel_year = None
                m = re.match(r"^(\d{4})", rel_date)
                if m:
                    rel_year = int(m.group(1))

                score = 0
                if song_key == desired_song_key:
                    score += 5
                inter1 = len(artist_set & desired_artist_set)
                inter2 = len(artist_set & desired_orig_set)
                score += min(inter1, 2) * 2
                score += min(inter2, 1)  # small bonus if matches original credit
                if chart_year and rel_year:
                    if rel_year == chart_year:
                        score += 3
                    elif abs(rel_year - chart_year) == 1:
                        score += 1
                # prefer more popular tracks slightly
                pop = tr.get("popularity") or 0
                score += int(pop) // 30  # 0..3

                if score > best_score:
                    best_score = score
                    best = tr.get("id")
            except Exception:
                continue
        if best is not None and best_score >= 5:
            print(f"   ✅ Found with {desc} (score {best_score})")
            return best

    if best is not None:
        print(f"   ✅ Found with fallback (score {best_score})")
        return best
    return None

# ==== ADD TO SPOTIFY WITH CACHING + RETRY + DEDUP ====
added_song_artist_pairs = set()
not_found_pairs: set[tuple[str, str]] = set()  # (song_as_seen, artist_original)

def add_song_to_playlist(song, artist, original_artist, original_song, chart_date: datetime | None):
    global existing_playlist_tracks
    pair_key = f"{base_song_key(song)}|{base_artist_key(artist)}"
    if pair_key in added_song_artist_pairs:
        print(f"⏩ Duplicate song+artist detected, skipping: {song} - {artist}")
        return

    cache_key = f"{base_song_key(song)}|{base_artist_key(artist)}"
    track_id = track_cache.get(cache_key)

    if not track_id:
        track_id = search_spotify_track(song, artist, original_artist, chart_date)
        if track_id:
            track_cache[cache_key] = track_id
        else:
            # Record as seen in Wikipedia (raw song title, original artist string)
            not_found_pairs.add((original_song, original_artist))
            print(f"❌ Not found after fuzzy search: {song} - {artist}")
            return

    if DEBUG:
        print(f"🔍 Found match: {song} - {artist} (ID: {track_id})")

    if track_id not in added_tracks and track_id not in existing_playlist_tracks:
        if DEBUG:
            print(f"📝 Would add: {song} - {artist}")
        else:
            for attempt in range(3):
                try:
                    sp.playlist_add_items(PLAYLIST_ID, [track_id])
                    added_tracks.append(track_id)
                    existing_playlist_tracks.add(track_id)
                    added_song_artist_pairs.add(pair_key)
                    print(f"✅ Added: {song} - {artist}")
                    break
                except requests.exceptions.ReadTimeout:
                    print(f"⚠️ Timeout adding {song} - retrying ({attempt+1}/3)...")
                    time.sleep(2)
    else:
        print(f"⏩ Already in playlist: {song} - {artist}")

# ==== REORDER PLAYLIST CHRONOLOGICALLY & SYNC JSON ====
def reorder_playlist_chronologically():
    print("🔄 Reordering playlist chronologically...")
    all_songs_sorted = get_all_number_ones_from_decades()
    track_ids_sorted = []
    unique_pairs = set()
    seen_track_ids = set()

    for s in all_songs_sorted:
        pair_key = f"{base_song_key(s['song'])}|{base_artist_key(s['artist'])}"
        if pair_key in unique_pairs:
            continue
        unique_pairs.add(pair_key)
        cache_key = f"{base_song_key(s['song'])}|{base_artist_key(s['artist'])}"
        track_id = track_cache.get(cache_key)
        if not track_id:
            track_id = search_spotify_track(s['song'], s['artist'], s['original_artist'], s.get('date'))
            if track_id:
                track_cache[cache_key] = track_id
            else:
                # Record missing with the Wikipedia-provided values
                not_found_pairs.add((s['original_song'], s['original_artist']))
        if track_id:
            # Guard against duplicates at the Spotify track ID level too
            if track_id in seen_track_ids:
                if DEBUG:
                    print(f"⏩ Skipping duplicate track ID already queued: {track_id} ({s['song']} - {s['artist']})")
            else:
                seen_track_ids.add(track_id)
                track_ids_sorted.append(track_id)

    if DEBUG:
        print(f"📝 Would reorder playlist to {len(track_ids_sorted)} unique tracks in chronological order")
        return

    if not track_ids_sorted:
        print("⚠️ No track IDs resolved; skipping playlist replace to avoid clearing it.")
        return
    sp.playlist_replace_items(PLAYLIST_ID, [])
    for i in range(0, len(track_ids_sorted), 100):
        sp.playlist_add_items(PLAYLIST_ID, track_ids_sorted[i:i+100])

    with open(DATA_FILE, "w") as f:
        json.dump(track_ids_sorted, f)
    print("✅ Playlist reordered and JSON synced")

# ==== MAIN ====
if __name__ == "__main__":
    # Manual action: clear playlist and reset JSONs
    if ACTION == "clear":
        print("🧹 Clearing playlist and resetting JSON outputs...")
        if not DEBUG:
            sp.playlist_replace_items(PLAYLIST_ID, [])
        # Reset JSON files
        with open(DATA_FILE, "w") as f:
            json.dump([], f)
        with open(NOT_FOUND_FILE, "w") as f:
            json.dump([], f, indent=2)
        print("✅ Playlist cleared and outputs reset")
        sys.exit(0)

    # Manual action: rebuild/reorder playlist from source data
    if ACTION in ("rebuild", "reorder"):
        reorder_playlist_chronologically()
        with open(CACHE_FILE, "w") as f:
            json.dump(track_cache, f)
        # Write not_found summary (if any)
        summary = [
            {"song": song, "artist": artist}
            for (song, artist) in sorted(not_found_pairs, key=lambda x: (x[1].lower(), x[0].lower()))
        ]
        with open(NOT_FOUND_FILE, "w") as f:
            json.dump(summary, f, indent=2)
        sys.exit(0)

    if not added_tracks:
        print("📀 First run detected — backfilling all Number 1s since 1996...")
        songs = get_all_number_ones_from_decades()
        print(f"✅ Found {len(songs)} songs to process")
        for idx, s in enumerate(songs, start=1):
            print(f"[{idx}/{len(songs)}] Processing: {s['song']} - {s['artist']}")
            add_song_to_playlist(s["song"], s["artist"], s["original_artist"], s["original_song"], s.get("date"))
        reorder_playlist_chronologically()
    else:
        print("🔍 Checking latest Number 1...")
        latest = get_latest_number_one()
        if latest and latest["date"] >= START_DATE:
            print(f"Latest chart-topper: {latest['song']} - {latest['artist']}")
            add_song_to_playlist(latest["song"], latest["artist"], latest["original_artist"], latest["original_song"], latest.get("date"))
        reorder_playlist_chronologically()

    with open(CACHE_FILE, "w") as f:
        json.dump(track_cache, f)

    # Write not-found summary (unique, minimal fields)
    summary = [
        {"song": song, "artist": artist}
        for (song, artist) in sorted(not_found_pairs, key=lambda x: (x[1].lower(), x[0].lower()))
    ]
    with open(NOT_FOUND_FILE, "w") as f:
        json.dump(summary, f, indent=2)
