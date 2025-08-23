# Every-Number-1

Personal repo: a small script that adds every number since I was born to a Spotify playlist.

Others are free to use, copy, or modify the code if they find it useful. Credit is appreciated but not required.

Notes
- You’ll likely need Spotify developer credentials and a target playlist to run it.
- See `script.py` for the current entry point and logic.

### Outputs
- `added_tracks.json`: Ordered list of Spotify track IDs currently in the playlist.
- `not_found.json`: Minimal report of songs that couldn’t be matched on Spotify during the run, as parsed from Wikipedia (fields: `song`, `artist`). This file is updated and committed by the workflow after each run.

## License

0BSD — see `LICENSE` for details.
