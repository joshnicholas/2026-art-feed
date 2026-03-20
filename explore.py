import os
import csv
import json
import logging
import requests
from tqdm import tqdm
from wikiartcrawler import WikiartAPI, VALID_ARTIST_GROUPS
from wikiartcrawler.artist_group import load_artists
from wikiartcrawler.wikiart_api import get_session_key

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

SESSION_CACHE = '.session_key'


def load_session_key(credentials_file: str):
    """Reuse a cached session key, or create a new one and cache it."""
    if os.path.exists(SESSION_CACHE):
        with open(SESSION_CACHE) as f:
            key = f.read().strip()
        if key:
            logging.info(f'Reusing cached session key')
            return key

    with open(credentials_file) as f:
        creds = json.loads(f.read().strip().splitlines()[0])
    key = get_session_key(creds['access_code'], creds['secret_code'])
    if key:
        with open(SESSION_CACHE, 'w') as f:
            f.write(key)
        logging.info(f'New session key cached')
    return key



def download_collection(name: str, credentials_file: str = None):
    """Download all images for an artist or art group.

    Args:
        name: Artist URL slug (e.g. 'claude-monet') or group name (e.g. 'impressionism').
    """
    is_group = name in VALID_ARTIST_GROUPS
    stem = name.replace(' ', '-').lower()
    img_dir = os.path.join('input', stem)
    csv_path = os.path.join('input', f'{stem}.csv')
    os.makedirs(img_dir, exist_ok=True)

    has_credentials = credentials_file is not None and os.path.exists(credentials_file)
    if has_credentials:
        session_key = load_session_key(credentials_file)
        api = WikiartAPI(session_key=[session_key], skip_download=False)
    else:
        api = WikiartAPI(skip_download=True)

    if is_group:
        artists = load_artists(name)
        if not has_credentials:
            from wikiartcrawler.artist_group import available_artist
            cached = set(available_artist())
            artists = [a for a in artists if a in cached]
        artists = [a for a in artists if a in api.dict_artist]
    else:
        artists = [name]

    fieldnames = [
        'file', 'artist', 'artist_name',
        'title', 'year', 'wikiart_url', 'painting_url', 'image_url', 'width', 'height',
        'styles', 'media', 'genres', 'tags', 'location', 'galleries', 'period', 'description',
    ]
    csv_exists = os.path.exists(csv_path)
    csv_file = open(csv_path, 'a', newline='', encoding='utf-8')
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    if not csv_exists:
        writer.writeheader()

    def join_list(val):
        if isinstance(val, list):
            return '|'.join(str(v) for v in val)
        return val or ''

    total_rows = 0
    try:
        for artist_url in artists:
            painting_info = api.get_painting_info(artist_url)
            if not painting_info:
                logging.warning(f'No paintings found for {artist_url}')
                continue

            logging.info(f'Downloading {len(painting_info)} paintings for {artist_url}')
            for p in tqdm(painting_info, desc=artist_url):
                image_url = p.get('image', '')
                if not image_url or 'FRAME-600x480' in image_url:
                    continue

                ext = image_url.split('.')[-1].split('?')[0]
                filename = f"{artist_url}__{p.get('url', p.get('id', 'unknown'))}.{ext}"
                dest = os.path.join(img_dir, filename)

                if not os.path.exists(dest):
                    try:
                        img_data = requests.get(image_url, timeout=30).content
                        with open(dest, 'wb') as f:
                            f.write(img_data)
                    except Exception as e:
                        logging.warning(f'Failed to download {image_url}: {e}')
                        continue

                detail = p.get('detail') or {}
                painting_slug = p.get('url', '')
                writer.writerow({
                    'file': os.path.join(stem, filename),
                    'artist': artist_url,
                    'artist_name': p.get('artistName', ''),
                    'title': p.get('title', ''),
                    'year': p.get('completitionYear', ''),
                    'wikiart_url': f'https://www.wikiart.org/en/{artist_url}/{painting_slug}' if painting_slug else '',
                    'painting_url': painting_slug,
                    'image_url': image_url,
                    'width': p.get('width', ''),
                    'height': p.get('height', ''),
                    'styles': join_list(detail.get('styles')),
                    'media': join_list(detail.get('media')),
                    'genres': join_list(detail.get('genres')),
                    'tags': join_list(detail.get('tags')),
                    'location': detail.get('location', ''),
                    'galleries': join_list(detail.get('galleries')),
                    'period': detail.get('period', '') or '',
                    'description': detail.get('description', ''),
                })
                csv_file.flush()
                total_rows += 1
    finally:
        csv_file.close()

    if total_rows == 0:
        logging.warning('No images downloaded.')
        return

    logging.info(f'Saved {total_rows} entries to {csv_path}')
    logging.info(f'Images saved to {img_dir}/')


def resolve_name(query: str, artist_keys: list) -> str:
    """Convert a human-readable name to the WikiArt slug format.

    Tries an exact slug match first, then falls back to fuzzy matching
    against known artist slugs and group names.
    """
    slug = query.strip().lower().replace(' ', '-')

    if slug in VALID_ARTIST_GROUPS:
        return slug
    if slug in artist_keys:
        return slug

    # fuzzy: find the closest match by counting matching slug parts
    all_options = VALID_ARTIST_GROUPS + artist_keys
    query_parts = set(slug.split('-'))
    scored = [(sum(p in candidate for p in query_parts), candidate) for candidate in all_options]
    scored.sort(key=lambda x: -x[0])
    best_score, best_match = scored[0]

    if best_score == 0:
        raise ValueError(f'No match found for "{query}". Available groups: {VALID_ARTIST_GROUPS}')

    return best_match


# _api = WikiartAPI(skip_download=True)
# _resolved = resolve_name("Vincent Van Gogh", list(_api.dict_artist.keys()))
# download_collection(_resolved, credentials_file='credentials.json')

def wrapper(nammo):

    _api = WikiartAPI(skip_download=True)
    _resolved = resolve_name(nammo, list(_api.dict_artist.keys()))
    download_collection(_resolved, credentials_file='credentials.json')


wrapper('Paul Cezanne')