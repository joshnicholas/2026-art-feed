import os
import csv
import json
import logging
import time
import unicodedata
import wikiartcrawler.wikiart_api as _wikiart_module
from wikiartcrawler import WikiartAPI, VALID_ARTIST_GROUPS
from wikiartcrawler.artist_group import load_artists
from wikiartcrawler.wikiart_api import get_session_key, api_request

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

# Skip per-painting detail requests entirely to avoid hitting API rate limits.
_wikiart_module.get_painting_detail = lambda paint_id, session_key=None: {}

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
    """Fetch painting metadata for an artist or art group and save to CSV.

    Args:
        name: Artist URL slug (e.g. 'claude-monet') or group name (e.g. 'impressionism').
    """
    is_group = name in VALID_ARTIST_GROUPS
    stem = name.replace(' ', '-').lower()
    csv_path = os.path.join('input', f'{stem}.csv')
    os.makedirs('input', exist_ok=True)

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
        'artist', 'artist_name',
        'title', 'year', 'wikiart_url', 'painting_url', 'image_url', 'width', 'height',
    ]
    csv_exists = os.path.exists(csv_path)
    csv_file = open(csv_path, 'a', newline='', encoding='utf-8')
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    if not csv_exists:
        writer.writeheader()

    total_rows = 0
    try:
        for artist_url in artists:
            if artist_url not in api.dict_artist:
                results = api_request(
                    f'https://www.wikiart.org/en/api/2/PaintingSearch?term={artist_url}',
                    api.session_key
                )
                artist_id = next((r['artistId'] for r in (results or []) if r.get('artistUrl') == artist_url), None)
                if not artist_id:
                    logging.warning(f'Could not find artist ID for {artist_url}, skipping')
                    continue
                api.dict_artist[artist_url] = artist_id
            try:
                painting_info = api.get_painting_info(artist_url)
            except ValueError as e:
                if 'limit exceeded' in str(e).lower():
                    logging.warning(f'Rate limit hit on {artist_url}, stopping for this session')
                    return
                raise
            if not painting_info:
                logging.warning(f'No paintings found for {artist_url}')
                continue
            time.sleep(1)

            logging.info(f'Fetching metadata for {len(painting_info)} paintings from {artist_url}')
            for p in painting_info:
                image_url = p.get('image', '')
                if not image_url or 'FRAME-600x480' in image_url:
                    continue

                painting_slug = p.get('url', '')
                writer.writerow({
                    'artist': artist_url,
                    'artist_name': p.get('artistName', ''),
                    'title': p.get('title', ''),
                    'year': p.get('completitionYear', ''),
                    'wikiart_url': f'https://www.wikiart.org/en/{artist_url}/{painting_slug}' if painting_slug else '',
                    'painting_url': painting_slug,
                    'image_url': image_url,
                    'width': p.get('width', ''),
                    'height': p.get('height', ''),
                })
                csv_file.flush()
                total_rows += 1
    finally:
        csv_file.close()

    if total_rows == 0:
        logging.warning('No metadata rows written.')
        return

    logging.info(f'Saved {total_rows} entries to {csv_path}')


def _normalize(s: str) -> str:
    """Lowercase, strip accents, replace spaces with hyphens."""
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s.strip().lower().replace(' ', '-')


def resolve_name(query: str, artist_keys: list) -> str:
    """Convert a human-readable name to the WikiArt slug format.

    Tries an exact slug match first, then falls back to fuzzy matching
    against known artist slugs and group names.
    """
    slug = _normalize(query)

    if slug in VALID_ARTIST_GROUPS:
        return slug
    if slug in artist_keys:
        return slug

    # fuzzy: find candidates where ALL query parts match slug tokens
    all_options = VALID_ARTIST_GROUPS + artist_keys
    query_parts = set(slug.split('-'))
    scored = [(sum(p in set(_normalize(candidate).split('-')) for p in query_parts), candidate) for candidate in all_options]
    scored.sort(key=lambda x: -x[0])
    best_score, best_match = scored[0]

    if best_score < len(query_parts):
        raise ValueError(f'No match found for "{query}" (normalized: "{slug}"). Pass the WikiArt slug directly.')

    return best_match


# _api = WikiartAPI(skip_download=True)
# _resolved = resolve_name("Vincent Van Gogh", list(_api.dict_artist.keys()))
# download_collection(_resolved, credentials_file='credentials.json')

def wrapper(nammo, credentials_file: str = 'credentials.json'):
    session_key = load_session_key(credentials_file)
    _api = WikiartAPI(session_key=[session_key], skip_download=True)
    try:
        _resolved = resolve_name(nammo, list(_api.dict_artist.keys()))
    except ValueError:
        _resolved = _normalize(nammo)
        logging.info(f'"{nammo}" not in artist dict, using slug "{_resolved}" directly')
    download_collection(_resolved, credentials_file=credentials_file)


# wrapper('Paul Cezanne')

# wrapper('Camille Pissarro')

# wrapper('Georges Seurat')


### Haven't scraped:
# wrapper('Léo Gausson')

# wrapper('Ferdinand Hodler')

# wrapper('Maurice Prendergast')

# wrapper('Pierre Bonnard')

# wrapper('leo-gausson')

# wrapper('Walter Sickert')

# wrapper('Post impressionism')

# wrapper('Les Fauves')

# wrapper('Henri Matisse')


# new = ['Maxime Maufra',
#        'Charles Reiffel',
#        'Charles Cottet',
#        'Paul Ranson',
#        'Louis Hayet',
#        'Henri de Toulouse-Lautrec',
#        'Suzanne Valadon',
#        'Gustave Loiseau',
#         'Józef Pankiewicz'       
#        'Roger Fry',
#        'Jules-Alexandre Grun',
#         'Georges Lacombe',
#         'Edouard Vuillard',
# 'Janos Tornyai',
#     'Louis Valtat',

#     'Samuel Peploe',
# 'John Duncan Fergusson',
# 'Andre Derain',
# 'Georges Braque',
# 'Henri Manguin',
# 'Alice Bailly',
# 'Oleksa Novakivskyi',
# 'Gheorghe Petrascu',
# 'Nadezda Petrovic',

#        ]

# for artist in new:
#     wrapper(artist)

# wrapper('Albert Namatjira')

# wrapper('Franklin Carmichael')



# wrapper('Edouard Manet')

# wrapper('Alfred Sisley')

# wrapper('Claude Monet')

# wrapper('Pierre-Auguste Renoir')

# wrapper('Nicolae Vermont')

# wrapper('Józef Pankiewicz')

# wrapper('Lajos Tihanyi')

# wrapper('Stefan Dimitrescu')

# wrapper('Hendrick Avercamp')

# thingos = ['J.M.W. Turner', 'Johan Jongkind', 'Childe Hassam']


# strayans = ['Dorrit Black','Arthur Streeton', 'Julian Ashton', 'Jane Sutherland',
#             'Frederick McCubbin', 'Tom Roberts', 'Clara Southern',
#             'David Davies','Charles Conder', 'John Brack', 'Hans Heysen',
#             'Grace Cossington Smith', 'Ethel Carrick']

# thingos = ['John Glover', 'J. E. H. MacDonald', 'Frederick Varley', 'A.Y. Jackson',
#             'Arthur Lismer', 'Frank Johnston', 'Franklin Carmichael']


thingos = ['Rembrandt', 'Edward Hopper', 'Katsushika Hokusai']


for thingo in thingos:
    wrapper(thingo)