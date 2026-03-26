import csv
import random
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom.minidom import parseString

FEEDS_DIR = Path('feeds')
INPUT_DIR = Path('input')


def load_csv(csv_path):
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return [r for r in reader if r.get('image_url') and r.get('wikiart_url')]


def pick_items(sample_size: int = 5) -> list:
    """Pick items by randomly selecting a CSV, then a row from it, repeated sample_size times."""
    csv_files = list(INPUT_DIR.glob('*.csv'))
    if not csv_files:
        raise FileNotFoundError(f'No CSV files found in {INPUT_DIR}/')

    csv_data = {path: load_csv(path) for path in csv_files}
    csv_data = {path: rows for path, rows in csv_data.items() if rows}

    if not csv_data:
        raise ValueError('No valid rows found in any CSV.')

    sources = list(csv_data.values())
    picks = []
    seen_urls = set()
    attempts = 0
    max_attempts = sample_size * 20

    while len(picks) < sample_size and attempts < max_attempts:
        attempts += 1
        rows = random.choice(sources)
        row = random.choice(rows)
        url = row.get('wikiart_url')
        if url not in seen_urls:
            seen_urls.add(url)
            picks.append(row)

    return picks


def build_feed(
    name: str,
    title: str = 'Art Feed',
    link: str = 'https://www.wikiart.org',
    description: str = 'A daily selection of artworks from WikiArt',
    sample_size: int = 5,
):
    picks = pick_items(sample_size)
    now = format_datetime(datetime.now(timezone.utc))

    rss = Element('rss', {
        'version': '2.0',
        'xmlns:media': 'http://search.yahoo.com/mrss/',
        'xmlns:dc': 'http://purl.org/dc/elements/1.1/',
    })
    channel = SubElement(rss, 'channel')

    SubElement(channel, 'title').text = title
    SubElement(channel, 'link').text = link
    SubElement(channel, 'description').text = description
    SubElement(channel, 'lastBuildDate').text = now
    SubElement(channel, 'language').text = 'en'

    for row in picks:
        item = SubElement(channel, 'item')

        item_title = row.get('title', 'Untitled')
        artist = row.get('artist_name') or row.get('artist', '')
        year = row.get('year', '')
        item_title_str = f"{item_title} — {artist}" + (f" ({year})" if year else '')
        SubElement(item, 'title').text = item_title_str
        SubElement(item, 'link').text = row['wikiart_url']
        SubElement(item, 'guid', {'isPermaLink': 'true'}).text = row['wikiart_url']
        SubElement(item, 'pubDate').text = now

        def detail_row(label, value):
            return f'<p><strong>{label}:</strong> {value}</p>' if value else ''

        def fmt(val):
            return val.replace('|', ', ') if val else ''

        description_html = f'''
<img src="{row['image_url']}" alt="{item_title}" style="max-width:100%;" />
{detail_row('Artist', artist)}
{detail_row('Year', year)}
{detail_row('Styles', fmt(row.get('styles')))}
{detail_row('Genres', fmt(row.get('genres')))}
{detail_row('Media', fmt(row.get('media')))}
{detail_row('Tags', fmt(row.get('tags')))}
{detail_row('Location', row.get('location'))}
{detail_row('Galleries', fmt(row.get('galleries')))}
{detail_row('Period', row.get('period'))}
{detail_row('Description', row.get('description'))}
'''.strip()

        SubElement(item, 'description').text = description_html

        SubElement(item, 'media:content', {
            'url': row['image_url'],
            'medium': 'image',
        })

        if artist:
            SubElement(item, 'dc:creator').text = artist

    xml_bytes = tostring(rss, encoding='unicode')
    pretty = parseString(xml_bytes).toprettyxml(indent='  ')
    lines = pretty.split('\n')
    if lines[0].startswith('<?xml'):
        lines[0] = '<?xml version="1.0" encoding="UTF-8"?>'
    output = '\n'.join(lines)

    FEEDS_DIR.mkdir(exist_ok=True)
    output_path = FEEDS_DIR / f'{name}.xml'
    output_path.write_text(output, encoding='utf-8')
    print(f'Feed written to {output_path} with {len(picks)} items')


build_feed(
    name='daily',
    title='Daily Art Feed',
    link='https://www.wikiart.org',
    description='A daily random selection of artworks from WikiArt',
)
