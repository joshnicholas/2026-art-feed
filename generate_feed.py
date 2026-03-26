import pandas as pd
from datetime import datetime, timezone
from email.utils import format_datetime
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom.minidom import parseString

FEEDS_DIR = Path('feeds')
PARQUET_PATH = Path('input/combined.parquet')


def pick_items(sample_size: int = 10) -> list[dict]:
    df = pd.read_parquet(PARQUET_PATH)
    picks = df.sample(n=min(sample_size, len(df)))
    return picks.to_dict(orient='records')


def build_feed(
    name: str,
    title: str = 'Art Feed',
    link: str = 'https://www.wikiart.org',
    description: str = 'A daily selection of artworks from WikiArt',
    sample_size: int = 10,
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

        item_title = row.get('title') or 'Untitled'
        artist = row.get('artist_name') or row.get('artist') or ''
        year = row.get('year') or ''
        item_title_str = f"{item_title} — {artist}" + (f" ({year})" if year else '')
        SubElement(item, 'title').text = item_title_str
        SubElement(item, 'link').text = row['wikiart_url']
        SubElement(item, 'guid', {'isPermaLink': 'true'}).text = row['wikiart_url']
        SubElement(item, 'pubDate').text = now

        def detail_row(label, value):
            return f'<p><strong>{label}:</strong> {value}</p>' if value else ''

        def fmt(val):
            return val.replace('|', ', ') if isinstance(val, str) and val else ''

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
