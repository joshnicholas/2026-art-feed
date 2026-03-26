import pandas as pd
from pathlib import Path

INPUT_DIR = Path('input')
OUTPUT_PATH = Path('input/combined.parquet')


def combine():
    csv_files = [p for p in INPUT_DIR.glob('*.csv') if p.name != 'combined.csv']
    if not csv_files:
        print('No CSV files found in input/')
        return

    frames = []
    for path in csv_files:
        try:
            df = pd.read_csv(path, dtype=str)
            df['source'] = path.stem
            frames.append(df)
            print(f'  {path.name}: {len(df)} rows')
        except Exception as e:
            print(f'  Skipping {path.name}: {e}')

    combined = pd.concat(frames, ignore_index=True)
    before = len(combined)

    combined = combined[combined['image_url'].notna() & combined['wikiart_url'].notna()]
    combined = combined[combined['image_url'].str.strip() != '']
    combined = combined[combined['wikiart_url'].str.strip() != '']
    combined = combined.drop_duplicates(subset=['image_url'])
    combined = combined.drop_duplicates(subset=['wikiart_url'])

    after = len(combined)
    print(f'\n{before} rows → {after} after deduplication ({before - after} removed)')

    combined.to_parquet(OUTPUT_PATH, index=False)
    print(f'Saved to {OUTPUT_PATH}')


combine()
