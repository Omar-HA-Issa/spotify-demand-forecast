import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

sys.path.append(str(Path(__file__).parent.parent.parent))
import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_spotify_dataset(dataset_path):
    try:
        logger.info(f"Loading Spotify dataset from {dataset_path}")
        df = pd.read_csv(dataset_path)

        required_columns = ['track_id', 'track_name', 'artists', 'popularity']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        logger.info(f"Loaded {len(df)} tracks from dataset")
        return df
    except FileNotFoundError:
        logger.error(f"Dataset file not found: {dataset_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading dataset: {str(e)}")
        raise


def is_weekend(date):
    return date.weekday() in [4, 5]


def is_summer_month(date):
    return date.month in [6, 7, 8]


def calculate_streams(popularity, date, base_streams):
    streams = base_streams

    if is_weekend(date):
        weekend_multiplier = 1 + np.random.uniform(config.WEEKEND_BOOST_MIN, config.WEEKEND_BOOST_MAX)
        streams *= weekend_multiplier

    if np.random.random() < config.VIRAL_SPIKE_PROBABILITY:
        viral_multiplier = np.random.uniform(config.VIRAL_SPIKE_MULTIPLIER_MIN, config.VIRAL_SPIKE_MULTIPLIER_MAX)
        streams *= viral_multiplier

    if is_summer_month(date):
        streams *= (1 + config.SUMMER_BOOST)

    return int(streams)


def calculate_playlist_adds(streams):
    base_adds = streams / config.PLAYLIST_ADDS_DIVISOR
    variance = np.random.uniform(0.8, 1.2)
    return int(base_adds * variance)


def generate_streaming_data(tracks_df, num_days, start_date, regions):
    logger.info(f"Generating {num_days} days of streaming data for {len(tracks_df)} tracks")

    streaming_records = []
    start = datetime.strptime(start_date, "%Y-%m-%d")

    for day_offset in range(num_days):
        current_date = start + timedelta(days=day_offset)

        if (day_offset + 1) % 30 == 0:
            logger.info(f"Progress: {day_offset + 1}/{num_days} days completed")

        for _, track in tracks_df.iterrows():
            for region in regions:
                base_streams = track['popularity'] * np.random.uniform(
                    config.BASE_STREAM_MULTIPLIER_MIN,
                    config.BASE_STREAM_MULTIPLIER_MAX
                )

                streams = calculate_streams(track['popularity'], current_date, base_streams)
                playlist_adds = calculate_playlist_adds(streams)

                record = {
                    'date': current_date.strftime('%Y-%m-%d'),
                    'track_id': track['track_id'],
                    'track_name': track['track_name'],
                    'artists': track['artists'],
                    'streams': streams,
                    'region': region,
                    'playlist_adds': playlist_adds,
                    'popularity': track['popularity']
                }

                streaming_records.append(record)

    logger.info(f"Generated {len(streaming_records)} streaming records")
    return pd.DataFrame(streaming_records)


def save_streaming_data(df, output_path):
    try:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"Saving streaming data to {output_path}")
        df.to_csv(output_path, index=False)
        logger.info(f"Successfully saved {len(df)} records to {output_path}")

        logger.info("\n=== Data Summary ===")
        logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
        logger.info(f"Total records: {len(df):,}")
        logger.info(f"Unique tracks: {df['track_id'].nunique()}")
        logger.info(f"Regions: {df['region'].unique().tolist()}")
        logger.info(f"Total streams: {df['streams'].sum():,}")
        logger.info(f"Avg daily streams per track: {df['streams'].mean():.0f}")
        logger.info(f"Total playlist adds: {df['playlist_adds'].sum():,}")

    except Exception as e:
        logger.error(f"Error saving data: {str(e)}")
        raise


def main():
    try:
        logger.info("=== Starting Streaming Data Generation ===")

        tracks_df = load_spotify_dataset(config.SPOTIFY_DATASET_PATH)

        if len(tracks_df) > config.SAMPLE_TRACKS:
            logger.info(f"Sampling {config.SAMPLE_TRACKS} tracks from {len(tracks_df)} total tracks")
            tracks_df = tracks_df.sample(n=config.SAMPLE_TRACKS, random_state=42)

        streaming_df = generate_streaming_data(
            tracks_df=tracks_df,
            num_days=config.NUM_DAYS,
            start_date=config.START_DATE,
            regions=config.REGIONS
        )

        save_streaming_data(streaming_df, config.OUTPUT_PATH)

        logger.info("=== Streaming Data Generation Complete ===")

    except Exception as e:
        logger.error(f"Fatal error in data generation: {str(e)}")
        raise


if __name__ == "__main__":
    main()
