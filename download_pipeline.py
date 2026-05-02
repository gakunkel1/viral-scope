import csv
import logging
import traceback
from pprint import pprint

from youtube.channels import get_uploads_playlist_by_handle, get_channel_details_by_handle
from models.channels import Channel

def run_pipeline():
    """Ingest channel details into PostgreSQL."""
    channels = get_channels_to_process()
    print(f'channels: {channels}')
    for channel in channels:
        process_channel(channel)
    
    
def get_channels_to_process() -> list[str]:
    """Get channels to process from channels.csv."""
    channels_file = 'channels.csv'
    with open(channels_file, mode='r') as file:
        reader = csv.DictReader(file)
        return [row['channel_handle'] for row in reader]
    
        
def process_channel(channel_handle: str):
    """Process a single channel."""
    channel_details = get_channel_details_by_handle(channel_handle)
    pprint(f'channel_details: {channel_details}')
    
def save_channel_details(channel: Channel):
    """Load channel details to PostgreSQL."""
    
    
    
if __name__ == '__main__':
    try:
        run_pipeline()
    except Exception as e:
        logging.error(traceback.format_exc())