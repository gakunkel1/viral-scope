import os
from dotenv import load_dotenv
from pprint import pprint

load_dotenv()

class Config:
    _instance = None
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            
            # Load credentials
            cls._instance._youtube_api_key = os.getenv('YOUTUBE_API_KEY', 'placeholder_youtube_api_key')
            print("Config initialized and API key loaded")
        
        return cls._instance
    
    @property
    def youtube_api_key(self):
        return self._instance._youtube_api_key
    
    @youtube_api_key.setter
    def youtube_api_key(self, value):
        self._instance._youtube_api_key = value