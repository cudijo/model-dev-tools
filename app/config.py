from functools import lru_cache
from typing import Any, Dict, List, Union
from pydantic import BaseModel, BaseSettings
import json

class Settings(BaseSettings):
    PATH_GOOGLE_CREDENTIALS: str
    REGROW_EMAIL: str

@lru_cache()
def get_settings():
    return Settings()

def read_json(path: str) -> Dict:    
    with open(path, 'r') as f:
        return json.load(f)