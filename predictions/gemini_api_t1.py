import os
import ast
import re
import json
import time
import pandas as pd
from tqdm.auto import tqdm
from google import genai
import dotenv

# =======================
# CONFIGURATION
# =======================
MODEL_ID = "gemini-2.5-flash-lite"
CSV_PATH = "test_filled_16.csv"
OUTPUT_CSV_PATH = "test_filled_17.csv"
LATEX_FILES = [
    "latex_affiliations_output.txt",
    "latex_affiliations_output_2.txt"
]

RATE_LIMIT_SECONDS = 60.0  # For free tier: 15 requests per minute
MAX_RETRIES = 3
RETRY_DELAY = 60  # seconds to wait on quota error

# =======================
# INIT GEMINI CLIENT
# =======================
dotenv.load_dotenv()

client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))

for m in client.models.list():
    for action in m.supported_actions:
            print(m.name, action)