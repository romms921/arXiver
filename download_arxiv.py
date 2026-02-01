import pandas as pd
import requests
import time
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from tqdm import tqdm  # pip install tqdm

# --- CONFIGURATION ---
# 1. Enter your SSD Name here (Case sensitive!)
SSD_NAME = "T7 Shield"  # <--- CHANGE THIS (e.g., "Samsung_T5")
FOLDER_NAME = "arXiv 2025"

# 2. Construct the full path (MacOS mounts external drives at /Volumes/)
output_dir = os.path.join("/Volumes", SSD_NAME, FOLDER_NAME)

# 3. Path to your CSV
csv_path = '/Users/ainsleylewis/Documents/Astronomy/arXiver/2025_Data.csv'

# --- GLOBAL STATUS ---
# Shared variables for the web server to read
status = {
    "total": 0,
    "current": 0,
    "last_downloaded": "None",
    "errors": 0,
    "running": True
}

# --- WEB SERVER ---
class ProgressHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        
        percent = 0
        if status["total"] > 0:
            percent = round((status["current"] / status["total"]) * 100, 2)
            
        html = f"""
        <html>
        <head>
            <title>ArXiv Downloader Status</title>
            <meta http-equiv="refresh" content="3"> <!-- Auto-refresh every 3s -->
            <style>
                body {{ font-family: sans-serif; padding: 20px; }}
                .bar-container {{ width: 100%; background-color: #ddd; }}
                .bar {{ width: {percent}%; height: 30px; background-color: #4CAF50; text-align: center; line-height: 30px; color: white; }}
            </style>
        </head>
        <body>
            <h1>ArXiv Downloader Progress</h1>
            <div class="bar-container">
                <div class="bar">{percent}%</div>
            </div>
            <p><strong>Processed:</strong> {status['current']} / {status['total']}</p>
            <p><strong>Last File:</strong> {status['last_downloaded']}</p>
            <p><strong>Errors:</strong> {status['errors']}</p>
            <p><strong>Status:</strong> {"Running" if status['running'] else "Complete"}</p>
        </body>
        </html>
        """
        self.wfile.write(html.encode("utf-8"))

    def log_message(self, format, *args):
        return # Suppress server logging to keep console clean

def start_server():
    server = HTTPServer(("localhost", 8000), ProgressHandler)
    print("Web progress available at http://localhost:8000")
    while status["running"]:
        server.handle_request()

# --- SAFETY CHECKS ---
# Check if the SSD is actually connected
if not os.path.exists(os.path.join("/Volumes", SSD_NAME)):
    print(f"Error: Could not find drive '{SSD_NAME}'. Is it plugged in?")
    print("Run 'ls /Volumes' in terminal to see the correct name.")
    exit()

# Create the folder on the SSD if it doesn't exist
os.makedirs(output_dir, exist_ok=True)
print(f"Files will be saved to: {output_dir}")

# --- DATA LOADING ---
data = pd.read_csv(csv_path)
data = data[9447:9500]

# Clean IDs: Remove .pdf, remove versions (v1), get ID from URL
def clean_id(link_str):
    if not isinstance(link_str, str): return ""
    return link_str.split('/')[-1].replace('.pdf', '').split('v')[0]

data['arxiv_id'] = data['pdf_link'].apply(clean_id)
arxiv_ids = [x for x in data['arxiv_id'].tolist() if x] # Filter empty

# Initialize status
status["total"] = len(arxiv_ids)

# Start Web Server in a separate thread
thread = threading.Thread(target=start_server)
thread.daemon = True
thread.start()

# --- DOWNLOAD LOOP ---
print(f"Starting download for {len(arxiv_ids)} papers...")

# Wrap loop with tqdm for progress bar
for i, aid in enumerate(tqdm(arxiv_ids, desc="Downloading Papers", unit="paper")):
    status["current"] = i + 1
    
    # Skip if we already downloaded it (checks for both .tar.gz and .pdf)
    # This lets you stop the script and restart it later without losing progress.
    if any(fname.startswith(aid) for fname in os.listdir(output_dir)):
        status["last_downloaded"] = f"{aid} (Skipped)"
        continue

    url = f"https://arxiv.org/e-print/{aid}"
    
    try:
        # User-Agent is required by arXiv. Change the email if you want.
        headers = {'User-Agent': 'ResearchProject/1.0 (rommuluslewis@gmail.com)'}
        response = requests.get(url, stream=True, headers=headers)
        
        if response.status_code == 200:
            start = time.time()
            # Determine extension (Source is usually tar.gz, but sometimes PDF if no source exists)
            content_type = response.headers.get('content-type', '')
            if "application/pdf" in content_type:
                ext = ".pdf"
            else:
                ext = ".tar.gz"
            
            file_path = os.path.join(output_dir, f"{aid}{ext}")
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            status["last_downloaded"] = f"{aid}{ext}"
            
        else:
            tqdm.write(f"Failed {aid}: Status {response.status_code}")
            status["errors"] += 1

    except Exception as e:
        tqdm.write(f"Error {aid}: {e}")
        status["errors"] += 1
    end = time.time()
    elapsed = end - start

    # CRITICAL: Sleep 3 seconds to avoid IP ban
    if elapsed < 3:
        time.sleep(3 - elapsed) 

status["running"] = False
print(f"Download complete. Web server stopping.")