import sys
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# URL of the directory listing
base_url = "your_url"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

# Local folder to save downloaded files
download_folder = "downloads"

# Variable to control whether to overwrite existing files
allow_override = True

# Lock for thread-safe console output
output_lock = threading.Lock()

# Boolean variable to specify whether to run in parallel or sequentially
is_parallel = False  # True = Parallel mode, False = Sequential mode

# Create the folder if it doesn't exist
if not os.path.exists(download_folder):
    os.makedirs(download_folder)


def sanitize_filename(filename):
    """Remove invalid characters from the filename."""
    invalid_chars = r'<>:"/\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, "")
    return filename


def download_file(file_url, file_name):
    """
    Download a single file, overriding if allowed.
    """
    file_path = os.path.join(download_folder, file_name)

    # Check if the file already exists and handle based on `allow_override`
    if os.path.exists(file_path):
        if not allow_override:
            with output_lock:
                print(f"[SKIP] {file_name}: already exists.")
            return
        else:
            with output_lock:
                print(f"[OVERWRITE] {file_name}...")

    try:
        # Send HEAD request to get file size
        head_response = requests.head(file_url, headers=headers)
        file_size = int(head_response.headers.get('Content-Length', 0))

        # Download the file with a progress bar
        file_response = requests.get(file_url, stream=True)
        file_response.raise_for_status()

        with open(file_path, "wb") as file:
            with tqdm(
                    total=file_size,
                    unit='B',
                    unit_scale=True,
                    desc=file_name,
                    position=0,  # Ensures multiple progress bars don't collide
                    leave=False,
                    file=sys.stderr  # Print progress bars to stderr
            ) as pbar:
                for chunk in file_response.iter_content(chunk_size=8192):
                    file.write(chunk)
                    pbar.update(len(chunk))

        with output_lock:
            print(f"[DOWNLOAD COMPLETE] {file_name}")

    except requests.exceptions.RequestException as e:
        with output_lock:
            print(f"[ERROR] {file_name}: {e}")


def download_files_parallel(url):
    try:
        # Get the HTML content
        response = requests.get(base_url, headers=headers)
        response.raise_for_status()

        # Parse the HTML with BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")

        # Find all <a> tags with file links
        links = soup.find_all("a")

        # List of tasks for parallel download
        tasks = []

        for link in links:
            file_name = link.get("href")

            # Skip parent directory links or subfolders
            if not file_name or file_name.endswith("/"):
                continue

            # Remove query strings and sanitize filename
            file_name = os.path.basename(urlparse(file_name).path)
            file_name = sanitize_filename(file_name)

            # Skip empty or invalid filenames after sanitization
            if not file_name:
                continue

            # Build full file URL
            file_url = urljoin(base_url, file_name)

            # Append the download task
            tasks.append((file_url, file_name))

        # Use ThreadPoolExecutor to download files in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:  # Adjust `max_workers` for performance
            futures = [executor.submit(download_file, file_url, file_name) for file_url, file_name in tasks]

            # Process completed tasks for cleaner output
            for future in as_completed(futures):
                future.result()  # Raise any exception if present

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] General: {e}")

def download_files_sequential(url):
    try:
        # Get the HTML content
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Parse the HTML with BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")

        # Find all <a> tags with file links
        links = soup.find_all("a")

        for link in links:
            file_name = link.get("href")

            # Skip parent directory links or subfolders
            if not file_name or file_name.endswith("/"):
                continue

            # Remove query strings and sanitize filename
            file_name = os.path.basename(urlparse(file_name).path)
            file_name = sanitize_filename(file_name)

            # Skip empty or invalid filenames after sanitization
            if not file_name:
                continue

            # Build full file URL
            file_url = urljoin(base_url, file_name)

            # Download each file sequentially
            download_file(file_url, file_name)

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] General: {e}")


# Start the download process with either sequential or parallel execution
if __name__ == "__main__":
    if is_parallel:
        print("[INFO] Running in PARALLEL mode")
        download_files_parallel(base_url)
    else:
        print("[INFO] Running in SEQUENTIAL mode")
        download_files_sequential(base_url)