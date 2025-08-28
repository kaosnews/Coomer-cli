#!/usr/bin/env python3

# Standard library
import os
import re
import sys
import time
import signal
import logging
import sqlite3
import hashlib
import threading
import argparse
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin, quote_plus, parse_qsl

# Third party
import requests
import speedtest
from tqdm import tqdm

# Color support
try:
    import colorama
    colorama.init(autoreset=True)
    
    class Colors:
        RESET  = colorama.Style.RESET_ALL
        RED    = colorama.Fore.RED
        GREEN  = colorama.Fore.GREEN
        YELLOW = colorama.Fore.YELLOW
except ImportError:
    class Colors:
        """Fallback when colorama is not available."""
        RESET = RED = GREEN = YELLOW = ""

# Type alias for media info
MediaTuple = Tuple[str, Optional[Any], Optional[str]]

# Configure logging
def setup_logging() -> None:
    """Configure logging with color support."""
    class ColorFormatter(logging.Formatter):
        COLORS = {
            logging.DEBUG: Colors.RESET,
            logging.INFO: Colors.RESET,
            logging.WARNING: Colors.YELLOW,
            logging.ERROR: Colors.RED
        }

        def format(self, record: logging.LogRecord) -> str:
            # Add color based on level
            color = self.COLORS.get(record.levelno, Colors.RESET)
            
            # Add success color
            if "Success" in record.msg or "Downloaded" in record.msg:
                color = Colors.GREEN
                
            return f"{color}{record.getMessage()}{Colors.RESET}"

    handler = logging.StreamHandler()
    handler.setFormatter(ColorFormatter())
    
    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler]
    )

setup_logging()

def log(msg: str, level: int = logging.INFO) -> None:
    """
    Log a message with the specified level.
    
    Args:
        msg: Message to log
        level: Logging level (logging.INFO/WARNING/ERROR/DEBUG), defaults to INFO
        
    Colors are automatically applied by the formatter based on level:
        - DEBUG/INFO: Default color
        - WARNING: Yellow
        - ERROR: Red
        - Success messages: Green
    """
    logging.log(level, msg)


class DownloaderCLI:
    def _print_download_summary(self, successful: int, total: int) -> None:
        """Print final download statistics."""
        if total == 0:
            return
            
        success_rate = (successful / total) * 100
        failed = total - successful
        
        log("\n=== Download Summary ===", logging.INFO)
        if success_rate >= 90:
            log(f"Success: {successful}/{total} ({success_rate:.1f}%)", logging.INFO)
        else:
            log(f"Success: {successful}/{total} ({success_rate:.1f}%)", logging.WARNING)
        if failed:
            log(f"Failed: {failed}", logging.ERROR)
        log("=====================\n", logging.INFO)
    def __init__(
        self,
        download_folder: str,
        max_workers: int = 5,
        rate_limit_interval: float = 2.0,
        domain_concurrency: int = 2,
        verify_checksum: bool = False,
        only_new_stop: bool = True,
        download_mode: str = 'concurrent',
        file_naming_mode: int = 0,
        cookie_string: Optional[str] = None,
        retry_count: int = 2,
        retry_delay: float = 2.0
    ) -> None:
        # Settings
        self.download_folder = download_folder
        self.max_workers = max_workers
        self.rate_limit_interval = rate_limit_interval
        self.domain_concurrency = domain_concurrency
        self.verify_checksum = verify_checksum
        self.only_new_stop = only_new_stop
        self.download_mode = download_mode
        self.file_naming_mode = file_naming_mode
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        
        # Threading
        self.cancel_requested = threading.Event()
        self.domain_last_request = defaultdict(float)
        self.domain_locks = defaultdict(lambda: threading.Semaphore(domain_concurrency))

        # Database
        self.db_conn: Optional[sqlite3.Connection] = None
        self.db_cursor: Optional[sqlite3.Cursor] = None
        self.download_cache: Dict[str, Tuple[str, int, Optional[str]]] = {}

        # Session setup
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/115.0",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive"
        }
        
        # Initialize session cookies if provided
        if cookie_string:
            # Parse cookie string (handles both comma and semicolon separators)
            cookie_string = cookie_string.replace(';', ',').strip(' ,')
            cookie_pairs = [p.strip() for p in cookie_string.split(',') if '=' in p]
            
            for pair in cookie_pairs:
                name, value = pair.split('=', 1)
                self.session.cookies.set(name, value)
                
            log(f"Session initialized with {len(cookie_pairs)} cookies", logging.DEBUG)

        self.file_extensions: Dict[str, Tuple[str, ...]] = {
            'images': ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff'),
            'videos': ('.mp4', '.mkv', '.webm', '.mov', '.avi', '.flv', '.wmv', '.m4v'),
            'documents': ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx'),
            'compressed': ('.zip', '.rar', '.7z', '.tar', '.gz'),
        }

        # Precompile regex to sanitize file names.
        self._filename_sanitize_re = re.compile(r'[<>:"/\\|?*]')

    def init_profile_database(self, profile_name: str) -> None:
        """Set up SQLite database to track downloads for a profile."""
        if self.db_conn:
            self.db_conn.close()
        
        db_path = os.path.join(self.download_folder, f"{profile_name}.db")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        try:
            log(f"Opening database: {db_path}", logging.INFO)
            self.db_conn = sqlite3.connect(db_path, check_same_thread=False)
            self.db_conn.execute("PRAGMA journal_mode=WAL;")
            self.db_conn.execute("PRAGMA synchronous=NORMAL;")
            self.db_conn.execute("PRAGMA temp_store=MEMORY;")
            self.db_cursor = self.db_conn.cursor()
            self.db_lock = threading.Lock()
            
            # Create schema
            self.db_cursor.execute("""
                CREATE TABLE IF NOT EXISTS downloads (
                    url TEXT PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    file_size INTEGER,
                    downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            self.db_conn.commit()
            
            # Load existing downloads
            self.db_cursor.execute("SELECT url, file_path, file_size FROM downloads")
            self.download_cache = {row[0]: (row[1], row[2], None) for row in self.db_cursor.fetchall()}
            
            log(f"Database initialized with {len(self.download_cache)} existing entries", logging.INFO)
            
        except sqlite3.Error as e:
            log(f"Database initialization failed: {e}", logging.ERROR)
            raise

    def _record_download(self, url: str, file_path: str) -> None:
        """Save download details to database (thread-safe)."""
        if not getattr(self, "db_cursor", None) or not getattr(self, "db_conn", None):
            log("Database not initialized", logging.WARNING)
            return

        try:
            size = os.path.getsize(file_path)
            filename = os.path.basename(file_path)

            # Serialize DB access across threads
            with self.db_lock:
                self.db_cursor.execute(
                    "INSERT OR REPLACE INTO downloads (url, file_path, file_size) VALUES (?, ?, ?)",
                    (url, file_path, size)
                )
                self.db_conn.commit()

            # Update cache *after* a successful commit
            self.download_cache[url] = (file_path, size, None)
            log(f"Recorded download: {filename} ({size/1024/1024:.1f}MB)", logging.INFO)

        except Exception as e:
            # Optional: import traceback and include stack for easier debugging
            # import traceback
            # log(f"Failed to record download in database: {e}\n{traceback.format_exc()}", logging.ERROR)
            log(f"Failed to record download in database: {e}", logging.ERROR)

    def close(self):
        """Gracefully commit and close the database connection."""
        try:
            if getattr(self, "db_conn", None):
                lock = getattr(self, "db_lock", None)
                if lock:
                    with lock:
                        try:
                            self.db_conn.commit()
                        except Exception:
                            pass
                        self.db_conn.close()
                else:
                    try:
                        self.db_conn.commit()
                    except Exception:
                        pass
                    self.db_conn.close()
        except Exception:
            log(f"DB close error:\n{traceback.format_exc()}", logging.WARNING)


    def request_cancel(self) -> None:
        """Request cancellation of downloads."""
        log("Cancellation requested...", logging.WARNING)
        self.cancel_requested.set()

    def sanitize_filename(self, filename: str) -> str:
        """Sanitize a filename by removing invalid characters."""
        return self._filename_sanitize_re.sub('_', filename)

    def detect_file_category(self, url: str) -> str:
        ext = os.path.splitext(urlparse(url).path)[1].lower()
        for category, extensions in self.file_extensions.items():
            if ext in extensions:
                return category
        return 'others'

    def safe_request(
        self,
        url: str,
        method: str = "get",
        stream: bool = True,
        extra_headers: Optional[Dict[str, str]] = None
    ) -> Optional[requests.Response]:
        """Make rate-limited HTTP request with error handling."""
        if self.cancel_requested.is_set():
            return None

        domain = urlparse(url).netloc
        filename = os.path.basename(url)

        # Prepare headers
        req_headers = {**self.headers}
        
        # Add required Accept header for coomer/kemono API requests
        if '/api/' in url and any(d in domain for d in ['coomer.', 'kemono.']):
            req_headers['Accept'] = 'text/css'
            
        if extra_headers:
            req_headers.update(extra_headers)
        if self.session.cookies:
            cookie_string = '; '.join([f"{k}={v}" for k, v in self.session.cookies.items()])
            if cookie_string:
                req_headers['Cookie'] = cookie_string

        # Handle rate limiting
        with self.domain_locks[domain]:
            elapsed = time.time() - self.domain_last_request[domain]
            if elapsed < self.rate_limit_interval:
                wait_time = self.rate_limit_interval - elapsed
                log(f"Rate limit: waiting {wait_time:.1f}s for {domain}", logging.INFO)
                time.sleep(wait_time)

            try:
                resp = self.session.request(
                    method,
                    url,
                    headers=req_headers,
                    stream=stream,
                    allow_redirects=True,
                    timeout=None if stream else 30.0
                )
                resp.raise_for_status()
                self.domain_last_request[domain] = time.time()
                
                if not stream:  # Only log for API requests, not file downloads
                    log(f"Request successful: {filename}", logging.INFO)
                return resp

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 403:
                    log(f"Access denied for {filename} - try using cookies", logging.ERROR)
                    # log response body for debugging in verbose mode
                    try:
                        response_text = e.response.text[:1000] if e.response.text else "No response body"
                        log(f"403 Response body: {response_text}", logging.DEBUG)
                    except Exception:
                        log("Could not read 403 response body", logging.DEBUG)
                elif e.response.status_code == 429:
                    log(f"Rate limited by {domain} - consider increasing delay", logging.ERROR)
                    try:
                        response_text = e.response.text[:1000] if e.response.text else "No response body"
                        log(f"429 Response body: {response_text}", logging.DEBUG)
                    except Exception:
                        log("Could not read 429 response body", logging.DEBUG)
                else:
                    log(f"HTTP {e.response.status_code} error for {filename}: {e}", logging.ERROR)
                    try:
                        response_text = e.response.text[:1000] if e.response.text else "No response body"
                        log(f"{e.response.status_code} Response body: {response_text}", logging.DEBUG)
                    except Exception:
                        log(f"Could not read {e.response.status_code} response body", logging.DEBUG)
            except requests.exceptions.ConnectionError as e:
                log(f"Connection error for {filename}: {e}", logging.ERROR)
            except requests.exceptions.Timeout as e:
                log(f"Timeout error for {filename}: {e}", logging.ERROR)
            except Exception as e:
                log(f"Request failed for {filename}: {e}", logging.ERROR)

            return None

    def generate_filename(
        self,
        media_url: str,
        post_id: Optional[Any] = None,
        post_title: Optional[str] = None,
        attachment_index: int = 1
    ) -> str:
        """
        Generate a filename based on the naming mode.
        Modes:
            0: original name + '_' + attachment_index
            1: post title + '_' + attachment_index + '_' + short MD5 hash
            2: post title + ' - ' + post_id + '_' + attachment_index
        """
        base_name = os.path.basename(media_url).split('?')[0]
        extension = os.path.splitext(base_name)[1]
        sanitized_base = self.sanitize_filename(os.path.splitext(base_name)[0])
        sanitized_title = self.sanitize_filename(post_title or "post")
        
        if self.file_naming_mode == 0:
            final_name = f"{sanitized_base}_{attachment_index}{extension}"
        elif self.file_naming_mode == 1:
            short_hash = hashlib.md5(media_url.encode()).hexdigest()[:8]
            final_name = f"{sanitized_title}_{attachment_index}_{short_hash}{extension}"
        elif self.file_naming_mode == 2:
            if post_id:
                final_name = f"{sanitized_title} - {post_id}_{attachment_index}{extension}"
            else:
                final_name = f"{sanitized_title}_{attachment_index}{extension}"
        else:
            final_name = f"{sanitized_base}_{attachment_index}{extension}"
        return final_name

    def _write_file(self, resp: requests.Response, final_path: str) -> bool:
        """Write downloaded file with simple progress display."""
        tmp_path = final_path + ".tmp"
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

        # Get file size for progress tracking
        total_size = int(resp.headers.get('content-length', 0))
        filename = os.path.basename(final_path)
        current_size = 0
        start_time = time.time()
        
        try:
            with open(tmp_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if self.cancel_requested.is_set():
                        f.close()
                        os.remove(tmp_path)
                        return False
                    if chunk:
                        f.write(chunk)
                        current_size += len(chunk)
                        
                        # Calculate progress
                        percent = (current_size / total_size * 100) if total_size > 0 else 0
                        mb_current = current_size / 1024 / 1024
                        mb_total = total_size / 1024 / 1024
                        
                        # Calculate estimated time remaining
                        elapsed = time.time() - start_time
                        if current_size > 0:
                            bytes_per_second = current_size / elapsed
                            remaining_bytes = total_size - current_size
                            est_remaining = remaining_bytes / bytes_per_second if bytes_per_second > 0 else 0
                            est_str = f"~{est_remaining:.0f}s" if est_remaining < 60 else f"~{est_remaining/60:.1f}m"
                        else:
                            est_str = "~???"
                            
                        # Use pastel colors
                        status = (
                            f"{Colors.GREEN}{filename}{Colors.RESET}  -  "
                            f"{Colors.YELLOW}{percent:3.0f}%{Colors.RESET} "
                            f"{Colors.GREEN}{mb_current:.1f}{Colors.RESET}/"
                            f"{Colors.YELLOW}{mb_total:.1f}{Colors.RESET}MB "
                            f"{Colors.RED}{est_str}{Colors.RESET}"
                        )
                        print(f"\r{status}", end="", flush=True)
                print()  # New line after download completes

            # Finalize download
            os.rename(tmp_path, final_path)
            log(f"Successfully written: {os.path.basename(final_path)}", logging.INFO)
            return True

        except Exception as e:
            log(f"Write failed: {e}", logging.ERROR)
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except:
                    pass  # Best effort cleanup
            return False
    def fetch_username(self, base_site: str, service: str, user_id: str) -> str:
        """Get username for folder naming."""
        profile_url = f"{base_site}/api/v1/{service}/user/{user_id}/profile"
        resp = self.safe_request(profile_url, method="get", stream=False)
        try:
            return resp.json().get("name", user_id) if resp else user_id
        except Exception as e:
            log(f"Error fetching username: {e}", logging.ERROR)
            return user_id

    def download_file(self, url: str, folder: str, post_id: Optional[Any] = None,
                     post_title: Optional[str] = None, attachment_index: int = 1) -> bool:
        """Download a single file."""
        if self.cancel_requested.is_set():
            return False

        os.makedirs(folder, exist_ok=True)
        filename = self.generate_filename(url, post_id, post_title, attachment_index)
        final_path = os.path.join(folder, filename)
        
        log(f"Downloading {filename}", logging.INFO)
        
        try:
            resp = self.safe_request(url, method="get", stream=True)
            if not resp:
                return False
            
            if self._write_file(resp, final_path):
                self._record_download(url, final_path)
                log(f"Successfully downloaded: {filename}")
                return True
            
            return False
            
        except Exception as e:
            log(f"Download failed: {filename} - {e}", logging.ERROR)
            if os.path.exists(final_path):
                os.remove(final_path)
            return False

    def compute_checksum(self, file_path: str) -> Optional[str]:
        """Calculate SHA256 hash of a file in chunks."""
        filename = os.path.basename(file_path)
        sha256 = hashlib.sha256()
        
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    sha256.update(chunk)
            
            checksum = sha256.hexdigest()
            log(f"Computed checksum for {filename}: {checksum[:8]}...", logging.INFO)
            return checksum
            
        except Exception as e:
            log(f"Failed to compute checksum for {filename}: {e}", logging.ERROR)
            return None

    def group_media_by_category(self, media_list: List[MediaTuple], file_type: str) -> Dict[str, List[MediaTuple]]:
        """
        Group media items by their category.
        If file_type is not 'all', filter by that type.
        """
        grouped: Dict[str, List[MediaTuple]] = defaultdict(list)
        for item in media_list:
            url, pid, ptitle = item
            cat = self.detect_file_category(url)
            if file_type == 'all' or cat == file_type:
                grouped[cat].append(item)
        return grouped

    def download_media(self, media_list: List[MediaTuple], folder_name: str, file_type: str = 'all') -> None:
        """Download media items either concurrently or sequentially."""
        self.init_profile_database(folder_name)
        base_folder = os.path.join(self.download_folder, folder_name)
        os.makedirs(base_folder, exist_ok=True)
        grouped = self.group_media_by_category(media_list, file_type)

        total_downloads = sum(len(items) for items in grouped.values())
        successful_downloads = 0

        # Create folder structure first
        for cat in grouped:
            folder = os.path.join(base_folder, cat)
            os.makedirs(folder, exist_ok=True)

        if self.download_mode == 'concurrent':
            log(f"Starting concurrent downloads ({self.max_workers} workers)", logging.INFO)
            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                futures = []
                active_downloads = {}
                
                # Queue all downloads
                for cat, items in grouped.items():
                    folder = os.path.join(base_folder, cat)
                    for idx, (url, pid, ptitle) in enumerate(items, 1):
                        if self.cancel_requested.is_set():
                            break
                        future = pool.submit(self.download_file, url, folder, pid, ptitle, idx)
                        future.url = url
                        futures.append(future)
                
                # Track progress
                completed = 0
                total = len(futures)
                while completed < total and not self.cancel_requested.is_set():
                    for future in [f for f in futures if f not in active_downloads and not f.done()]:
                        active_downloads[future] = True
                        
                    # Clear line and move cursor up for each active download
                    if active_downloads:
                        print("\033[2K\033[A" * len(active_downloads), end="")
                    
                    # Show current downloads
                    still_active = {}
                    for future in list(active_downloads.keys()):
                        if future.done():
                            try:
                                if future.result():
                                    successful_downloads += 1
                            except Exception as e:
                                log(f"Download failed: {os.path.basename(future.url)} - {e}", logging.ERROR)
                            completed += 1
                        else:
                            still_active[future] = True
                    
                    active_downloads = still_active
                    
                    # Show overall progress
                    print(f"{Colors.GREEN}Overall Progress: {Colors.YELLOW}{completed}/{total}{Colors.RESET} files ({successful_downloads} successful)")
                    time.sleep(0.1)  # Prevent excessive CPU usage
            
        else:  # Sequential downloads
            log(f"Starting sequential downloads", logging.INFO)
            completed = 0
            for cat, items in grouped.items():
                folder = os.path.join(base_folder, cat)
                for idx, (url, pid, ptitle) in enumerate(items, 1):
                    try:
                        if self.cancel_requested.is_set():
                            break
                        if self.download_file(url, folder, pid, ptitle, idx):
                            successful_downloads += 1
                        completed += 1
                        print(f"\n{Colors.GREEN}Overall Progress: {Colors.YELLOW}{completed}/{total_downloads}{Colors.RESET} files ({successful_downloads} successful)")
                    except Exception as e:
                        log(f"Download failed: {os.path.basename(url)} - {e}", logging.ERROR)
                        completed += 1

        # Print summary
        self._print_download_summary(successful_downloads, total_downloads)

    def download_only_new_posts(self, media_list: List[MediaTuple], folder_name: str, file_type: str = 'all') -> None:
        """Download only new files, optionally stopping at first existing file."""
        self.init_profile_database(folder_name)
        base_folder = os.path.join(self.download_folder, folder_name)
        os.makedirs(base_folder, exist_ok=True)
        grouped = self.group_media_by_category(media_list, file_type)

        total = sum(len(items) for items in grouped.values())
        if not total:
            return

        with tqdm(total=total, desc="Processing files", unit="file") as pbar:
            for cat, items in grouped.items():
                folder = os.path.join(base_folder, cat)
                os.makedirs(folder, exist_ok=True)

                for idx, (url, pid, ptitle) in enumerate(items, 1):
                    if self.cancel_requested.is_set():
                        break

                    filename = os.path.basename(url)
                    
                    if url in self.download_cache:
                        log(f"Skipping existing: {filename}", logging.INFO)
                        if self.only_new_stop:
                            log("Stopping at first existing file - use --continue-existing to override", logging.INFO)
                            return
                        pbar.update(1)
                        continue

                    try:
                        if self.download_file(url, folder, pid, ptitle, idx):
                            log(f"Downloaded new file: {filename}", logging.INFO)
                    except Exception as e:
                        log(f"Failed to download {filename}: {e}", logging.ERROR)
                    pbar.update(1)

    def _download_only_new_helper(self, url: str, folder: str, post_id: Optional[Any],
                                post_title: Optional[str], attachment_index: int) -> bool:
        """Helper method for downloading new files with proper headers."""
        parsed_url = urlparse(url)
        filename = self.generate_filename(url, post_id, post_title, attachment_index)
        
        # Set up headers with domain info
        extra_headers = {
            "Host": parsed_url.netloc,
            "Origin": f"{parsed_url.scheme}://{parsed_url.netloc}",
            "Referer": f"{parsed_url.scheme}://{parsed_url.netloc}/artists"
        }

        try:
            # Create folder and download file
            os.makedirs(folder, exist_ok=True)
            final_path = os.path.join(folder, filename)
            
            resp = self.safe_request(url, extra_headers=extra_headers, stream=True)
            if not resp:
                return False
                
            # Write file and record in database
            if self._write_file(resp, final_path):
                self._record_download(url, final_path)
                return True
                
            return False
            
        except Exception as e:
            log(f"Failed to download {filename}: {e}", logging.ERROR)
            return False

    def fetch_all_posts(self, base_site: str, user_id: str, service: str) -> List[Any]:
        """Fetch all posts for a user."""
        all_posts = []
        offset = 0
        user_enc = quote_plus(user_id)
        
        while not self.cancel_requested.is_set():
            url = f"{base_site}/api/v1/{service}/user/{user_enc}/posts?o={offset}"
            log(f"Fetching posts from offset {offset}", logging.INFO)
            
            resp = self.safe_request(url, method="get", stream=False)
            if not resp:
                break
                
            try:
                posts = resp.json()
                if not posts:
                    break
                all_posts.extend(posts)
                offset += 50
            except Exception as e:
                log(f"Error parsing response: {e}", logging.ERROR)
                break
                
        log(f"Found {len(all_posts)} total posts")
        return all_posts

    def fetch_search_posts(self, base_site: str, query: str) -> List[Any]:
        """Fetch posts matching a search query."""
        all_posts = []
        offset = 0
        query_enc = quote_plus(query)
        
        while not self.cancel_requested.is_set():
            url = f"{base_site}/api/v1/posts?q={query_enc}&o={offset}"
            log(f"Fetching results from offset {offset}", logging.INFO)
            
            resp = self.safe_request(url, method="get", stream=False)
            if not resp:
                break
                
            try:
                data = resp.json()
                if isinstance(data, dict) and 'posts' in data:
                    posts = data['posts']
                    if not posts:
                        break
                    all_posts.extend(posts)
                    offset += 50
                    
                    # Stop if we received fewer posts than expected
                    if len(posts) < 50:
                        break
                else:
                    log("Invalid response format from search API", logging.ERROR)
                    break
                    
            except Exception as e:
                log(f"Failed to parse search results: {e}", logging.ERROR)
                break
        
        log(f"Found {len(all_posts)} posts matching query")
        return all_posts

    def fetch_popular_posts(self, base_site: str, date: Optional[str] = None, period: Optional[str] = None) -> List[Any]:
        """Fetch popular posts with optional filtering.
        
        Args:
            base_site: Base site URL (coomer.su or kemono.su)
            date: Optional YYYY-MM-DD date filter
            period: Optional time period ('day', 'week', 'month')
        """
        # Build URL with filters
        url = f"{base_site}/api/v1/posts/popular"
        params = {}
        if date:
            params['date'] = date
        if period:
            params['period'] = period

        if params:
            param_str = '&'.join(f'{k}={quote_plus(str(v))}' for k, v in params.items())
            url = f"{url}?{param_str}"

        log(f"Fetching popular posts{f' for {period}' if period else ''}"
            f"{f' on {date}' if date else ''}")
            
        resp = self.safe_request(url, method="get", stream=False)
        if not resp:
            return []

        try:
            data = resp.json()
            posts = []
            
            # Handle different response formats
            if isinstance(data, dict):
                posts = data.get('results', data.get('posts', []))
            elif isinstance(data, list):
                posts = data
            else:
                log("Invalid response format from popular API", logging.ERROR)
                return []
                
            log(f"Found {len(posts)} popular posts")
            return posts
            
        except Exception as e:
            log(f"Failed to parse popular posts: {e}", logging.ERROR)
            return []

    def fetch_tag_posts(self, base_site: str, tag: str) -> List[Any]:
        """Fetch posts with a specific tag."""
        all_posts = []
        offset = 0
        tag_enc = quote_plus(tag)
        
        while not self.cancel_requested.is_set():
            url = f"{base_site}/api/v1/posts?tag={tag_enc}&o={offset}"
            log(f"Fetching tagged posts from offset {offset}", logging.INFO)
            
            resp = self.safe_request(url, method="get", stream=False)
            if not resp:
                break
                
            try:
                data = resp.json()
                if isinstance(data, dict) and 'posts' in data:
                    posts = data['posts']
                    if not posts:
                        break
                    
                    all_posts.extend(posts)
                    offset += 50
                    
                    # Stop if we got fewer posts than expected or hit total count
                    if len(posts) < 50 or (data.get('count', 0) <= len(all_posts)):
                        break
                else:
                    log("Invalid response format from tag API", logging.ERROR)
                    break
                    
            except Exception as e:
                log(f"Failed to parse tagged posts: {e}", logging.ERROR)
                break
            
            # Rate limiting
            time.sleep(0.5)
        
        log(f"Found {len(all_posts)} posts with tag")
        return all_posts

    def fetch_posts(self, base_site: str, user_id: str, service: str, max_posts: Optional[int] = None) -> List[Any]:
        """
        Fetch all posts from a profile/URL, up to max_posts if specified.
        Always scans through the entire profile to find newest posts first.
        Args:
            base_site: Base site URL (e.g. https://coomer.su)
            user_id: User ID to fetch posts for
            service: Service name (e.g. onlyfans, fanbox)
            max_posts: Maximum number of posts to return, or None for all posts
        """
        all_posts = self.fetch_all_posts(base_site, user_id, service)
        
        # Always scan all posts but limit the returned amount if max_posts specified
        if max_posts:
            return all_posts[:max_posts]
        return all_posts

    def _fetch_single_post(self, base_site: str, user_id: str, service: str) -> List[Any]:
        url = f"{base_site}/api/v1/{service}/user/{user_id}"
        resp = self.safe_request(url, method="get", stream=False)
        try:
            return resp.json() if resp else []
        except Exception:
            return []

    def extract_media(self, posts: List[Any], file_type: str, base_site: str) -> List[MediaTuple]:
        """
        Extract media information from posts.
        Returns a list of tuples: (media_url, post_id, post_title).
        """
        results: List[MediaTuple] = []
        for post in posts:
            post_id = post.get('id')
            post_title = post.get('title') or "Untitled"
            # Files
            if 'file' in post and 'path' in post['file']:
                path = post['file']['path']
                # Add /data/ prefix if needed and construct full URL
                if not path.startswith('http'):
                    # First ensure we have /data/ prefix
                    if not path.startswith('/data/'):
                        path = f"/data/{path.lstrip('/')}"
                    # Then join with base site
                    path = urljoin(base_site, path)
                else:
                    # For full URLs, still ensure /data/ is present
                    parsed = urlparse(path)
                    path_parts = parsed.path.lstrip('/').split('/')
                    if path_parts[0] != 'data':
                        new_path = f"/data/{'/'.join(path_parts)}"
                        path = f"{parsed.scheme}://{parsed.netloc}{new_path}"
                
                if file_type == 'all' or self.detect_file_category(path) == file_type:
                    results.append((path, post_id, post_title))
            # Attachments
            if 'attachments' in post:
                for att in post['attachments']:
                    path = att.get('path')
                    if path:
                        # Add /data/ prefix if needed and construct full URL
                        if not path.startswith('http'):
                            # First ensure we have /data/ prefix
                            if not path.startswith('/data/'):
                                path = f"/data/{path.lstrip('/')}"
                            # Then join with base site
                            path = urljoin(base_site, path)
                        else:
                            # For full URLs, still ensure /data/ is present
                            parsed = urlparse(path)
                            path_parts = parsed.path.lstrip('/').split('/')
                            if path_parts[0] != 'data':
                                new_path = f"/data/{'/'.join(path_parts)}"
                                path = f"{parsed.scheme}://{parsed.netloc}{new_path}"
                        
                        if file_type == 'all' or self.detect_file_category(path) == file_type:
                            results.append((path, post_id, post_title))
        return results

    def retry_file(self, url: str, folder: str) -> bool:
        """Retry failed download with exponential backoff."""
        filename = os.path.basename(url)
        
        for attempt in range(self.retry_count):
            try:
                if attempt > 0:  # Skip delay on first attempt
                    delay = min(30.0, self.retry_delay * (2 ** attempt))
                    log(f"Retrying {filename} (attempt {attempt + 1}/{self.retry_count}) after {delay:.1f}s delay", logging.INFO)
                    time.sleep(delay)
                    
                if self.download_file(url, folder):
                    log(f"Retry successful: {filename}", logging.INFO)
                    return True
                    
            except Exception as e:
                log(f"Retry attempt {attempt + 1} failed: {e}", logging.ERROR)
        
        log(f"All retry attempts failed for: {filename}", logging.ERROR)
        return False

    def close(self) -> None:
        """Clean up resources on exit."""
        if self.db_conn:
            try:
                self.db_conn.close()
                log("Database connection closed", logging.INFO)
            except Exception as e:
                log(f"Error closing database: {e}", logging.ERROR)


def create_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="coomer.py",
        description=(
            "Media Downloader for Coomer & Kemono\n\n"
            "Downloads media from user profiles, searches, tags, or popular posts.\n"
            "Supports batch downloading, filtering, authentication, and more.\n"
            "Use -h or --help for detailed usage information."
        ),
        epilog=(
            "Examples:\n"
            "  # Download images from a specific user profile\n"
            "  python coomer.py https://coomer.su/onlyfans/user/12345 -t images\n\n"
            "  # Download profile sequentially, using cookies, naming files with post title/ID\n"
            "  python3 coomer.py --url 'https://kemono.su/fanbox/user/4284365' -d ./downloads --sequential-videos -t all -c 25 -fn 2 --cookies \"session=...\"\n\n"
            "  # Download all favorited artists using login (requires --site)\n"
            "  python coomer.py --favorites --login --username myuser --password mypass --site coomer.su\n\n"
            "  # Download URLs from a file, filtering by date and size (requires --site if URLs are relative)\n"
            "  python coomer.py --input-file urls.txt --site kemono.su --date-after 2024-01-01 --max-size 50M\n\n"
            "  # Dry run a search query and export potential download URLs\n"
            "  python coomer.py --url 'https://coomer.su/posts?q=search_term' --dry-run --export-urls found_urls.txt\n\n"
            "Happy Downloading!"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # --- Performance & Networking ---
    perf_opts = parser.add_argument_group(
        "Performance & Networking",
        "Adjust download speed, concurrency, and network settings."
    )

    retry_group = perf_opts.add_mutually_exclusive_group()
    retry_group.add_argument(
        "--retry-immediately",
        action="store_true",
        help=(
            "Try to retry failed downloads immediately with doubled delay.\n"
            "If immediate retry fails, file will still be retried at end.\n"
            "Not recommended for rate-limited servers."
        )
    )
    retry_group.add_argument(
        "--retry-at-end",
        action="store_true",
        help=(
            "Save all failed downloads and retry them at the end (Default).\n"
            "More reliable for rate-limited servers.\n"
            "Uses exponential backoff with delays capped at 30s."
        )
    )

    perf_opts.add_argument(
        "--retry-count",
        type=int,
        default=2,
        metavar="COUNT",
        help=(
            "Number of retries for failed downloads.\n"
            "Default: 2 retries.\n"
            "Set to 0 to disable retries."
        )
    )

    perf_opts.add_argument(
        "--retry-delay",
        type=float,
        default=2.0,
        metavar="SECONDS",
        help=(
            "Delay between retries in seconds.\n"
            "Uses exponential backoff (doubles after each retry).\n"
            "Default: 2.0 seconds."
        )
    )

    # --- Input Source & Site Selection ---
    source_group = parser.add_argument_group(
        "Input Source & Site Selection",
        "Specify what to download and from which site. Choose ONE input method: URL, --input-file, or --favorites."
    )
    # Create mutually exclusive group for input methods
    source_mutex = source_group.add_mutually_exclusive_group(required=False) # required=False allows showing help without args

    # Add positional URL argument first
    source_mutex.add_argument(
        "url",
        nargs="?", # makes it optional
        help=(
            "Positional URL argument (optional). Can be used instead of --url.\n"
            "Specifies the target URL for downloading.\n"
            "Examples:\n"
            "  - User Profile: 'https://coomer.su/onlyfans/user/12345'\n"
            "  - Search: 'https://kemono.su/posts?q=search+term'\n"
            "  - Tag: 'https://coomer.su/posts?tag=artist_tag'\n"
            "  - Popular: 'https://kemono.su/posts/popular?period=week'\n"
            "The base site (coomer.su/kemono.su) is inferred from the URL.\n"
            "Conflicts with: --url, --input-file, --favorites."
        ),
        default=None,
        metavar="URL"
    )
    # Add flag-based input methods
    source_mutex.add_argument(
        "--url",
        dest="flag_url",  # Different destination to avoid conflict with positional arg
        metavar="URL",
        help=(
            "Flag-based URL argument. Use if you prefer flags or if the positional URL causes issues.\n"
            "Same functionality and examples as the positional URL argument.\n"
            "The base site (coomer.su/kemono.su) is inferred from the URL.\n"
            "Conflicts with: positional URL, --input-file, --favorites."
        )
    )
    # Add other input methods
    source_mutex.add_argument(
        "--input-file",
        metavar="FILE",
        help=(
            "Path to a text file containing URLs to download (one URL per line).\n"
            "Lines starting with '#' and empty lines are ignored.\n"
            "Each URL in the file will be processed individually.\n"
            "Requires --site if URLs in the file are relative or don't specify the domain.\n"
            "Conflicts with: positional URL, --url, --favorites."
        )
    )
    source_mutex.add_argument(
        "--favorites",
        action="store_true",
        help=(
            "Download posts from all your favorited artists on the specified site.\n"
            "Requires authentication (--login or --cookies) to access your favorites list.\n"
            "Requires --site to specify which site's favorites to fetch (e.g., --site coomer.su).\n"
            "Conflicts with: positional URL, --url, --input-file."
        )
    )

    # Add site selection (not part of mutex group, but related)
    source_group.add_argument(
        "--site",
        choices=['coomer.su', 'coomer.party', 'coomer.st', 'kemono.su', 'kemono.party', 'kemono.cr'],
        help=(
            "Specify the target site domain for API calls.\n"
            "Required when using --favorites or --input-file (if URLs in the file don't specify the domain).\n"
            "If using --url or the positional URL, the site is usually inferred automatically.\n"
            "Example: --site coomer.su"
        )
    )


    # --- Authentication ---
    auth_group = parser.add_argument_group(
        "Authentication (Optional, Choose One)",
        "Provide credentials if needed to access content (e.g., favorites, restricted posts)."
    )
    auth_mutex = auth_group.add_mutually_exclusive_group()
    auth_mutex.add_argument(
        "-ck", "--cookies",
        metavar="COOKIE_STRING",
        type=str,
        help=(
            "Provide browser cookies as a string to authenticate.\n"
            "Useful for accessing content that requires login without using username/password.\n"
            "Format: 'cookie1=value1; cookie2=value2' (semicolon or comma separated).\n"
            "See README for instructions on how to obtain cookie strings from your browser.\n"
            "Example: --cookies \"__ddg1_=abc; session=xyz\"\n"
            "Conflicts with: --login."
        )
    )
    auth_mutex.add_argument(
        "--login",
        action="store_true",
        help=(
            "Authenticate using your site username and password.\n"
            "Requires --username and --password arguments to be provided.\n"
            "The script will attempt to log in and use the session cookies for subsequent requests.\n"
            "Conflicts with: --cookies."
        )
    )
    auth_group.add_argument(
        "--username",
        metavar="USER",
        help="Your username for the site. Required if using --login."
    )
    auth_group.add_argument(
        "--password",
        metavar="PASS",
        help="Your password for the site. Required if using --login."
    )

    # --- Download Options ---
    download_opts = parser.add_argument_group(
        "Download Options",
        "Control how and where files are downloaded and named."
    )
    download_opts.add_argument(
        "-d", "--download-dir",
        default="./downloads",
        metavar="DIRECTORY",
        help=(
            "Specify the main directory where downloaded files will be stored.\n"
            "Subdirectories will be created within this directory based on the source (e.g., artist name, search term).\n"
            "Default: './downloads' (a folder named 'downloads' in the current directory)."
        )
    )
    download_opts.add_argument(
        "-p", "--post-ids",
        metavar="ID1,ID2,...",
        help=(
            "Download only specific posts from a user profile URL by providing their IDs.\n"
            "Provide a comma-separated list of post IDs.\n"
            "Example: --post-ids 12345,67890\n"
            "Only works when the main input is a user profile URL."
        )
    )
    download_opts.add_argument(
        "-n", "--only-new",
        action="store_true",
        help=(
            "Download only media files that are not already recorded in the profile's database.\n"
            "Checks the URL against the database for the specific profile/source being downloaded.\n"
            "By default, stops downloading for that profile/source as soon as the first existing file URL is encountered.\n"
            "Use --continue-existing to skip existing files and continue checking the rest of the posts."
        )
    )
    download_opts.add_argument(
        "-x", "--continue-existing",
        action="store_true",
        help=(
            "Modify the behavior of --only-new / -n.\n"
            "Instead of stopping when the first existing file URL is found, skip that file and continue checking subsequent posts for new files.\n"
            "Requires --only-new / -n to be active."
        )
    )
    download_opts.add_argument(
        "-k", "--verify-checksum",
        action="store_true",
        help=(
            "Verify the SHA256 checksum of downloaded files against the checksum stored in the database (if available).\n"
            "If a file exists locally but the checksum doesn't match the database record, or if the remote file size differs, it will be re-downloaded.\n"
            "Increases processing time slightly as it requires reading local files for checksumming."
        )
    )
    download_opts.add_argument(
        "-sv", "--sequential-videos",
        action="store_true",
        help=(
            "Force sequential download mode (one file at a time) specifically when downloading videos (-t videos).\n"
            "Overrides the general --download-mode setting for videos only.\n"
            "Can be helpful for very large video files or unstable connections where parallel downloads might fail."
        )
    )
    download_opts.add_argument(
        "-fn", "--file-naming-mode",
        type=int,
        default=0,
        choices=[0, 1, 2],
        metavar="MODE",
        help=(
            "Choose the pattern for naming downloaded files:\n"
            "  0: Use the original filename from the URL, adding an index if needed (e.g., 'original_1.jpg'). (Default)\n"
            "  1: Use the post title, attachment index, and a short hash of the URL (e.g., 'Post_Title_1_a1b2c3d4.mp4').\n"
            "  2: Use the post title, post ID, and attachment index (e.g., 'Post_Title - 12345_1.png').\n"
            "Note: Filenames are always sanitized to remove invalid characters."
        )
    )
    download_opts.add_argument(
        "--archive",
        choices=["zip", "tar"],
        metavar="TYPE",
        help=(
            "After successfully downloading files for a specific source (profile, search, etc.), create a compressed archive.\n"
            "Specify 'zip' for a .zip file or 'tar' for a .tar.gz file.\n"
            "The archive will be created in the main download directory, containing the subdirectory for that source.\n"
            "Example: --archive zip"
        )
    )

    # --- Filtering Options ---
    filter_opts = parser.add_argument_group(
        "Filtering Options",
        "Selectively download content based on type, date, or size."
    )

    filter_opts.add_argument(
        "-t", "--file-type",
        default="all",
        choices=["all", "images", "videos", "documents", "compressed", "others"],
        metavar="TYPE",
        help=(
            "Filter downloads to include only specific types of files.\n"
            "Available types:\n"
            "  - images: jpg, jpeg, png, gif, bmp, tiff\n"
            "  - videos: mp4, mkv, webm, mov, avi, flv, wmv, m4v\n"
            "  - documents: pdf, doc, docx, xls, xlsx, ppt, pptx\n"
            "  - compressed: zip, rar, 7z, tar, gz\n"
            "  - others: Any file extension not matching the above categories.\n"
            "  - all: Download all file types (Default).\n"
            "This filter is applied based on the file extension in the URL."
        )
    )

    filter_opts.add_argument(
        "--date-after",
        metavar="YYYY-MM-DD",
        help=(
            "Only download media from posts published strictly *after* this date.\n"
            "Format: YYYY-MM-DD (e.g., 2024-01-01).\n"
            "Uses the 'published' date associated with the post.\n"
            "Can be combined with --date-before to specify a date range.\n"
            "Note: The comparison is based on the date part only; time is ignored."
        )
    )

    filter_opts.add_argument(
        "--date-before",
        metavar="YYYY-MM-DD",
        help=(
            "Only download media from posts published strictly *before* this date.\n"
            "Format: YYYY-MM-DD (e.g., 2024-12-31).\n"
            "Uses the 'published' date associated with the post.\n"
            "Can be combined with --date-after to specify a date range.\n"
            "Note: The comparison is based on the date part only; time is ignored."
        )
    )

    filter_opts.add_argument(
        "--min-size",
        metavar="SIZE",
        help=(
            "Skip downloading files that are smaller than the specified size.\n"
            "Format: A number followed by a unit (K, M, G, T for Kilo-, Mega-, Giga-, Terabytes) or just bytes if no unit.\n"
            "Examples: '500K' (500 KB), '10M' (10 MB), '1G' (1 GB), '1024' (1024 bytes).\n"
            "Checks the 'Content-Length' header before starting the download. May not work if the server doesn't provide the size."
        )
    )

    filter_opts.add_argument(
        "--max-size",
        metavar="SIZE",
        help=(
            "Skip downloading files that are larger than the specified size.\n"
            "Format: A number followed by a unit (K, M, G, T for Kilo-, Mega-, Giga-, Terabytes) or just bytes if no unit.\n"
            "Examples: '100M' (100 MB), '2G' (2 GB).\n"
            "Checks the 'Content-Length' header before starting the download. May not work if the server doesn't provide the size."
        )
    )

    # --- Performance & Networking ---
    perf_opts = parser.add_argument_group(
        "Performance & Networking",
        "Adjust download speed, concurrency, and network settings."
    )

    perf_opts.add_argument(
        "-w", "--workers",
        type=int,
        default=5,
        metavar="NUM",
        help=(
            "Set the maximum number of parallel download threads.\n"
            "Higher values can lead to faster downloads but increase CPU/network usage and the risk of rate limiting.\n"
            "Default: 5.\n"
            "Recommended range: 3-10.\n"
            "Ignored if --download-mode is 'sequential'."
        )
    )

    perf_opts.add_argument(
        "-r", "--rate-limit",
        type=float,
        default=2.0,
        metavar="SECONDS",
        help=(
            "Minimum time interval (in seconds) between consecutive requests to the same domain.\n"
            "Helps prevent overwhelming the server and avoids potential IP bans or rate limits.\n"
            "Lower values might speed up fetching metadata but increase risk.\n"
            "Default: 2.0 seconds.\n"
            "Recommended range: 1.0 - 3.0 seconds."
        )
    )

    perf_opts.add_argument(
        "-c", "--concurrency",
        type=int,
        default=2,
        metavar="NUM",
        help=(
            "Maximum number of simultaneous connections allowed to the *same domain* at any given time.\n"
            "This is different from --workers, which controls the total number of download threads.\n"
            "Higher values might trigger anti-bot measures (like Cloudflare challenges).\n"
            "Default: 2.\n"
            "Recommended range: 2-4."
        )
    )

    perf_opts.add_argument(
        "-dm", "--download-mode",
        choices=["concurrent", "sequential"],
        default="concurrent",
        metavar="MODE",
        help=(
            "Choose the overall download strategy:\n"
            "  - concurrent: Download multiple files in parallel using multiple threads (controlled by --workers). Faster but more resource-intensive. (Default)\n"
            "  - sequential: Download files one after another in a single thread. Slower but more stable, especially for large files or unreliable networks.\n"
            "Note: --sequential-videos can override this setting specifically for video files."
        )
    )

    perf_opts.add_argument(
        "--proxy",
        metavar="PROXY_URL",
        help=(
            "Route all HTTP/HTTPS requests through the specified proxy server.\n"
            "Useful for bypassing network restrictions or masking your IP address.\n"
            "Supported formats:\n"
            "  - http://user:pass@host:port\n"
            "  - socks5://host:port\n"
            "  - socks5h://host:port (for DNS resolution via proxy)\n"
            "Example: --proxy http://127.0.0.1:8080"
        )
    )

    # --- Other Options ---
    other_opts = parser.add_argument_group(
        "Other Options",
        "Miscellaneous settings for logging, output, and execution control."
    )

    other_opts.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Perform a simulation without downloading any files.\n"
            "Fetches post data, applies filters, and shows a summary of what *would* be downloaded (file counts, sample names).\n"
            "Useful for testing filters, checking the scope of a download, or generating URL lists with --export-urls."
        )
    )

    other_opts.add_argument(
        "--export-urls",
        metavar="FILE",
        help=(
            "Save the list of media URLs that would be downloaded to a text file.\n"
            "Requires --dry-run to be active.\n"
            "The file will contain one URL per line, reflecting all applied filters.\n"
            "Useful for reviewing URLs before downloading or using them with other tools.\n"
            "Example: --export-urls my_download_list.txt"
        )
    )

    other_opts.add_argument(
        "--interactive",
        action="store_true",
        help=(
            "Launch an interactive command-line menu to guide you through setting up download options.\n"
            "(Experimental) May not support all features available via command-line flags.\n"
            "Helpful for beginners or for exploring available settings."
        )
    )

    other_opts.add_argument(
        "--speedtest",
        action="store_true",
        help=(
            "Perform a download speed test before any other actions and exit.\n"
            "Uses the speedtest-cli library."
        )
    )

    other_opts.add_argument(
        "-v", "--verbose",
        action="store_true",
        help=(
            "Enable verbose logging output.\n"
            "Shows detailed information about requests, responses, errors, and internal steps.\n"
            "Useful for debugging issues or understanding the script's behavior.\n"
            "Can generate a lot of output."
        )
    )

    # --- Argument Validation ---
    args = parser.parse_args()
    
    # Get URL from either positional argument or flag
    final_url = args.url or args.flag_url
    
    # Only show help if no arguments or explicit help request
    if len(sys.argv) <= 1 or any(arg in sys.argv for arg in ['-h', '--help']):
        parser.print_help()
        sys.exit(1)
        
    # Validate input source
    if not (final_url or getattr(args, 'input_file', None) or getattr(args, 'favorites', False)):
        parser.error(
            "You must provide an input source (URL, --input-file, or --favorites).\n"
            "Use --help for detailed descriptions of all options."
        )

    # Store the final URL value (either positional or from --url flag)
    args.url = final_url

    # Validation: --login requires --username and --password
    if args.login and (not args.username or not args.password):
        parser.error("--login requires --username and --password.")

    # Validation: --favorites requires auth and --site
    if args.favorites and not (args.login or args.cookies):
         parser.error("--favorites requires authentication (--login or --cookies).")
    if args.favorites and not args.site:
         parser.error("--favorites requires --site to be specified (e.g., --site coomer.su).")

    # Validation: --export-urls requires --dry-run
    if args.export_urls and not args.dry_run:
        parser.error("--export-urls can only be used with --dry-run.")

    # Validation: Ensure URL is provided if not using --input-file or --favorites
    # This is handled by the mutually exclusive group being required=True

    # If no arguments (or only prog name) are provided, print help.
    # The required=True on the mutex group handles the case where no source is specified.
    # We still might want help if only e.g. --verbose is given.
    # Check if only default/action args were effectively passed besides the source.
    # This logic might need refinement depending on how argparse handles defaults.
    # A simpler check: if only the program name is present.
    if len(sys.argv) == 1:
         parser.print_help()
         sys.exit(1)

    return args



# Global state
downloader: Optional[DownloaderCLI] = None

def handle_interrupt(sig: int, frame: Any) -> None:
    """Handle interrupt signal (Ctrl+C) gracefully."""
    if downloader:
        log("\nInterrupt received - stopping downloads...", logging.WARNING)
        downloader.request_cancel()
        downloader.close()
    sys.exit(1)

# Helper functions for new features

# This section seems correct based on the previous successful diff.
# No changes needed here based on the re-read.
# Keeping the existing improved login_to_site function.
def login_to_site(downloader: DownloaderCLI, base_site: str, username: str, password: str) -> bool:
    """
    Login to the site using username and password.
    Uses the provided base_site.
    Returns True if login successful, False otherwise.
    """
    if not base_site:
        log("Login failed: No base site provided", logging.ERROR)
        return False

    # Prepare login request
    login_url = f"{base_site}/api/v1/authentication/login"
    json_data = {
        "username": username,
        "password": password
    }

    try:
        log(f"Attempting login as {username}...")
        # Build proper headers for form submission
        login_headers = {
            **downloader.headers,
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Origin": base_site,
            "Host": urlparse(base_site).netloc,
            "Referer": f"{base_site}/login",
            "Upgrade-Insecure-Requests": "1"
        }

        # Make sure cookies are included if already present
        if downloader.session.cookies:
            cookie_string = '; '.join([f"{k}={v}" for k, v in downloader.session.cookies.items()])
            if cookie_string:
                login_headers['Cookie'] = cookie_string

        # Prepare JSON data
        json_data = {
            "username": username,
            "password": password
        }

        # Use proper JSON headers
        login_headers = {
            **downloader.headers,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Origin": base_site,
            "Host": urlparse(base_site).netloc,
            "Referer": f"{base_site}/login"
        }

        response = downloader.session.post(
            login_url,
            json=json_data,  # Use json parameter for proper JSON encoding
            headers=login_headers,
            allow_redirects=True,
            timeout=30.0
        )

        # Check response status - Expect 200 OK for successful API login
        if response.status_code == 200:
            try:
                user_data = response.json()
                log(f"Login successful - Response: {user_data}", logging.DEBUG)
            except requests.exceptions.JSONDecodeError:
                log("Login successful (no JSON response)", logging.DEBUG)
                log(f"Raw response: {response.text}", logging.DEBUG)
            
            log("Login successful - session cookies updated")
            return True
        else:
            # Handle non-200 status
            log(f"Login failed with status code: {response.status_code}", logging.ERROR)
            error_msg = "Unknown error"
            try:
                error_data = response.json()
                if isinstance(error_data, dict):
                    if 'error' in error_data and isinstance(error_data['error'], dict):
                        error_msg = error_data['error'].get('message', str(error_data))
                    else:
                        error_msg = str(error_data)
                else:
                    error_msg = str(error_data)
            except (requests.exceptions.JSONDecodeError, ValueError):
                error_msg = response.text if response.text else "No error message provided"
            
            log(f"Server error message: {error_msg[:500]}", logging.ERROR)
            return False

    except requests.exceptions.Timeout:
        log(f"Login request timed out after 30 seconds connecting to {login_url}.", logging.ERROR)
        log(traceback.format_exc(), logging.DEBUG)
        return False
    except requests.exceptions.ConnectionError as e:
        log(f"Login connection error to {login_url}: {e}", logging.ERROR)
        log("Please check your network connection, proxy settings, and if the site is reachable.", logging.ERROR)
        log(traceback.format_exc(), logging.DEBUG)
        return False
    except requests.exceptions.RequestException as e:
        # Catch other potential requests library errors (e.g., invalid URL, SSL errors)
        log(f"Login request failed for {login_url}: {e}", logging.ERROR)
        log(traceback.format_exc(), logging.DEBUG)
        return False
    except Exception as e:
        # Catch any other unexpected errors during the login process
        log(f"An unexpected error occurred during login to {login_url}: {e}", logging.ERROR)
        log(traceback.format_exc(), logging.DEBUG)
        return False
def logout_from_site(downloader: DownloaderCLI, base_site: str) -> None:
    """
    Logout from the site
    """
    if not base_site:
        log("Cannot logout - no base site provided", logging.WARNING)
        return

    logout_url = f"{base_site}/api/v1/authentication/logout"
    headers = {
        **downloader.headers,
        "Host": urlparse(base_site).netloc,
        "Origin": base_site,
        "Referer": f"{base_site}/artists"
    }
    
    try:
        response = downloader.session.post(
            logout_url,
            headers=headers,
            timeout=30.0
        )
        if response and response.ok:
            log("Logged out successfully")
        else:
            log("Logout request failed", logging.WARNING)
    except Exception as e:
        log(f"Logout error: {e}", logging.WARNING)
        log(traceback.format_exc(), logging.DEBUG)

def process_favorites(downloader: DownloaderCLI, base_site: str) -> List[Dict[str, Any]]:
    """
    Fetch and process favorite artists from the API
    Returns a list of formatted sources to download
    """
    favorites_url = f"{base_site}/api/v1/account/favorites?type=artist&?sort_by=last_imported&order=desc" # Changed from /v1/ to /api/v1/
    
    log("Fetching favorites list...")
    resp = downloader.safe_request(favorites_url, method="get", stream=False)
    
    if not resp or not resp.ok:
        status = resp.status_code if resp else 'No response'
        log(f"Failed to fetch favorites: {status}", logging.ERROR)
        return []
        
    try:
        favorites = resp.json()
        log(f"Found {len(favorites)} favorited artists")
        
        # Process favorites
        sources = []
        for fav in favorites:
            service = fav.get('service')
            user_id = fav.get('id')
            name = fav.get('name', user_id)
            
            if not service or not user_id:
                log(f"Invalid favorite data: {fav}", logging.WARNING)
                continue
                
            sources.append({
                'service': service,
                'user_id': user_id,
                'name': name,
                'url': f"{base_site}/{service}/user/{user_id}"
            })
            
        return sources
        
    except Exception as e:
        log(f"Error processing favorites: {e}", logging.ERROR)
        log(traceback.format_exc(), logging.DEBUG)
        return []

def read_input_file(file_path: str) -> List[str]:
    """Read URLs from a text file, skipping comments and empty lines."""
    if not os.path.exists(file_path):
        log(f"Input file not found: {file_path}", logging.ERROR)
        raise FileNotFoundError(f"No such file: {file_path}")
        
    urls = []
    try:
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line and not line.startswith('#'):
                    urls.append(line)
                    
        log(f"Read {len(urls)} URLs from {file_path}")
        return urls
        
    except Exception as e:
        log(f"Failed to read {file_path}: {e}", logging.ERROR)
        log(traceback.format_exc(), logging.DEBUG)
        raise ValueError(f"Could not process input file: {e}")

def parse_size(size_str: str) -> int:
    """
    Convert size string like '10M', '1G', '500K' to bytes
    """
    if not size_str:
        return 0
        
    size_str = size_str.upper()
    
    # Handle units
    multipliers = {
        'K': 1024,
        'M': 1024 * 1024,
        'G': 1024 * 1024 * 1024,
        'T': 1024 * 1024 * 1024 * 1024,
    }
    
    if size_str[-1] in multipliers:
        return int(float(size_str[:-1]) * multipliers[size_str[-1]])
    else:
        try:
            return int(size_str)
        except ValueError:
            log(f"Invalid size format: {size_str}", logging.WARNING)
            return 0

def apply_filters(media_tuples: List[MediaTuple], args, all_posts: List[Any]) -> List[MediaTuple]:
    """
    Apply date and size filters to media tuples
    """
    if not (args.date_after or args.date_before or args.min_size or args.max_size):
        return media_tuples  # No filters to apply
    
    # Create a lookup of post_id -> post for date filtering
    post_lookup = {str(post.get('id')): post for post in all_posts if 'id' in post}
    
    # Parse date filters if provided
    date_after = None
    date_before = None
    
    if args.date_after:
        try:
            date_after = time.strptime(args.date_after, "%Y-%m-%d")
        except ValueError:
            log(f"Invalid --date-after format: {args.date_after} (use YYYY-MM-DD)", logging.WARNING)
    
    if args.date_before:
        try:
            date_before = time.strptime(args.date_before, "%Y-%m-%d")
        except ValueError:
            log(f"Invalid --date-before format: {args.date_before} (use YYYY-MM-DD)", logging.WARNING)
    
    # Parse size filters
    min_size = parse_size(args.min_size) if args.min_size else None
    max_size = parse_size(args.max_size) if args.max_size else None
    
    filtered_media = []
    
    for media_url, post_id, post_title in media_tuples:
        # Apply date filters if applicable
        if (date_after or date_before) and post_id in post_lookup:
            post = post_lookup[post_id]
            post_date_str = post.get('published')
            
            if post_date_str:
                try:
                    # Try to parse the post date - format might vary
                    post_date = None
                    for fmt in ["%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"]:
                        try:
                            post_date = time.strptime(post_date_str.split('.')[0], fmt)
                            break
                        except ValueError:
                            continue
                    
                    if post_date:
                        # Apply date filters
                        if date_after and post_date < date_after:
                            continue  # Skip if post is before date_after
                        if date_before and post_date > date_before:
                            continue  # Skip if post is after date_before
                except Exception as e:
                    log(f"Failed to parse date '{post_date_str}': {e}", logging.DEBUG)
        
        # Apply size filters if applicable
        if min_size or max_size:
            # We need to do a HEAD request to get the file size
            try:
                remote_size = None
                resp = downloader.safe_request(media_url, method="head", stream=False)
                if resp and resp.ok and 'content-length' in resp.headers:
                    remote_size = int(resp.headers['content-length'])
                    
                    if min_size and remote_size < min_size:
                        log(f"Size too small: {os.path.basename(media_url)} ({remote_size} < {min_size})", logging.DEBUG)
                        continue
                    if max_size and remote_size > max_size:
                        log(f"Size too large: {os.path.basename(media_url)} ({remote_size} > {max_size})", logging.DEBUG)
                        continue
            except Exception as e:
                log(f"Failed to get size for {media_url}: {e}", logging.DEBUG)
        
        # If we get here, the media passed all filters
        filtered_media.append((media_url, post_id, post_title))
    
    log(f"Filters matched {len(filtered_media)} of {len(media_tuples)} files")
    return filtered_media

def perform_dry_run(downloader: DownloaderCLI, media_tuples: List[MediaTuple], export_path: Optional[str] = None) -> None:
    """
    Perform a dry run - display what would be downloaded without actually downloading
    Optionally export URLs to a file
    """
    log("\n=== DRY RUN MODE ===", logging.INFO)
    log("No files will be downloaded", logging.INFO)
    
    # Group by category
    categories = defaultdict(list)
    for url, post_id, post_title in media_tuples:
        cat = downloader.detect_file_category(url)
        categories[cat].append((url, post_id, post_title))
    
    # Print summary by category
    for cat, items in categories.items():
        log(f"\n{cat.title()}: {len(items)} files", logging.INFO)
        for i, (url, post_id, post_title) in enumerate(items[:5]):
            filename = downloader.generate_filename(url, post_id, post_title, i+1)
            log(f"  - {filename}", logging.INFO)
        if len(items) > 5:
            log(f"  ... and {len(items) - 5} more files", logging.INFO)
    
    # Export URLs if requested
    if export_path:
        try:
            with open(export_path, 'w') as f:
                for url, _, _ in media_tuples:
                    f.write(f"{url}\n")
            log(f"Exported {len(media_tuples)} URLs to {export_path}", logging.INFO)
        except Exception as e:
            log(f"Failed to export URLs: {e}", logging.ERROR)
            
    log("\n===================\n", logging.INFO)

def create_archive(downloader: DownloaderCLI, folder_path: str, archive_type: str) -> None:
    """
    Create a compressed archive (zip or tar) of the downloaded files
    """
    import shutil
    import datetime
    
    if archive_type not in ['zip', 'tar']:
        log(f"Unsupported archive type: {archive_type}", logging.WARNING)
        return
    
    try:
        # Generate archive name
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        basename = os.path.basename(folder_path.rstrip('/'))
        archive_name = f"{basename}_{timestamp}.{archive_type}"
        archive_path = os.path.join(os.path.dirname(folder_path), archive_name)
        
        log(f"Creating {archive_type} archive: {archive_name}")
        
        if archive_type == 'zip':
            # Create zip archive
            shutil.make_archive(
                os.path.splitext(archive_path)[0],  # base name without extension
                'zip',                              # format
                folder_path                         # root dir
            )
        else:  # tar
            # Create tar.gz archive
            shutil.make_archive(
                os.path.splitext(archive_path)[0],  # base name without extension
                'gztar',                            # format
                folder_path                         # root dir
            )
            # Rename to match expected filename
            os.rename(f"{os.path.splitext(archive_path)[0]}.tar.gz", archive_path)
        
        log(f"Archive created: {archive_name}")
    except Exception as e:
        log(f"Failed to create archive: {e}", logging.ERROR)
        log(traceback.format_exc(), logging.DEBUG)

def process_url(downloader: DownloaderCLI, base_site: str, url: str, args) -> None:
    """
    Process a single URL and download media from it
    """
    parsed_url = urlparse(url)
    path_parts = [p for p in parsed_url.path.strip('/').split('/') if p]
    query_params = dict(parse_qsl(parsed_url.query))

    # Handle popular posts
    if path_parts and path_parts[0] == 'posts' and len(path_parts) > 1 and path_parts[1] == 'popular':
        date = query_params.get('date')
        period = query_params.get('period')
        all_posts = downloader.fetch_popular_posts(base_site, date, period)
        if not all_posts:
            log("No popular posts found")
            return
        # Add site name and handle popularizer if available
        site_name = urlparse(base_site).netloc.split('.')[0]  # get coomer or kemono
        folder_name = f"{site_name}_popular"
        if date: folder_name += f"_{date}"
        if period: folder_name += f"_{period}"
        media_tuples = downloader.extract_media(all_posts, args.file_type, base_site)
        log(f"Found {len(media_tuples)} media URLs")
    
    # Handle search query
    elif 'q' in query_params:
        query = query_params['q']
        all_posts = downloader.fetch_search_posts(base_site, query)
        if not all_posts:
            log(f"No posts found matching query: {query}", logging.INFO)
            return
        
        # Use site - term format for folder name
        site_name = urlparse(base_site).netloc.split('.')[0]  # get coomer or kemono
        folder_name = f"{site_name} - {query.replace(' ', '_').replace('/', '_')[:30]}"
        
        # Override generate_filename method temporarily for this download
        original_generate = downloader.generate_filename
        def custom_filename(url: str, post_id: Optional[Any], post_title: Optional[str], attachment_index: int) -> str:
            ext = os.path.splitext(url)[1]
            username = "unknown"
            if isinstance(post_title, str):
                username = post_title.split(' - ')[0]
            return f"{post_id} - {username} - {attachment_index}{ext}"
        
        downloader.generate_filename = custom_filename
        media_tuples = downloader.extract_media(all_posts, args.file_type, base_site)
    
    # Handle tag-based search
    elif 'tag' in query_params:
        tag = query_params['tag']
        all_posts = downloader.fetch_tag_posts(base_site, tag)
        if not all_posts:
            log(f"No posts found with tag '{tag}'")
            return
        
        # Use site - term format for folder name
        site_name = urlparse(base_site).netloc.split('.')[0]
        folder_name = f"{site_name} - {tag.replace(' ', '_').replace('/', '_')[:30]}"
        
        # Use the same custom filename format as search results
        custom_filename = downloader.generate_filename  # Use existing custom function from search
        media_tuples = downloader.extract_media(all_posts, args.file_type, base_site)
    
    # Handle user/service based URL
    else:
        if len(path_parts) < 2:
            raise ValueError(f"Could not parse service/user_id from URL: {url}")
        
        service = path_parts[0]
        user_id = path_parts[2] if (len(path_parts) >= 3 and path_parts[1] == 'user') else path_parts[1]
        
        username = downloader.fetch_username(base_site, service, user_id)
        
        # Restore original filename generator for user profiles
        if 'original_generate' in locals():
            downloader.generate_filename = original_generate
        
        # Use format without site prefix: "username - service"
        folder_name = downloader.sanitize_filename(f"{username} - {service}")
        
        all_posts = downloader.fetch_posts(base_site, user_id, service)  # always fetches entire profile by default
        if not all_posts:
            log(f"No posts found for user {user_id} on {service}")
            return
        
        if args.post_ids:
            post_ids = [pid.strip() for pid in args.post_ids.split(',')]
            posts_by_id = {str(p.get('id')): p for p in all_posts}
            media_tuples = []
            for pid in post_ids:
                post = posts_by_id.get(pid)
                if not post:
                    log(f"Post ID not found: {pid}", logging.WARNING)
                else:
                    media_tuples.extend(downloader.extract_media([post], args.file_type, base_site))
        else:
            media_tuples = downloader.extract_media(all_posts, args.file_type, base_site)
    
    # Apply filters if needed
    if args.date_after or args.date_before or args.min_size or args.max_size:
        media_tuples = apply_filters(media_tuples, args, all_posts)
    
    # Restore original filename generator if it was overridden
    if 'original_generate' in locals():
        downloader.generate_filename = original_generate
        
    # Restore original filename generator if it was changed
    if 'original_generate' in locals():
        downloader.generate_filename = original_generate
    
    # Handle dry run if requested
    if args.dry_run:
        perform_dry_run(downloader, media_tuples, args.export_urls)
        return
    
    # Use folder name without site prefix
    folder_name = folder_name
    
    # Download the media
    if not media_tuples:
        log("No media to download after applying filters.", logging.INFO)
        return
        
    log(f"Starting download of {len(media_tuples)} files to folder: {folder_name}", logging.INFO)
    
    if args.only_new:
        downloader.download_only_new_posts(media_tuples, folder_name, file_type=args.file_type)
    else:
        log("Starting concurrent downloads...", logging.INFO)
        downloader.download_media(media_tuples, folder_name, file_type=args.file_type)
        log("Download session completed", logging.INFO)
    
    # Create archive if requested
    if args.archive:
        create_archive(downloader, os.path.join(downloader.download_folder, folder_name), args.archive)

def process_source(downloader: DownloaderCLI, base_site: str, source_info: Dict[str, Any], args) -> None:
    """
    Process a single source (e.g., a favorite artist)
    """
    service = source_info['service']
    user_id = source_info['user_id']
    name = source_info['name']
    
    log(f"Processing artist: {name} ({service}/user/{user_id})")
    folder_name = downloader.sanitize_filename(f"{name[:30]} - {service}")
    
    try:
        log(f"Fetching posts for {name}...")
        all_posts = downloader.fetch_posts(base_site, user_id, service)
        
        if not all_posts:
            log(f"No posts found for {name}")
            return
        
        log(f"Found {len(all_posts)} posts - extracting media")
        media_tuples = downloader.extract_media(all_posts, args.file_type, base_site)
        
        # Apply any filters
        if args.date_after or args.date_before or args.min_size or args.max_size:
            media_tuples = apply_filters(media_tuples, args, all_posts)
        
        # Handle dry run
        if args.dry_run:
            perform_dry_run(downloader, media_tuples, args.export_urls)
            return
        
        if not media_tuples:
            log(f"No media to download for {name} after filtering", logging.INFO)
            return
            
        log(f"Starting download of {len(media_tuples)} files for {name}", logging.INFO)
        log("Initializing download threads...", logging.INFO)
        
        # Download based on mode
        if args.only_new:
            downloader.download_only_new_posts(media_tuples, folder_name, file_type=args.file_type)
        else:
            downloader.download_media(media_tuples, folder_name, file_type=args.file_type)
        
        # Create archive if requested
        if args.archive:
            create_archive(downloader, os.path.join(downloader.download_folder, folder_name), args.archive)
            
    except Exception as e:
        log(f"Failed to process {name}: {e}", logging.ERROR)
        log(f"Full error: {traceback.format_exc()}", logging.DEBUG)

def interactive_menu():
    """
    Interactive CLI menu for selecting download options
    This is a placeholder for future implementation
    """
    print("\n=== Interactive Mode ===")
    print("Note: Interactive mode is experimental and not fully implemented yet.")
    print("For now, we'll use default values and command line arguments.\n")
    
    # Simple text-based menu could be implemented here
    # For now, just return the parsed command line args with interactive flag removed
    parser = create_arg_parser()
    args = parser.parse_args()
    args.interactive = False  # Disable interactive flag to avoid recursion
    return args

def main() -> None:
    global downloader # allow modification by signal handler

    # --- Argument Parsing & Setup ---
    # Handle interactive mode separately if needed
    if len(sys.argv) > 1 and '--interactive' in sys.argv:
        args = interactive_menu()
    else:
        args = create_arg_parser()

    # Configure logging verbosity
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        log("Debug logging enabled", logging.DEBUG)
    else:
        # Suppress verbose logs from libraries
        for logger_name in ['requests', 'urllib3', 'chardet', 'charset_normalizer']:
            logging.getLogger(logger_name).setLevel(logging.WARNING)

    # --- Perform Speed Test if requested ---
    if args.speedtest:
        try:
            log("Starting network speed test...")
            st = speedtest.Speedtest()
            
            log("Finding optimal server...")
            st.get_best_server()
            
            log("Testing download speed...")
            st.download()
            
            log("Testing upload speed...")
            st.upload()
            
            results = st.results.dict()
            server = results['server']
            
            # Convert to human readable values
            down_mbps = results["download"] / 1_000_000
            up_mbps = results["upload"] / 1_000_000
            ping_ms = results["ping"]
            
            # Print results
            log("\n=== Speed Test Results ===")
            log(f"Server: {server['name']} ({server['sponsor']})")
            log(f"Location: {server['country']}")
            log(f"Ping: {ping_ms:.1f} ms")
            log(f"Download: {down_mbps:.1f} Mbps")
            log(f"Upload: {up_mbps:.1f} Mbps")
            log("=======================\n")
            sys.exit(0)
            
        except speedtest.SpeedtestException as e:
            log(f"Speed test failed: {e}", logging.ERROR)
            sys.exit(1)
            
        except Exception as e:
            log(f"Unexpected error during speed test: {e}", logging.ERROR)
            log(traceback.format_exc(), logging.DEBUG)
            sys.exit(1)

    # Set up signal handlers
    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_interrupt)

    # Handle terminal resize on Unix-like systems
    if hasattr(signal, 'SIGWINCH'):
        def handle_resize(signum: int, frame: Any) -> None:
            """Refresh progress bars when terminal size changes."""
            if downloader and hasattr(downloader, '_active_bars'):
                try:
                    for bar in downloader._active_bars:
                        bar.refresh()
                except:
                    pass
        
        signal.signal(signal.SIGWINCH, handle_resize)

    # --- Initialize Downloader ---
    # Determine download mode, considering --sequential-videos override
    download_mode = args.download_mode
    if args.sequential_videos and args.file_type == "videos":
        download_mode = "sequential"
        log("Using sequential mode for video downloads", logging.INFO)

    try:
        # Create the downloader with basic options
        downloader = DownloaderCLI(
            download_folder=args.download_dir,
            max_workers=args.workers,
            rate_limit_interval=args.rate_limit,
            domain_concurrency=args.concurrency,
            retry_count=args.retry_count,
            retry_delay=args.retry_delay,
            verify_checksum=args.verify_checksum,
            only_new_stop=(not args.continue_existing),
            download_mode=download_mode,
            file_naming_mode=args.file_naming_mode,
            cookie_string=args.cookies if args.cookies else None
        )
        
        # Set retry behavior based on command line arguments
        # Default to retry-at-end unless retry-immediately is explicitly set
        if args.retry_immediately:
            downloader.retry_immediately = True
        else:
            downloader.retry_immediately = False
        
        # Set up proxy if specified
        if args.proxy:
            proxy_url = args.proxy
            try:
                parsed = urlparse(proxy_url)
                if not parsed.scheme or not parsed.netloc:
                    raise ValueError("Invalid proxy URL format")
                    
                downloader.session.proxies = {
                    'http': proxy_url,
                    'https': proxy_url
                }
                log(f"Using proxy server: {proxy_url}")
                
            except Exception as e:
                log(f"Invalid proxy configuration: {e}", logging.ERROR)
                log("Expected format: scheme://host:port (e.g., http://127.0.0.1:8080)", logging.ERROR)
                sys.exit(1)

        # --- Determine Base Site (Needed for API calls) ---
        base_site = None
        supported_domains = ['coomer.su', 'coomer.party', 'coomer.st', 'kemono.su', 'kemono.party', 'kemono.cr']

        # Get URL from either positional argument or flag
        url = None
        if hasattr(args, 'url') and args.url:
            url = args.url
        elif hasattr(args, 'url_flag') and args.url_flag:
            url = args.url_flag
            
        if url:
            try:
                parsed_url = urlparse(url)
                site_domain = parsed_url.netloc.lower()
                if not any(domain == site_domain for domain in supported_domains):
                    raise ValueError(f"Unsupported domain: {site_domain}")
                    
                base_site = f"https://{site_domain}"
                log(f"Using site from URL: {base_site}", logging.INFO)
                
            except Exception as e:
                log(f"Invalid URL format: {url}", logging.ERROR)
                log(f"Error details: {e}", logging.DEBUG)
                raise ValueError("Please provide a valid URL")
                
        elif args.site:
            if args.site not in supported_domains:
                log(f"Unsupported site: {args.site}", logging.ERROR)
                log(f"Supported sites: {', '.join(supported_domains)}", logging.ERROR)
                raise ValueError("Invalid site specified")
                
            base_site = f"https://{args.site}"
            log(f"Using specified site: {base_site}", logging.INFO)
            
        elif args.input_file:
            log("Processing URLs from input file (site determined per URL)", logging.INFO)
            base_site = None
        else:
             # This case should ideally not be reached due to argparse validation
             # (e.g., --favorites requires --site)
             raise ValueError("Cannot determine target site. Please provide --url or use --site with --favorites/--input-file.")

        # --- Authentication ---
        logged_in_session = False
        if args.login:
            log(f"Authenticating as {args.username}...")
            if not base_site:
                log("Login failed - no target site specified (use --url or --site)", logging.ERROR)
                sys.exit(1)
            success = login_to_site(downloader, base_site, args.username, args.password)
            if success:
                log("Successfully logged in")
                logged_in_session = True
            else:
                log("Authentication failed - check your credentials", logging.ERROR)
                sys.exit(1)
        elif args.cookies:
            # Session cookies initialized in __init__
            log("Using cookie-based authentication", logging.DEBUG)
        # Note: Authentication is optional unless using --favorites

        # --- Process Input Sources ---
        if args.favorites:
            log("Fetching favorites list...", logging.INFO)
            media_sources = process_favorites(downloader, base_site)
            if not media_sources:
                log("No favorites found - check your authentication", logging.ERROR)
                sys.exit(1)

            log(f"Found {len(media_sources)} favorites to process", logging.INFO)
            for source_info in media_sources:
                process_source(downloader, base_site, source_info, args)

        elif args.input_file:
            log(f"Reading URLs from: {args.input_file}", logging.INFO)
            urls = read_input_file(args.input_file)
            log(f"Found {len(urls)} URLs to process", logging.INFO)
            
            for url in urls:
                try:
                    log(f"Processing: {url}", logging.INFO)
                    parsed = urlparse(url)
                    site = parsed.netloc.lower()
                    
                    # Validate domain
                    if not any(domain in site for domain in ['coomer.su', 'coomer.party', 'coomer.st', 'kemono.su', 'kemono.party', 'kemono.cr']):
                        log(f"Skipping unsupported site: {site}", logging.WARNING)
                        continue
                    
                    # Process with site-specific base URL
                    current_base = f"https://{site}"
                    process_url(downloader, current_base, url, args)
                    
                except Exception as e:
                    log(f"Failed to process {url}: {e}", logging.ERROR)
                    log(traceback.format_exc(), logging.DEBUG)
                    log("Continuing with next URL...")

        elif args.url:
            log(f"Processing URL: {args.url}", logging.INFO)
            process_url(downloader, base_site, args.url, args)

        else:
             # This case is prevented by argparse validation
             log("No valid input source specified", logging.ERROR)
             sys.exit(1)

        # Final cleanup - try to logout if needed
        if logged_in_session and args.login:
            try:
                log("Ending session...", logging.INFO)
                logout_from_site(downloader, base_site)
            except Exception as e:
                log(f"Warning: Session cleanup failed - {e}", logging.WARNING)
                log(traceback.format_exc(), logging.DEBUG)

    except sqlite3.OperationalError as e:
        log(f"Database error: {e}", logging.ERROR)
        log(traceback.format_exc(), logging.DEBUG)
        sys.exit(1)
        
    except requests.exceptions.ConnectionError as e:
        log(f"Connection failed - check your internet connection", logging.ERROR)
        log(f"Error details: {e}", logging.DEBUG)
        log(traceback.format_exc(), logging.DEBUG)
        sys.exit(1)
        
    except requests.exceptions.Timeout as e:
        log("Request timed out - site may be slow or unresponsive", logging.ERROR)
        log(f"Error details: {e}", logging.DEBUG)
        log(traceback.format_exc(), logging.DEBUG)
        sys.exit(1)
        
    except ValueError as e:
        log(f"Invalid configuration or URL: {e}", logging.ERROR)
        log(traceback.format_exc(), logging.DEBUG)
        sys.exit(1)
        
    except Exception as e:
        log(f"Unexpected error: {e}", logging.ERROR)
        log(traceback.format_exc(), logging.DEBUG)
        if downloader:
            downloader.request_cancel()
        sys.exit(1)
        
    finally:
        if downloader:
            downloader.close()
        log("Script finished", logging.INFO)


if __name__ == "__main__":
    main()
