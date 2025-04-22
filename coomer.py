#!/usr/bin/env python3
import os
import re
import sys
import time
import signal
import threading
import logging
import argparse
import sqlite3
import requests
import hashlib
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin, quote_plus, parse_qsl
from tqdm import tqdm
from requests.adapters import HTTPAdapter, Retry
from typing import Any, Dict, List, Optional, Tuple

# Configure logging; default to INFO (can be changed with --verbose)
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# Alias for media tuple: (media_url, post_id, post_title)
MediaTuple = Tuple[str, Optional[Any], Optional[str]]


class DownloaderCLI:
    def __init__(
        self,
        download_folder: str,
        max_workers: int = 5,
        rate_limit_interval: float = 2.0,
        domain_concurrency: int = 2,
        verify_checksum: bool = False,
        only_new_stop: bool = True,
        download_mode: str = 'concurrent',  # "concurrent" for parallel downloads, "sequential" for sequential download
        file_naming_mode: int = 0
    ) -> None:
        """
        Initialize DownloaderCLI.

        :param download_folder: Directory where downloads will be stored.
        :param max_workers: Maximum number of threads for concurrent downloads.
        :param rate_limit_interval: Minimum interval between requests to the same domain.
        :param domain_concurrency: Maximum number of concurrent requests per domain.
        :param verify_checksum: If True, SHA256 checksums of files will be calculated and verified.
        :param only_new_stop: In "only new" mode, stops at the first existing file (True) or skips it (False).
        :param download_mode: 'concurrent' for parallel downloads or 'sequential' for sequential download.
        :param file_naming_mode: 0 = original name + index, 1 = post title + index + short MD5 hash, 2 = post title - post_id + index.
        """
        self.download_folder: str = download_folder
        self.max_workers: int = max_workers
        self.rate_limit_interval: float = rate_limit_interval
        self.domain_concurrency: int = domain_concurrency
        self.verify_checksum: bool = verify_checksum
        self.only_new_stop: bool = only_new_stop
        self.download_mode: str = download_mode  # "concurrent" or "sequential"
        self.file_naming_mode: int = file_naming_mode

        # Configure requests session with a retry adapter
        self.session: requests.Session = requests.Session()
        retries = Retry(
            total=10,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        self.max_retries: int = 5  # fallback value if needed

        # Select ThreadPoolExecutor based on download mode
        if self.download_mode == 'sequential':
            self.executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=1)
        else:
            self.executor = ThreadPoolExecutor(max_workers=self.max_workers)

        self.cancel_requested: threading.Event = threading.Event()
        self.domain_last_request: Dict[str, float] = defaultdict(float)
        self.domain_locks: Dict[str, threading.Semaphore] = defaultdict(lambda: threading.Semaphore(domain_concurrency))

        # Database-related attributes
        self.db_conn: Optional[sqlite3.Connection] = None
        self.db_cursor: Optional[sqlite3.Cursor] = None
        self.db_lock: threading.Lock = threading.Lock()
        self.download_cache: Dict[str, Tuple[str, int, Optional[str]]] = {}  # url -> (file_path, file_size, checksum)
        self.current_profile: Optional[str] = None

        self.headers: Dict[str, str] = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/127.0.0.0 Safari/537.36"
            ),
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "priority": "u=0, i",
            "sec-ch-ua": '"Not:A-Brand";v="24", "Chromium";v="134"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1"
        }

        # Cookie header will be added later if provided through command line
        self.cookie_header: Optional[str] = None

        self.file_extensions: Dict[str, Tuple[str, ...]] = {
            'images': ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff'),
            'videos': ('.mp4', '.mkv', '.webm', '.mov', '.avi', '.flv', '.wmv', '.m4v'),
            'documents': ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx'),
            'compressed': ('.zip', '.rar', '.7z', '.tar', '.gz'),
        }

        # Precompile regex to sanitize file names.
        self._filename_sanitize_re = re.compile(r'[<>:"/\\|?*]')

    def init_profile_database(self, profile_name: str) -> None:
        """Initialize a database for the specified profile."""
        if self.db_conn:
            self.db_conn.close()
        self.current_profile = profile_name
        db_path = os.path.join(self.download_folder, f"{profile_name}.db")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        try:
            # try increasing timeout slightly for db operations
            self.db_conn = sqlite3.connect(db_path, timeout=10.0, check_same_thread=False)
            self.db_cursor = self.db_conn.cursor()
            self._init_database_schema()
            self._load_download_cache()
            logger.info(f"Initialized database for profile: {profile_name}")
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logger.error(
                    f"Database error for profile '{profile_name}': {e}. "
                    "This usually means another coomer.py process is running or has locked the database file. "
                    "Please close any other instances and try again. If the problem persists, check file permissions for '{db_path}'.",
                )
            else:
                logger.error(f"Unexpected database error for profile '{profile_name}': {e}")
            raise # re-raise the exception after logging

    def _init_database_schema(self) -> None:
        """create the database tables if they do not exist."""
        assert self.db_cursor is not None
        try:
            # these pragmas help with concurrency but can still lock sometimes
            self.db_cursor.execute("PRAGMA journal_mode=WAL;")
            self.db_cursor.execute("PRAGMA synchronous=NORMAL;")
            self.db_cursor.execute("PRAGMA cache_size=-2000;") # use more memory for cache
            self.db_cursor.execute("""
                CREATE TABLE IF NOT EXISTS downloads (
                    url TEXT PRIMARY KEY,
                file_path TEXT NOT NULL,
                file_size INTEGER,
                checksum TEXT,
                downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
            self.db_conn.commit()
        except sqlite3.OperationalError as e:
            # catch lock errors during initial setup too
            if "database is locked" in str(e):
                 logger.error(
                    f"Database setup error: {e}. "
                    "Another process might be accessing the database. Please ensure no other coomer.py instances are running."
                 )
            else:
                 logger.error(f"Unexpected database setup error: {e}")
            raise

    def _load_download_cache(self) -> None:
        """Load downloaded files from the database into memory."""
        assert self.db_cursor is not None
        self.db_cursor.execute("SELECT url, file_path, file_size, checksum FROM downloads")
        self.download_cache = {row[0]: (row[1], row[2], row[3]) for row in self.db_cursor.fetchall()}

    def _record_download(self, url: str, file_path: str, file_size: int, checksum: Optional[str] = None) -> None:
        """Record the download of a file in the database."""
        with self.db_lock:
            try:
                assert self.db_cursor is not None
                self.db_cursor.execute(
                    "INSERT OR REPLACE INTO downloads (url, file_path, file_size, checksum) VALUES (?, ?, ?, ?)",
                    (url, file_path, file_size, checksum)
                )
                self.db_conn.commit()
                self.download_cache[url] = (file_path, file_size, checksum)
            except Exception as e:
                logger.exception(f"Error recording download for {url}: {e}")

    def log(self, msg: str, level: int = logging.INFO) -> None:
        logger.log(level, msg)

    def request_cancel(self) -> None:
        self.log("Cancellation requested.", logging.WARNING)
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
        extra_headers: Optional[Dict[str, str]] = None,
        timeout: float = 30.0
    ) -> Optional[requests.Response]:
        """
        Make a safe HTTP request with optimized rate limiting and retry handling.
        """
        if self.cancel_requested.is_set():
            return None

        req_headers = self.headers.copy()
        if extra_headers:
            req_headers.update(extra_headers)

        domain = urlparse(url).netloc
        with self.domain_locks[domain]:
            elapsed = time.time() - self.domain_last_request[domain]
            # Adjust rate limiting based on request type
            if method.lower() == "get" and stream:
                # Streaming requests (like file downloads) use full rate limit
                wait_time = self.rate_limit_interval
            else:
                # API requests use a shorter interval
                wait_time = self.rate_limit_interval / 2

            if elapsed < wait_time:
                time.sleep(wait_time - elapsed)

            try:
                resp = self.session.request(
                    method,
                    url,
                    headers=req_headers,
                    stream=stream,
                    allow_redirects=True,
                    timeout=timeout
                )
                resp.raise_for_status()
                self.domain_last_request[domain] = time.time()
                # Reset 403 counter on successful request
                if hasattr(self, '_403_counter'):
                    self._403_counter = 0
                return resp
            except requests.exceptions.HTTPError as e:
                # handle specific http errors first
                if e.response.status_code == 429:
                    # handle rate limiting response
                    retry_after = int(e.response.headers.get('Retry-After', 5))
                    self.log(f"Rate limited by server for {url}. Waiting {retry_after} seconds...", logging.WARNING)
                    time.sleep(retry_after)
                    # retry the request after waiting
                    return self.safe_request(url, method, stream, extra_headers, timeout)
                elif e.response.status_code == 403:
                    # handle forbidden errors - often needs cookies
                    if not hasattr(self, '_403_counter'):
                        self._403_counter = 0
                    self._403_counter += 1
                    
                    # suggest cookies after the first 403
                    if self._403_counter >= 1: # changed from 3 to 1 for earlier suggestion
                         self.log(
                             f"\n[!] Received 403 Forbidden error for {url}.\n"
                             "    This often means the content requires login/authentication.\n"
                             "    Try providing your browser cookies using the --cookies argument.\n"
                             '    Example: --cookies "__ddg1_=abc123;__ddg2_=xyz789"\n'
                             "    See README for instructions on how to get cookies.",
                             logging.ERROR # make it stand out more
                         )
                    else:
                         # log less severe message for initial 403s if threshold > 1
                         self.log(f"Access denied (403 Forbidden) for {url}", logging.WARNING)
                    # dont retry 403 automatically, user needs to fix it (usually with cookies)
                    return None
                else:
                    # other http errors
                    self.log(f"HTTP Error {e.response.status_code} requesting {url}: {e}", logging.ERROR)
                    self.log(traceback.format_exc(), logging.DEBUG)
                    return None
            except requests.exceptions.ConnectionError as e:
                # handle network connection problems
                self.log(f"Connection Error requesting {url}: {e}", logging.ERROR)
                self.log("Could not connect to the server. Check your internet connection, DNS, or firewall.", logging.ERROR)
                self.log(traceback.format_exc(), logging.DEBUG)
                # might be temporary, let the main retry logic handle it if configured, otherwise fail
                return None
            except requests.exceptions.Timeout as e:
                 # handle request timeouts
                self.log(f"Timeout Error requesting {url}: {e}", logging.ERROR)
                self.log("The request took too long. The server might be slow or your connection unstable.", logging.ERROR)
                self.log(traceback.format_exc(), logging.DEBUG)
                # might be temporary, let the main retry logic handle it
                return None
            except requests.exceptions.RequestException as e:
                # catch any other requests-related errors
                self.log(f"General Error requesting {url}: {e}", logging.ERROR)
                self.log(traceback.format_exc(), logging.DEBUG)
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

    def _write_file(self, resp: requests.Response, final_path: str, total_size: Optional[int],
                    hasher: Optional[hashlib._hashlib.HASH]) -> Optional[str]:
        """
        Write the file in chunks to a temporary file, then rename it.
        If cancelled, remove the partial file.
        Returns the checksum (in hexadecimal) if a hasher is provided.
        """
        tmp_path = final_path + ".tmp"
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

        desc_for_tqdm = os.path.basename(final_path)
        try:
            with open(tmp_path, 'wb') as f, tqdm(
                total=total_size,
                initial=0,
                unit='B',
                unit_scale=True,
                desc=desc_for_tqdm
            ) as pbar:
                for chunk in resp.iter_content(chunk_size=8192):
                    if self.cancel_requested.is_set():
                        f.close()
                        os.remove(tmp_path)
                        self.log(f"Cancelled and removed partial file: {tmp_path}", logging.WARNING)
                        return None
                    f.write(chunk)
                    if hasher:
                        hasher.update(chunk)
                    pbar.update(len(chunk))
            os.rename(tmp_path, final_path)
            return hasher.hexdigest() if hasher else None
        except Exception as e:
            self.log(f"Error writing file {final_path}: {e}", logging.ERROR)
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            return None

    def fetch_username(self, base_site: str, service: str, user_id: str) -> str:
        """Fetch the username for the profile (used for naming the folder)."""
        profile_url = f"{base_site}/api/v1/{service}/user/{user_id}/profile"
        resp = self.safe_request(profile_url, method="get", stream=False)
        try:
            return resp.json().get("name", user_id) if resp else user_id
        except Exception:
            return user_id

    def get_remote_file_size(self, url: str) -> Optional[int]:
        resp = self.safe_request(url, method="head", stream=False)
        if resp and resp.ok and 'content-length' in resp.headers:
            try:
                return int(resp.headers['content-length'])
            except ValueError:
                return None
        return None

    def download_file(
        self,
        url: str,
        folder: str,
        post_id: Optional[Any] = None,
        post_title: Optional[str] = None,
        attachment_index: int = 1
    ) -> bool:
        """Download a single file. Returns True if download was successful."""
        if self.cancel_requested.is_set():
            return False

        remote_size = self.get_remote_file_size(url)
        if url in self.download_cache:
            file_path, cached_size, cached_checksum = self.download_cache[url]
            if not os.path.exists(file_path):
                self.log(f"File missing from disk: {file_path}. Redownloading...", logging.WARNING)
            else:
                size_mismatch = (remote_size is not None and cached_size != remote_size)
                checksum_mismatch = False
                if self.verify_checksum and os.path.exists(file_path):
                    local_checksum = self.compute_checksum(file_path)
                    checksum_mismatch = (cached_checksum != local_checksum)
                if not size_mismatch and not checksum_mismatch:
                    self.log(f"File already downloaded: {os.path.basename(file_path)}")
                    return True
                else:
                    self.log(
                        f"File mismatch for {os.path.basename(file_path)}. Cached: {cached_size}, remote: {remote_size}.",
                        logging.INFO
                    )

        os.makedirs(folder, exist_ok=True)
        filename = self.generate_filename(url, post_id, post_title, attachment_index)
        final_path = os.path.join(folder, filename)

        if remote_size is not None:
            self.log(f"Remote size for {filename}: {remote_size} bytes")
        else:
            self.log(f"No remote size for {filename} (Content-Length not provided).")
        self.log(f"Starting download for: {filename}")

        resp = self.safe_request(url)
        if not resp:
            self.log(f"Failed to download after retries: {filename}", logging.ERROR)
            return False

        sha256 = hashlib.sha256() if self.verify_checksum else None
        checksum = self._write_file(resp, final_path, remote_size, sha256)
        if checksum is None and self.verify_checksum:
            self.log(f"Download failed or checksum error for: {filename}", logging.ERROR)
            return False
        try:
            final_size = os.path.getsize(final_path)
        except Exception as e:
            self.log(f"Error getting file size for {final_path}: {e}", logging.ERROR)
            return False
        self._record_download(url, final_path, final_size, checksum)
        return True

    def compute_checksum(self, file_path: str) -> Optional[str]:
        """Compute the SHA256 checksum of a file."""
        sha256 = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    sha256.update(chunk)
            return sha256.hexdigest()
        except Exception as e:
            self.log(f"Error computing checksum for {file_path}: {e}", logging.ERROR)
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
        """
        Download the media items, either in concurrent or sequential mode based on self.download_mode.
        """
        self.init_profile_database(folder_name)
        base_folder = os.path.join(self.download_folder, folder_name)
        os.makedirs(base_folder, exist_ok=True)
        grouped = self.group_media_by_category(media_list, file_type)

        total_downloads = sum(len(items) for items in grouped.values())
        successful_downloads = 0

        if self.download_mode == 'concurrent':
            futures = []
            for cat, items in grouped.items():
                folder = os.path.join(base_folder, cat)
                attachment_index = 1
                for url, pid, ptitle in items:
                    if self.cancel_requested.is_set():
                        break
                    future = self.executor.submit(
                        self.download_file,
                        url, folder,
                        post_id=pid, post_title=ptitle,
                        attachment_index=attachment_index
                    )
                    futures.append(future)
                    attachment_index += 1
            for future in as_completed(futures):
                if self.cancel_requested.is_set():
                    break
                if future.result():
                    successful_downloads += 1
            print('\n')  # Add extra newline to clear any remaining progress bars
            self.log(f"Finished downloading {successful_downloads} out of {total_downloads}!! <3")
        else:
            # Sequential mode.
            for cat, items in grouped.items():
                folder = os.path.join(base_folder, cat)
                attachment_index = 1
                for url, pid, ptitle in items:
                    if self.cancel_requested.is_set():
                        break
                    if self.download_file(url, folder, post_id=pid, post_title=ptitle, attachment_index=attachment_index):
                        successful_downloads += 1
                    attachment_index += 1
            print('\n')  # Add extra newline to clear any remaining progress bars
            self.log(f"Finished downloading {successful_downloads} out of {total_downloads}!! <3")

    def download_only_new_posts(self, media_list: List[MediaTuple], folder_name: str, file_type: str = 'all') -> None:
        """
        Download only new posts. If an existing URL is found, either stop or skip based on self.only_new_stop.
        """
        self.init_profile_database(folder_name)
        base_folder = os.path.join(self.download_folder, folder_name)
        os.makedirs(base_folder, exist_ok=True)
        grouped = self.group_media_by_category(media_list, file_type)

        if self.download_mode == 'concurrent':
            futures = []
            for cat, items in grouped.items():
                folder = os.path.join(base_folder, cat)
                attachment_index = 1
                for url, pid, ptitle in items:
                    if self.cancel_requested.is_set():
                        break
                    if url in self.download_cache:
                        filename = os.path.basename(url.split('?')[0])
                        self.log(f"Existing file found in DB: {filename}", logging.INFO)
                        if self.only_new_stop:
                            self.log("Stopping in only-new mode.", logging.INFO)
                            break
                        else:
                            self.log("Skipping existing file.", logging.INFO)
                            continue
                    future = self.executor.submit(
                        self._download_only_new_helper,
                        url, folder, pid, ptitle, attachment_index
                    )
                    futures.append(future)
                    attachment_index += 1
            for future in as_completed(futures):
                if self.cancel_requested.is_set():
                    break
            self.log("Finished 'only new posts' (concurrent).")
        else:
            # Sequential mode.
            for cat, items in grouped.items():
                folder = os.path.join(base_folder, cat)
                attachment_index = 1
                for url, pid, ptitle in items:
                    if self.cancel_requested.is_set():
                        break
                    if url in self.download_cache:
                        filename = os.path.basename(url.split('?')[0])
                        self.log(f"Existing file found in DB: {filename}", logging.INFO)
                        if self.only_new_stop:
                            self.log("Stopping in only-new mode.", logging.INFO)
                            break
                        else:
                            self.log("Skipping existing file.", logging.INFO)
                            continue
                    self._download_only_new_helper(url, folder, pid, ptitle, attachment_index)
                    attachment_index += 1
            self.log("Finished 'only new posts' (sequential).")

    def _download_only_new_helper(self, url: str, folder: str, post_id: Optional[Any],
                                  post_title: Optional[str], attachment_index: int) -> None:
        resp = self.safe_request(url)
        if not resp:
            self.log(f"Failed to download: {url}", logging.ERROR)
            return
        os.makedirs(folder, exist_ok=True)
        filename = self.generate_filename(url, post_id, post_title, attachment_index)
        path = os.path.join(folder, filename)
        sha256 = hashlib.sha256() if self.verify_checksum else None
        checksum = self._write_file(resp, path, None, sha256)
        if checksum is None and self.verify_checksum:
            self.log(f"Download failed or checksum error for: {filename}", logging.ERROR)
            return
        try:
            final_size = os.path.getsize(path)
        except Exception as e:
            self.log(f"Error getting file size for {path}: {e}", logging.ERROR)
            return
        self._record_download(url, path, final_size, checksum)

    def fetch_all_posts(self, base_site: str, user_id: str, service: str) -> List[Any]:
        all_posts = []
        offset = 0
        user_enc = quote_plus(user_id)
        while not self.cancel_requested.is_set():
            url = f"{base_site}/api/v1/{service}/user/{user_enc}?o={offset}"
            self.log(f"Fetching posts: {url}", logging.DEBUG)
            resp = self.safe_request(url, method="get", stream=False)
            if not resp:
                break
            try:
                posts = resp.json()
            except Exception:
                self.log("Error parsing JSON response.", logging.ERROR)
                break
            if not posts:
                break
            all_posts.extend(posts)
            offset += 50
        return all_posts

    def fetch_search_posts(self, base_site: str, query: str) -> List[Any]:
        all_posts = []
        offset = 0
        query_enc = quote_plus(query)
        while not self.cancel_requested.is_set():
            url = f"{base_site}/api/v1/posts?q={query_enc}&o={offset}"
            self.log(f"Fetching search results: {url}", logging.DEBUG)
            resp = self.safe_request(url, method="get", stream=False)
            if not resp:
                break
            try:
                data = resp.json()
                # Extract posts from the response data structure
                if isinstance(data, dict) and 'posts' in data:
                    posts = data['posts']
                    if not posts:
                        break
                    all_posts.extend(posts)
                    # Check if we've received all posts
                    if len(posts) < 50:
                        break
                else:
                    self.log("Unexpected response format", logging.ERROR)
                    break
            except Exception as e:
                self.log(f"Error parsing JSON response: {e}", logging.ERROR)
                break
            offset += 50
        return all_posts

    def fetch_popular_posts(self, base_site: str, date: Optional[str] = None, period: Optional[str] = None) -> List[Any]:
        """Fetch popular posts with optional date and period filtering.

        Args:
            base_site: The base site URL (coomer.su or kemono.su)
            date: Optional date in YYYY-MM-DD format
            period: Optional period ('day', 'week', or 'month')

        Returns:
            List of posts from the popular posts API endpoint
        """
        url = f"{base_site}/api/v1/posts/popular"
        params = {}
        if date:
            params['date'] = date
        if period:
            params['period'] = period

        # Construct URL with parameters
        if params:
            param_str = '&'.join(f'{k}={quote_plus(str(v))}' for k, v in params.items())
            url = f"{url}?{param_str}"

        self.log(f"Fetching popular posts: {url}", logging.DEBUG)
        resp = self.safe_request(url, method="get", stream=False)
        if not resp:
            return []

        try:
            data = resp.json()
            # Extract posts from the response data structure
            if isinstance(data, dict):
                if 'results' in data:
                    posts = data['results']
                    if not posts:
                        self.log("No popular posts found", logging.INFO)
                    return posts
                elif 'posts' in data:
                    posts = data['posts']
                    if not posts:
                        self.log("No popular posts found", logging.INFO)
                    return posts
            elif isinstance(data, list):
                if not data:
                    self.log("No popular posts found", logging.INFO)
                return data
            
            self.log("Unexpected response format", logging.ERROR)
            return []
        except Exception as e:
            self.log(f"Error parsing JSON response: {e}", logging.ERROR)
            return []

    def fetch_tag_posts(self, base_site: str, tag: str) -> List[Any]:
        """Fetch posts by tag with improved response handling."""
        all_posts = []
        offset = 0
        tag_enc = quote_plus(tag)
        while not self.cancel_requested.is_set():
            url = f"{base_site}/api/v1/posts?tag={tag_enc}&o={offset}"
            self.log(f"Fetching posts with tag: {url}", logging.DEBUG)
            resp = self.safe_request(url, method="get", stream=False)
            if not resp:
                break
            try:
                data = resp.json()
                # Extract posts from the response data structure
                if isinstance(data, dict) and 'posts' in data:
                    posts = data['posts']
                    if not posts:
                        break
                    all_posts.extend(posts)
                    # Check if we've received all posts based on count
                    if len(posts) < 50 or (data.get('count', 0) <= len(all_posts)):
                        break
                else:
                    self.log("Unexpected response format", logging.ERROR)
                    break
            except Exception as e:
                self.log(f"Error parsing JSON response: {e}", logging.ERROR)
                break
            offset += 50
            # Add a small delay between requests to avoid overwhelming the server
            time.sleep(0.5)
        return all_posts

    def fetch_posts(self, base_site: str, user_id: str, service: str, entire_profile: bool = False) -> List[Any]:
        if entire_profile:
            return self.fetch_all_posts(base_site, user_id, service)
        else:
            return self._fetch_single_post(base_site, user_id, service)

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
                if not path.startswith('http'):
                    path = urljoin(base_site, path.lstrip('/'))
                if file_type == 'all' or self.detect_file_category(path) == file_type:
                    results.append((path, post_id, post_title))
            # Attachments
            if 'attachments' in post:
                for att in post['attachments']:
                    path = att.get('path')
                    if path:
                        if not path.startswith('http'):
                            path = urljoin(base_site, path.lstrip('/'))
                        if file_type == 'all' or self.detect_file_category(path) == file_type:
                            results.append((path, post_id, post_title))
        return results

    def close(self) -> None:
        """Close the thread pool and the database connection."""
        self.executor.shutdown(wait=True)
        if self.db_conn:
            self.db_conn.close()


def create_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="coomer.py",
        description=(
            "Media Downloader for Coomer & Kemono\n\n"
            "Downloads media from user profiles, searches, tags, or popular posts.\n"
            "Supports batch downloading, filtering, authentication, and more."
        ),
        epilog=(
            "Examples:\n"
            "  # Download images from a specific user\n"
            "  # Download images from a specific user\n"
            "  python coomer.py --url 'https://coomer.su/onlyfans/user/12345' -t images\n\n"
            "  # Download entire profile, sequentially, using cookies, naming files with post title/ID\n"
            "  python3 coomer.py --url 'https://kemono.su/fanbox/user/4284365' -d ./downloads -sv -t all -e -c 25 -fn 2 --cookies \"...\"\n\n"
            "  # Download all favorited artists using login\n"
            "  python coomer.py --favorites --login --username myuser --password mypass\n\n"
            "  # Download URLs from a file, filtering by date and size\n"
            "  python coomer.py --input-file urls.txt --date-after 2024-01-01 --max-size 50M\n\n"
            "  # Dry run a search query and export potential download URLs\n"
            "  python coomer.py --url 'https://coomer.su/posts?q=search_term' --dry-run --export-urls found_urls.txt\n\n"
            "Happy Downloading!"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # --- Input Source ---
    source_group = parser.add_argument_group("Input Source (Choose One)")
    source_mutex = source_group.add_mutually_exclusive_group(required=True)
    source_mutex.add_argument(
        "--url", # Changed from positional to optional argument
        metavar="URL",
        help=(
            "Complete URL for a user profile, post search, tag search, or popular posts. Examples:\n"
            "  --url 'https://coomer.su/onlyfans/user/12345'\n"
            "  --url 'https://kemono.su/fanbox/user/67890'\n"
            "  --url 'https://coomer.su/posts?q=search_term'\n"
            "  --url 'https://kemono.su/posts?tag=tag_name'\n"
            "  --url 'https://coomer.su/posts/popular?period=week'"
        )
    )
    source_mutex.add_argument(
        "--input-file",
        metavar="FILE",
        help="Path to a text file containing URLs to download (one URL per line)."
    )
    source_mutex.add_argument(
        "--favorites",
        action="store_true",
        help="Download all favorited artists (requires --login or --cookies)."
    )

    # --- Authentication ---
    auth_group = parser.add_argument_group("Authentication (Optional, Choose One)")
    auth_mutex = auth_group.add_mutually_exclusive_group()
    auth_mutex.add_argument(
        "-ck", "--cookies",
        metavar="COOKIE_STRING",
        type=str,
        help="Provide browser cookies as a string (e.g., '__ddg1_=abc; session=xyz'). See README for details."
    )
    auth_mutex.add_argument(
        "--login",
        action="store_true",
        help="Authenticate using username and password (requires --username and --password)."
    )
    auth_group.add_argument(
        "--username",
        metavar="USER",
        help="Username for login (required with --login)."
    )
    auth_group.add_argument(
        "--password",
        metavar="PASS",
        help="Password for login (required with --login)."
    )

    # --- Download Options ---
    download_opts = parser.add_argument_group("Download Options")
    download_opts.add_argument(
        "-d", "--download-dir",
        default="./downloads",
        help="Download directory (default: ./downloads)."
    )
    download_opts.add_argument(
        "-p", "--post-ids",
        help="Comma-separated list of specific post IDs to download from a profile URL (e.g., 123,124,125)."
    )
    download_opts.add_argument(
        "-e", "--entire-profile",
        action="store_true",
        help="Download the entire profile (all pages) instead of just the first page (when using a profile URL)."
    )
    download_opts.add_argument(
        "-n", "--only-new",
        action="store_true",
        help="Download only new posts (based on database). Stops at first existing file unless --continue-existing is used."
    )
    download_opts.add_argument(
        "-x", "--continue-existing",
        action="store_true",
        help="In --only-new mode, skip existing files instead of stopping."
    )
    download_opts.add_argument(
        "-k", "--verify-checksum",
        action="store_true",
        help="Verify SHA256 checksums of downloaded files against database record (if available)."
    )
    download_opts.add_argument(
        "-sv", "--sequential-videos",
        action="store_true",
        help="Force sequential download mode specifically for videos (may help with large files)."
    )
    download_opts.add_argument(
        "-fn", "--file-naming-mode",
        type=int,
        default=0,
        choices=[0, 1, 2],
        help="File naming mode: 0=original_index, 1=title_index_hash, 2=title-postID_index (default: 0)."
    )
    download_opts.add_argument(
        "--archive",
        choices=["zip", "tar"],
        help="Create a compressed archive (zip or tar) of downloaded files for each profile/source after completion."
    )

    # --- Filtering ---
    filter_opts = parser.add_argument_group("Filtering Options")
    filter_opts.add_argument(
        "-t", "--file-type",
        default="all",
        choices=["all", "images", "videos", "documents", "compressed", "others"],
        help="Filter downloads by file category (default: all)."
    )
    filter_opts.add_argument(
        "--date-after",
        metavar="YYYY-MM-DD",
        help="Only download posts published on or after this date."
    )
    filter_opts.add_argument(
        "--date-before",
        metavar="YYYY-MM-DD",
        help="Only download posts published on or before this date."
    )
    filter_opts.add_argument(
        "--min-size",
        metavar="SIZE",
        help="Only download files larger than this size (e.g., 10M, 500K, 1G)."
    )
    filter_opts.add_argument(
        "--max-size",
        metavar="SIZE",
        help="Only download files smaller than this size (e.g., 100M, 2G)."
    )

    # --- Performance & Networking ---
    perf_opts = parser.add_argument_group("Performance & Networking")
    perf_opts.add_argument(
        "-w", "--workers",
        type=int,
        default=5,
        help="Max number of parallel download threads (default: 5)."
    )
    perf_opts.add_argument(
        "-r", "--rate-limit",
        type=float,
        default=2.0,
        help="Min interval (seconds) between requests to the same domain (default: 2.0)."
    )
    perf_opts.add_argument(
        "-c", "--concurrency",
        type=int,
        default=2,
        help="Max concurrent requests per domain (default: 2)."
    )
    perf_opts.add_argument(
        "-dm", "--download-mode",
        choices=["concurrent", "sequential"],
        default="concurrent",
        help="Download files concurrently (faster) or sequentially (one by one)."
    )
    perf_opts.add_argument(
        "--proxy",
        metavar="PROXY_URL",
        help="Proxy server URL (e.g., http://user:pass@host:port, socks5://host:port)."
    )

    # --- Other Options ---
    other_opts = parser.add_argument_group("Other Options")
    other_opts.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the download process: fetch post info and list files that would be downloaded, but don't actually download."
    )
    other_opts.add_argument(
        "--export-urls",
        metavar="FILE",
        help="When using --dry-run, export the list of media URLs that would be downloaded to this file."
    )
    other_opts.add_argument(
        "--interactive", # moved here
        action="store_true",
        help="Launch interactive CLI mode for selecting options (experimental)."
    )
    other_opts.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable detailed debug logging."
    )

    # --- Argument Validation ---
    args = parser.parse_args()

    # Validation: --login requires --username and --password
    if args.login and (not args.username or not args.password):
        parser.error("--login requires --username and --password.")

    # Validation: --favorites requires auth
    if args.favorites and not (args.login or args.cookies):
         parser.error("--favorites requires authentication (--login or --cookies).")

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



def signal_handler(sig, frame) -> None:
    print("Ctrl+C received. Cancelling downloads...")
    if downloader:
        downloader.request_cancel()

# Global downloader instance for signal handler
downloader: Optional[DownloaderCLI] = None

# Helper functions for new features

def login_to_site(downloader: DownloaderCLI, username: str, password: str) -> bool:
    """
    Login to the site using username and password.
    Returns True if login successful, False otherwise.
    """
    try:
        base_site = "https://coomer.su"  # Default site for login
        login_url = f"{base_site}/v1/authentication/login"
        login_data = {
            "username": username,
            "password": password
        }
        
        # Use session's post method directly for login to ensure JSON is sent properly
        logger.info("Sending login request...")
        response = downloader.session.post(
            login_url,
            json=login_data,  # This ensures proper JSON format
            headers={
                **downloader.headers,
                "Content-Type": "application/json",
                "Accept": "application/json"
            },
            allow_redirects=True,
            timeout=30.0
        )
        
        if not response or response.status_code != 200:
            logger.error(f"Login failed with status code: {response.status_code if response else 'No response'}")
            return False
            
        # Parse response
        try:
            user_data = response.json()
            logger.debug(f"Login successful for user ID: {user_data.get('id')}")
            
            # Extract and set cookies from the response
            for cookie in response.cookies:
                downloader.session.cookies.set(cookie.name, cookie.value)
                
            logger.info(f"Successfully set {len(response.cookies)} cookies from login response")
            return True
        except Exception as e:
            logger.error(f"Error processing login response: {e}")
            return False
            
    except Exception as e:
        logger.error(f"Error during login: {e}")
        logger.debug(traceback.format_exc())
        return False

def logout_from_site(downloader: DownloaderCLI, base_site: str) -> None:
    """
    Logout from the site
    """
    try:
        logout_url = f"{base_site}/v1/authentication/logout"
        response = downloader.safe_request(logout_url, method="post", stream=False)
        if response and response.ok:
            logger.info("Successfully logged out")
        else:
            logger.warning("Logout request failed or returned non-OK status")
    except Exception as e:
        logger.warning(f"Error during logout: {e}")
        logger.debug(traceback.format_exc())

def process_favorites(downloader: DownloaderCLI, base_site: str) -> List[Dict[str, Any]]:
    """
    Fetch and process favorite artists from the API
    Returns a list of formatted sources to download
    """
    favorites_url = f"{base_site}/v1/account/favorites?type=artist"
    
    # Request the favorites list
    logger.info(f"Fetching favorites from {favorites_url}")
    resp = downloader.safe_request(favorites_url, method="get", stream=False)
    
    if not resp or not resp.ok:
        logger.error(f"Failed to fetch favorites: {resp.status_code if resp else 'No response'}")
        return []
        
    try:
        favorites = resp.json()
        logger.info(f"Found {len(favorites)} favorited artists")
        
        # Transform the favorites into a format we can process
        sources = []
        for fav in favorites:
            service = fav.get('service')
            user_id = fav.get('id')
            name = fav.get('name', user_id)
            
            if not service or not user_id:
                logger.warning(f"Skipping favorite with missing data: {fav}")
                continue
                
            sources.append({
                'service': service,
                'user_id': user_id,
                'name': name,
                'url': f"{base_site}/{service}/user/{user_id}"
            })
            
        return sources
    except Exception as e:
        logger.error(f"Error processing favorites: {e}")
        logger.debug(traceback.format_exc())
        return []

def read_input_file(file_path: str) -> List[str]:
    """
    Read URLs from an input file, one URL per line
    Skips empty lines and lines starting with #
    """
    urls = []
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    urls.append(line)
        return urls
    except Exception as e:
        logger.error(f"Error reading input file {file_path}: {e}")
        raise ValueError(f"Could not read input file: {e}")

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
            logger.warning(f"Could not parse size string: {size_str}")
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
            logger.warning(f"Invalid date format for --date-after: {args.date_after}. Expected YYYY-MM-DD.")
    
    if args.date_before:
        try:
            date_before = time.strptime(args.date_before, "%Y-%m-%d")
        except ValueError:
            logger.warning(f"Invalid date format for --date-before: {args.date_before}. Expected YYYY-MM-DD.")
    
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
                    logger.debug(f"Error parsing post date '{post_date_str}': {e}")
        
        # Apply size filters if applicable
        if min_size or max_size:
            # We need to do a HEAD request to get the file size
            try:
                remote_size = None
                resp = downloader.safe_request(media_url, method="head", stream=False)
                if resp and resp.ok and 'content-length' in resp.headers:
                    remote_size = int(resp.headers['content-length'])
                    
                    if min_size and remote_size < min_size:
                        logger.debug(f"Skipping {os.path.basename(media_url)} (size {remote_size} < min_size {min_size})")
                        continue
                    if max_size and remote_size > max_size:
                        logger.debug(f"Skipping {os.path.basename(media_url)} (size {remote_size} > max_size {max_size})")
                        continue
            except Exception as e:
                logger.debug(f"Error getting size for {media_url}: {e}")
        
        # If we get here, the media passed all filters
        filtered_media.append((media_url, post_id, post_title))
    
    logger.info(f"Applied filters: {len(filtered_media)} of {len(media_tuples)} files match criteria")
    return filtered_media

def perform_dry_run(downloader: DownloaderCLI, media_tuples: List[MediaTuple], export_path: Optional[str] = None) -> None:
    """
    Perform a dry run - display what would be downloaded without actually downloading
    Optionally export URLs to a file
    """
    logger.info("=== DRY RUN MODE - No files will be downloaded ===")
    
    # Group by category
    categories = defaultdict(list)
    for url, post_id, post_title in media_tuples:
        cat = downloader.detect_file_category(url)
        categories[cat].append((url, post_id, post_title))
    
    # Print summary
    for cat, items in categories.items():
        logger.info(f"{cat}: {len(items)} files")
        for i, (url, post_id, post_title) in enumerate(items[:5]):
            filename = downloader.generate_filename(url, post_id, post_title, i+1)
            logger.info(f"  Sample: {filename} ({url})")
        if len(items) > 5:
            logger.info(f"  ... and {len(items) - 5} more")
    
    # Export URLs if requested
    if export_path:
        try:
            with open(export_path, 'w') as f:
                for url, _, _ in media_tuples:
                    f.write(f"{url}\n")
            logger.info(f"Exported {len(media_tuples)} URLs to {export_path}")
        except Exception as e:
            logger.error(f"Error exporting URLs to {export_path}: {e}")

def create_archive(downloader: DownloaderCLI, folder_path: str, archive_type: str) -> None:
    """
    Create a compressed archive (zip or tar) of the downloaded files
    """
    import shutil
    import datetime
    
    if archive_type not in ['zip', 'tar']:
        logger.warning(f"Unsupported archive type: {archive_type}")
        return
    
    try:
        # Create archive filename with timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        basename = os.path.basename(folder_path.rstrip('/'))
        archive_name = f"{basename}_{timestamp}.{archive_type}"
        archive_path = os.path.join(os.path.dirname(folder_path), archive_name)
        
        logger.info(f"Creating {archive_type} archive of {folder_path} at {archive_path}")
        
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
            # Rename to match the expected filename
            os.rename(f"{os.path.splitext(archive_path)[0]}.tar.gz", archive_path)
        
        logger.info(f"Archive created successfully: {archive_path}")
    except Exception as e:
        logger.error(f"Error creating archive: {e}")
        logger.debug(traceback.format_exc())

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
            logger.info("No popular posts found.")
            return
        folder_name = "popular"
        if date: folder_name += f"_{date}"
        if period: folder_name += f"_{period}"
        media_tuples = downloader.extract_media(all_posts, args.file_type, base_site)
    
    # Handle search query
    elif 'q' in query_params:
        query = query_params['q']
        all_posts = downloader.fetch_search_posts(base_site, query)
        if not all_posts:
            logger.info(f"No posts found for search query: {query}")
            return
        media_tuples = downloader.extract_media(all_posts, args.file_type, base_site)
        folder_name = f"search_{query.replace(' ', '_').replace('/', '_')[:30]}"  # Sanitize and limit length
    
    # Handle tag-based search
    elif 'tag' in query_params:
        tag = query_params['tag']
        all_posts = downloader.fetch_tag_posts(base_site, tag)
        if not all_posts:
            logger.info(f"No posts found with tag: {tag}")
            return
        media_tuples = downloader.extract_media(all_posts, args.file_type, base_site)
        folder_name = f"tag_{tag.replace(' ', '_').replace('/', '_')[:30]}"  # Sanitize and limit length
    
    # Handle user/service based URL
    else:
        if len(path_parts) < 2:
            raise ValueError(f"Could not parse service/user_id from URL: {url}")
        
        service = path_parts[0]
        user_id = path_parts[2] if (len(path_parts) >= 3 and path_parts[1] == 'user') else path_parts[1]
        
        username = downloader.fetch_username(base_site, service, user_id)
        folder_name = downloader.sanitize_filename(f"{username[:30]} - {service}")  # Sanitize and limit length
        
        all_posts = downloader.fetch_posts(base_site, user_id, service, entire_profile=args.entire_profile)
        if not all_posts:
            logger.info(f"No posts found for {service}/user/{user_id}")
            return
        
        if args.post_ids:
            post_ids = [pid.strip() for pid in args.post_ids.split(',')]
            posts_by_id = {str(p.get('id')): p for p in all_posts}
            media_tuples = []
            for pid in post_ids:
                post = posts_by_id.get(pid)
                if not post:
                    logger.warning(f"No post found with ID {pid}")
                else:
                    media_tuples.extend(downloader.extract_media([post], args.file_type, base_site))
        else:
            media_tuples = downloader.extract_media(all_posts, args.file_type, base_site)
    
    # Apply filters if needed
    if args.date_after or args.date_before or args.min_size or args.max_size:
        media_tuples = apply_filters(media_tuples, args, all_posts)
    
    # Handle dry run if requested
    if args.dry_run:
        perform_dry_run(downloader, media_tuples, args.export_urls)
        return
    
    # Download the media
    if not media_tuples:
        logger.info("No media to download after applying filters.")
        return
        
    logger.info(f"Starting download of {len(media_tuples)} files to folder: {folder_name}")
    
    if args.only_new:
        downloader.download_only_new_posts(media_tuples, folder_name, file_type=args.file_type)
    else:
        downloader.download_media(media_tuples, folder_name, file_type=args.file_type)
    
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
    
    logger.info(f"Processing {service}/user/{user_id} ({name})")
    folder_name = downloader.sanitize_filename(f"{name[:30]} - {service}")  # Sanitize and limit length
    
    try:
        all_posts = downloader.fetch_posts(base_site, user_id, service, entire_profile=args.entire_profile)
        if not all_posts:
            logger.info(f"No posts found for {service}/user/{user_id}")
            return
            
        media_tuples = downloader.extract_media(all_posts, args.file_type, base_site)
        
        # Apply filters if needed
        if args.date_after or args.date_before or args.min_size or args.max_size:
            media_tuples = apply_filters(media_tuples, args, all_posts)
        
        # Handle dry run if requested
        if args.dry_run:
            perform_dry_run(downloader, media_tuples, args.export_urls)
            return
        
        # Download the media
        if not media_tuples:
            logger.info(f"No media to download for {name} after applying filters.")
            return
            
        logger.info(f"Starting download of {len(media_tuples)} files for {name} to folder: {folder_name}")
        
        if args.only_new:
            downloader.download_only_new_posts(media_tuples, folder_name, file_type=args.file_type)
        else:
            downloader.download_media(media_tuples, folder_name, file_type=args.file_type)
        
        # Create archive if requested
        if args.archive:
            create_archive(downloader, os.path.join(downloader.download_folder, folder_name), args.archive)
            
    except Exception as e:
        logger.error(f"Error processing {service}/user/{user_id} ({name}): {e}")
        logger.debug(traceback.format_exc())

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

    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        # silence underlying libraries like requests/urllib3 unless verbose
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)


    signal.signal(signal.SIGINT, signal_handler)

    # --- Initialize Downloader ---
    # Determine download mode, considering --sequential-videos override
    download_mode = args.download_mode
    if args.sequential_videos and args.file_type == "videos":
        download_mode = "sequential"
        logger.info("Sequential download mode forced for videos (--sequential-videos).")

    try:
        # Create the downloader with basic options
        downloader = DownloaderCLI(
            download_folder=args.download_dir,
            max_workers=args.workers,
            rate_limit_interval=args.rate_limit,
            domain_concurrency=args.concurrency,
            verify_checksum=args.verify_checksum,
            only_new_stop=(not args.continue_existing),
            download_mode=download_mode,
            file_naming_mode=args.file_naming_mode
        )
        
        # Configure proxy if specified
        if args.proxy:
            logger.info(f"Using proxy: {args.proxy}")
            downloader.session.proxies = {
                'http': args.proxy,
                'https': args.proxy
            }

        # --- Authentication ---
        logged_in_session = False
        if args.login:
            logger.info(f"Attempting login as user: {args.username}...")
            success = login_to_site(downloader, args.username, args.password)
            if success:
                logger.info("Login successful.")
                logged_in_session = True
            else:
                logger.error("Login failed. Please check credentials.")
                sys.exit(1)
        elif args.cookies:
            # Parse and set cookies from string
            # Handle both comma and semicolon separators, strip whitespace
            cookie_string = args.cookies.replace(';', ',').replace(' ', '')
            cookie_string = cookie_string.strip(',;')
            cookie_pairs = [pair.strip() for pair in cookie_string.split(',') if '=' in pair]
            # Basic parsing, might need refinement for complex cookie values
            for pair in cookie_pairs:
                name, value = pair.split('=', 1)
                downloader.session.cookies.set(name, value) # Use session's cookie jar
            logger.debug(f"Using provided cookies: {'; '.join(cookie_pairs)}")


        # --- Determine Base Site (Needed for API calls) ---
        # We need a base site even for favorites/batch. Infer from first valid URL or default.
        # This logic needs refinement. Maybe require a base site arg if not using URL input?
        base_site = None
        if args.url:
            try:
                parsed_url = urlparse(args.url)
                site = parsed_url.netloc.lower()
                if any(domain in site for domain in ['coomer.su', 'coomer.party', 'kemono.su', 'kemono.party']):
                    base_site = f"https://{site}"
                else:
                     raise ValueError(f"Unsupported domain in URL: {site}")
            except Exception as e:
                 raise ValueError(f"Invalid URL provided: {args.url} - {e}")
        else:
             # If using --favorites or --input-file, need a default or way to determine site
             # For now, default to coomer.su if auth is provided, otherwise error
             if args.login or args.cookies:
                 logger.warning("No URL provided, defaulting base site to https://coomer.su for API calls.")
                 base_site = "https://coomer.su" # Or make this configurable?
             else:
                 raise ValueError("Cannot determine target site. Please provide a URL or use authentication with --favorites/--input-file.")

        # --- Process Input Sources ---
        if args.favorites:
            logger.info("Processing favorites...")
            media_sources = process_favorites(downloader, base_site)
            if not media_sources:
                logger.error("No favorites found or error accessing favorites.")
                sys.exit(1)

            # Process each favorite source
            for source_info in media_sources:
                process_source(downloader, base_site, source_info, args)

        elif args.input_file:
            logger.info(f"Processing URLs from file: {args.input_file}")
            urls = read_input_file(args.input_file)
            logger.info(f"Found {len(urls)} URLs in {args.input_file}")
            
            for url in urls:
                try:
                    logger.info(f"Processing URL: {url}")
                    parsed = urlparse(url)
                    site = parsed.netloc.lower()
                    
                    # Make sure the URL domain is supported
                    if not any(domain in site for domain in ['coomer.su', 'coomer.party', 'kemono.su', 'kemono.party']):
                        logger.warning(f"Skipping unsupported URL: {url}")
                        continue
                    
                    # Use the site from the URL for this specific entry
                    current_base = f"https://{site}"
                    process_url(downloader, current_base, url, args)
                except Exception as e:
                    logger.error(f"Error processing {url}: {e}")
                    logger.debug(traceback.format_exc())
                    # Continue with next URL rather than aborting

        elif args.url:
            logger.info(f"Processing URL: {args.url}")
            process_url(downloader, base_site, args.url, args)

        else:
             # This case should not be reached due to argparser validation
             logger.error("No valid input source specified.")
             sys.exit(1)

        # Perform logout if we logged in
        if logged_in_session and args.login:
            logger.info("Logging out...")
            logout_from_site(downloader, base_site)

    except sqlite3.OperationalError:
        # db lock errors already logged in init_profile_database
        sys.exit(1)
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection Error: Failed to connect to the server. Details: {e}")
        logger.error("Please check your internet connection, firewall settings, or if the website is down.")
        logger.debug(traceback.format_exc())
        sys.exit(1)
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout Error: The request timed out. Details: {e}")
        logger.error("The server might be slow, or your connection might be unstable. Try increasing the timeout or check your network.")
        logger.debug(traceback.format_exc())
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Configuration Error: {e}")
        logger.error("Please check the URL format or other command-line arguments.")
        logger.debug(traceback.format_exc())
        sys.exit(1)
    except Exception as e:
        # catch-all for anything else unexpected
        logger.error(f"An unexpected error occurred: {e}")
        logger.error("Please report this issue if it persists.")
        logger.debug(traceback.format_exc())
        if downloader:
            downloader.request_cancel()
        sys.exit(1) # exit with error code
    finally:
        if downloader:
            downloader.close()
        logger.info("Script finished.") # indicate completion


if __name__ == "__main__":
    main()
