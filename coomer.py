#!/usr/bin/env python3
import os
import re
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
from urllib.parse import urlparse, urljoin, quote_plus
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
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100)
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
            "Accept": "application/json",
        }

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
        self.db_conn = sqlite3.connect(db_path, check_same_thread=False)
        self.db_cursor = self.db_conn.cursor()
        self._init_database_schema()
        self._load_download_cache()
        logger.info(f"Initialized database for profile: {profile_name}")

    def _init_database_schema(self) -> None:
        """Create the database tables if they do not exist."""
        assert self.db_cursor is not None
        self.db_cursor.execute("PRAGMA journal_mode=WAL;")
        self.db_cursor.execute("PRAGMA synchronous=NORMAL;")
        self.db_cursor.execute("PRAGMA cache_size=-2000;")
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
        timeout: float = 10.0
    ) -> Optional[requests.Response]:
        """
        Make a safe HTTP request with rate limiting and retry handling.
        """
        if self.cancel_requested.is_set():
            return None

        req_headers = self.headers.copy()
        if extra_headers:
            req_headers.update(extra_headers)

        domain = urlparse(url).netloc
        with self.domain_locks[domain]:
            elapsed = time.time() - self.domain_last_request[domain]
            if elapsed < self.rate_limit_interval:
                time.sleep(self.rate_limit_interval - elapsed)
            try:
                resp = self.session.request(method, url, headers=req_headers,
                                            stream=stream, allow_redirects=True, timeout=timeout)
                resp.raise_for_status()
                self.domain_last_request[domain] = time.time()
                return resp
            except requests.exceptions.RequestException as e:
                self.log(f"Error requesting {url}: {e}", logging.ERROR)
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
    ) -> None:
        """Download a single file."""
        if self.cancel_requested.is_set():
            return

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
                    return
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
            return

        sha256 = hashlib.sha256() if self.verify_checksum else None
        checksum = self._write_file(resp, final_path, remote_size, sha256)
        if checksum is None and self.verify_checksum:
            self.log(f"Download failed or checksum error for: {filename}", logging.ERROR)
            return
        try:
            final_size = os.path.getsize(final_path)
        except Exception as e:
            self.log(f"Error getting file size for {final_path}: {e}", logging.ERROR)
            return
        self._record_download(url, final_path, final_size, checksum)

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
            self.log(f"Concurrent '{file_type}' download complete/cancelled.")
        else:
            # Sequential mode.
            for cat, items in grouped.items():
                folder = os.path.join(base_folder, cat)
                attachment_index = 1
                for url, pid, ptitle in items:
                    if self.cancel_requested.is_set():
                        break
                    self.download_file(url, folder, post_id=pid, post_title=ptitle, attachment_index=attachment_index)
                    attachment_index += 1
            self.log(f"Sequential '{file_type}' download complete/cancelled.")

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

    def extract_media(self, posts: List[Any], file_type: str) -> List[MediaTuple]:
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
                    path = urljoin("https://coomer.su", path)
                if file_type == 'all' or self.detect_file_category(path) == file_type:
                    results.append((path, post_id, post_title))
            # Attachments
            if 'attachments' in post:
                for att in post['attachments']:
                    path = att.get('path')
                    if path:
                        if not path.startswith('http'):
                            path = urljoin("https://coomer.su", path)
                        if file_type == 'all' or self.detect_file_category(path) == file_type:
                            results.append((path, post_id, post_title))
        return results

    def close(self) -> None:
        """Close the thread pool and the database connection."""
        self.executor.shutdown(wait=True)
        if self.db_conn:
            self.db_conn.close()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="coomer.py",
        description=(
            "Media Downloader for Coomer & Kemono\n\n"
            "This script downloads images, videos, documents, compressed files, or any other type of media.\n"
        ),
        epilog=(
            "Examples:\n"
            "  python coomer.py 'https://coomer.su/onlyfans/user/12345' -t images\n"
            "  python coomer.py 'https://kemono.party/fanbox/user/67890' -e -n\n\n"
            "Happy Downloading!"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    required = parser.add_argument_group("Required")
    required.add_argument(
        "url",
        help=(
            "Complete URL to download. Examples:\n"
            "  https://coomer.su/onlyfans/user/12345\n"
            "  https://kemono.party/fanbox/user/67890"
        )
    )

    download_opts = parser.add_argument_group("Download Options")
    download_opts.add_argument(
        "-d", "--download-dir",
        default="./downloads",
        help="Download directory (default: ./downloads)"
    )
    download_opts.add_argument(
        "-t", "--file-type",
        default="all",
        choices=["all", "images", "videos", "documents", "compressed", "others"],
        help="File type to filter (default: all)"
    )
    download_opts.add_argument(
        "-p", "--post-ids",
        help="Comma-separated list of post IDs to download (e.g., 123,124,125)."
    )
    download_opts.add_argument(
        "-e", "--entire-profile",
        action="store_true",
        help="Download the entire profile (all pages) instead of a single post or page."
    )
    download_opts.add_argument(
        "-n", "--only-new",
        action="store_true",
        help="Download only new posts. Stops at the first existing file unless --continue-existing is used."
    )
    download_opts.add_argument(
        "--continue-existing",
        action="store_true",
        help="In only-new mode, skip existing files instead of stopping."
    )
    download_opts.add_argument(
        "--verify-checksum",
        action="store_true",
        help="Calculate and verify SHA256 checksums of downloaded files."
    )
    # New flag to force sequential mode for videos (recommended for Coomer)
    download_opts.add_argument(
        "--sequential-videos",
        action="store_true",
        help="Force sequential mode for videos (recommended for Coomer to ensure complete downloads)."
    )

    perf_opts = parser.add_argument_group("Performance & Networking")
    perf_opts.add_argument(
        "-w", "--workers",
        type=int,
        default=5,
        help="Maximum number of threads for parallel downloads (default: 5)"
    )
    perf_opts.add_argument(
        "-r", "--rate-limit",
        type=float,
        default=2.0,
        help="Minimum interval (in seconds) between requests to the same domain (default: 2.0)"
    )
    perf_opts.add_argument(
        "-c", "--concurrency",
        type=int,
        default=2,
        help="Maximum number of concurrent requests per domain (default: 2)"
    )
    perf_opts.add_argument(
        "--download-mode",
        choices=["concurrent", "sequential"],
        default="concurrent",
        help="Choose 'concurrent' for parallel downloads or 'sequential' for single sequential download."
    )
    perf_opts.add_argument(
        "--file-naming-mode",
        type=int,
        default=0,
        choices=[0, 1, 2],
        help="File naming mode: 0 = original+index, 1 = post title+index+hash, 2 = post title - postID_index (default=0)."
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging."
    )

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    downloader: Optional[DownloaderCLI] = None

    def signal_handler(sig, frame) -> None:
        print("Ctrl+C received. Cancelling downloads...")
        if downloader:
            downloader.request_cancel()

    signal.signal(signal.SIGINT, signal_handler)

    try:
        parsed_url = urlparse(args.url)
        site = parsed_url.netloc.lower()
        path_parts = [p for p in parsed_url.path.strip('/').split('/') if p]

        # Determine base_site.
        if 'coomer.su' in site:
            base_site = "https://coomer.su"
        elif 'kemono.party' in site:
            base_site = "https://kemono.party"
        else:
            raise ValueError("Unsupported site")

        if len(path_parts) < 2:
            raise ValueError("Could not parse service/user_id from the URL")

        service = path_parts[0]
        user_id = path_parts[2] if (len(path_parts) >= 3 and path_parts[1] == 'user') else path_parts[1]

        # If --sequential-videos is specified and videos from Coomer are being downloaded, force sequential mode.
        download_mode = args.download_mode
        if args.sequential_videos and args.file_type == "videos" and "coomer.su" in base_site:
            download_mode = "sequential"
            logger.info("Sequential download mode forced for videos due to --sequential-videos flag.")

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

        # 1) Fetch the username to name the folder.
        username = downloader.fetch_username(base_site, service, user_id)
        folder_name = f"{username} - {service}"

        # 2) Fetch posts (entire profile or single page)
        all_posts = downloader.fetch_posts(base_site, user_id, service, entire_profile=args.entire_profile)
        if not all_posts:
            print("No posts found.")
            return

        # 3) Filter by post IDs if provided.
        if args.post_ids:
            post_ids = [pid.strip() for pid in args.post_ids.split(',')]
            posts_by_id = {str(p.get('id')): p for p in all_posts}
            media_tuples: List[MediaTuple] = []
            for pid in post_ids:
                post = posts_by_id.get(pid)
                if not post:
                    print(f"No post found with ID {pid}")
                    continue
                media_tuples.extend(downloader.extract_media([post], args.file_type))
        else:
            media_tuples = downloader.extract_media(all_posts, args.file_type)

        # 4) Decide whether to use only-new mode or normal mode.
        if args.only_new:
            downloader.download_only_new_posts(media_tuples, folder_name, file_type=args.file_type)
        else:
            downloader.download_media(media_tuples, folder_name, file_type=args.file_type)

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.debug(traceback.format_exc())
        if downloader:
            downloader.request_cancel()
        raise
    finally:
        if downloader:
            downloader.close()


if __name__ == "__main__":
    main()
