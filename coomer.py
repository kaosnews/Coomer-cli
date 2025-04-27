#!/usr/bin/env python3
import os
import re
import sys
import time
import signal
import functools
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

# Try to initialize color and terminal support
try:
    import colorama
    import shutil
    import signal
    colorama.init()
    HAS_COLOR = True
    
    def get_terminal_width():
        return shutil.get_terminal_size().columns
    
    TERM_WIDTH = get_terminal_width()
    
    # Handle terminal resize
    def handle_resize(signum, frame):
        global TERM_WIDTH, BAR_WIDTH, DESC_WIDTH
        TERM_WIDTH = get_terminal_width()
        BAR_WIDTH = min(30, max(20, TERM_WIDTH - 70))
        DESC_WIDTH = min(50, max(30, TERM_WIDTH - BAR_WIDTH - 40))
    
    signal.signal(signal.SIGWINCH, handle_resize)
    
except ImportError:
    HAS_COLOR = False
    TERM_WIDTH = 80  # Default width

# Adjust progress bar width based on terminal
BAR_WIDTH = min(30, max(20, TERM_WIDTH - 70))  # Dynamic but reasonable size
DESC_WIDTH = min(50, max(30, TERM_WIDTH - BAR_WIDTH - 40))  # Adjust filename width

# ANSI color codes
class Colors:
    RESET   = "\033[0m" if HAS_COLOR else ""
    RED     = "\033[31m" if HAS_COLOR else ""
    GREEN   = "\033[32m" if HAS_COLOR else ""
    YELLOW  = "\033[33m" if HAS_COLOR else ""
    BLUE    = "\033[34m" if HAS_COLOR else ""
    MAGENTA = "\033[35m" if HAS_COLOR else ""
    CYAN    = "\033[36m" if HAS_COLOR else ""

# Unicode symbols with fallbacks
class Symbols:
    CHECK     = "✓" if HAS_COLOR else "+"
    CROSS     = "✗" if HAS_COLOR else "x"
    RETRY     = "⟳" if HAS_COLOR else "R"
    DOWNLOAD  = "⇣" if HAS_COLOR else "v"
    CLOCK     = "◴" if HAS_COLOR else "T"
    DISK      = "⬙" if HAS_COLOR else "D"
    STAR      = "★" if HAS_COLOR else "*"
    BORDER_V  = "│" if HAS_COLOR else "|"
    BORDER_H  = "─" if HAS_COLOR else "-"

# Configure logging with color support
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# Alias for media tuple: (media_url, post_id, post_title)
MediaTuple = Tuple[str, Optional[Any], Optional[str]]


class DownloaderCLI:
    def _print_download_summary(self, successful: int, total: int) -> None:
        """Print a formatted download summary with progress bar and stats."""
        print('\n' + f"{Colors.BLUE}{Symbols.BORDER_H * TERM_WIDTH}{Colors.RESET}")
        
        success_rate = (successful / total) * 100 if total > 0 else 0
        failed = total - successful
        
        # Create summary progress bar
        bar_width = TERM_WIDTH - 50
        filled = int(bar_width * (success_rate / 100))
        empty = bar_width - filled
        
        # Build progress bar with gradient colors
        if success_rate > 80:
            bar_color = Colors.GREEN
        elif success_rate > 50:
            bar_color = Colors.YELLOW
        else:
            bar_color = Colors.RED
            
        bar = (
            f"{bar_color}{'█' * filled}{Colors.RESET}"
            f"{Colors.RED}{'░' * empty}{Colors.RESET}"
        )
        
        # Print summary with icons
        self.log(f"Download Summary {Colors.BLUE}│{Colors.RESET}{bar}{Colors.BLUE}│{Colors.RESET}")
        self.log(
            f"{Colors.GREEN}{Symbols.CHECK} {successful:,} successful{Colors.RESET}, "
            f"{Colors.RED}{Symbols.CROSS} {failed:,} failed{Colors.RESET} "
            f"({Colors.YELLOW}{success_rate:.1f}%{Colors.RESET})"
        )
        
        print(f"{Colors.BLUE}{Symbols.BORDER_H * TERM_WIDTH}{Colors.RESET}")
        print()  # Add final newline for spacing
    def __init__(
        self,
        download_folder: str,
        max_workers: int = 5,
        rate_limit_interval: float = 2.0,
        domain_concurrency: int = 2,
        verify_checksum: bool = False,
        only_new_stop: bool = True,
        download_mode: str = 'concurrent',  # "concurrent" for parallel downloads, "sequential" for sequential download
        file_naming_mode: int = 0,
        cookie_string: Optional[str] = None,
        retry_count: int = 2,  # Default number of retries for failed downloads
        retry_delay: float = 2.0,  # Delay between retries in seconds
        final_retry_count: int = 3  # Number of retries to attempt at the end for failed downloads
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

        # Store retry configuration
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.final_retry_count = final_retry_count
        self.failed_downloads: List[Tuple[str, str, Dict[str, Any]]] = []
        self.retry_immediately = False  # Default to retry at end

        # Configure session
        self.session: requests.Session = requests.Session()

        # Create custom retry strategy
        retries = Retry(
            total=0,  # Disable built-in retries to use our custom logic
            status_forcelist=[403, 429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
            respect_retry_after_header=True,
            raise_on_status=True,  # Raise exceptions so we can handle them
            backoff_factor=0  # We'll handle our own backoff
        )
        
        # Configure adapter with connection pooling and no timeouts
        adapter = HTTPAdapter(
            pool_connections=100,  # Increased pool size
            pool_maxsize=100,
            pool_block=False,
            max_retries=Retry(
                total=0,  # We handle retries ourselves
                connect=3,  # Only retry initial connection
                read=0,    # No read timeouts
                allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
                status_forcelist=[403, 429, 500, 502, 503, 504]
            )
        )

        # Disable timeouts globally for this session
        self.session.request = functools.partial(self.session.request, timeout=None)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)

        # Initialize download tracking
        self._download_position = 0
        self._position_lock = threading.Lock()
        
        # Create custom thread pool that tracks worker position
        class PositionTrackingThreadPoolExecutor(ThreadPoolExecutor):
            def __init__(self, max_workers, downloader):
                super().__init__(max_workers=max_workers)
                self.downloader = downloader
            
            def submit(self, fn, *args, **kwargs):
                def wrapped_fn(*args, **kwargs):
                    with self.downloader._position_lock:
                        self.downloader._download_position += 1
                        pos = self.downloader._download_position
                        threading.current_thread().download_position = pos
                    try:
                        return fn(*args, **kwargs)
                    finally:
                        with self.downloader._position_lock:
                            self.downloader._download_position -= 1
                
                return super().submit(wrapped_fn, *args, **kwargs)
        
        # Select ThreadPoolExecutor based on download mode
        if self.download_mode == 'sequential':
            self.executor: ThreadPoolExecutor = PositionTrackingThreadPoolExecutor(1, self)
        else:
            self.executor = PositionTrackingThreadPoolExecutor(self.max_workers, self)

        # Threading control
        self.cancel_requested: threading.Event = threading.Event()
        self.domain_last_request: Dict[str, float] = defaultdict(float)
        self.domain_locks: Dict[str, threading.Semaphore] = defaultdict(lambda: threading.Semaphore(domain_concurrency))
        
        # Progress bar management
        self._active_bars: List[tqdm] = []
        self._bars_lock = threading.Lock()

        # Database-related attributes
        self.db_conn: Optional[sqlite3.Connection] = None
        self.db_cursor: Optional[sqlite3.Cursor] = None
        self.db_lock: threading.Lock = threading.Lock()
        self.download_cache: Dict[str, Tuple[str, int, Optional[str]]] = {}  # url -> (file_path, file_size, checksum)
        self.current_profile: Optional[str] = None

        # Enhanced browser-like headers
        self.headers: Dict[str, str] = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:137.0) Gecko/20100101 Firefox/137.0",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Connection": "keep-alive",
            "Sec-GPC": "1",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "TE": "trailers",
            "Priority": "u=0"
        }

        # Initialize session cookies if provided
        if cookie_string:
            # Parse cookie string - handle both comma and semicolon separators
            cookie_string = cookie_string.replace(';', ',').replace(' ', '')
            cookie_string = cookie_string.strip(',;')
            cookie_pairs = [pair.strip() for pair in cookie_string.split(',') if '=' in pair]
            
            for pair in cookie_pairs:
                name, value = pair.split('=', 1)
                self.session.cookies.set(name, value)
            logger.debug(f"Initialized session with cookies: {'; '.join(cookie_pairs)}")

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
        timeout: float = 60.0,  # Increased default timeout
        bypass_rate_limit: bool = False,
        max_retries: int = 3,   # Allow override of retry count
        adaptive_delay: bool = True  # Enable adaptive delay between retries
    ) -> Optional[requests.Response]:
        """
        Make a safe HTTP request with optimized rate limiting and retry handling.
        Args:
            retry_immediately: If True, retry failed downloads immediately instead of at the end
        """
        if self.cancel_requested.is_set():
            return None

        # Build complete request headers for all requests
        parsed_url = urlparse(url)
        base_site = f"{parsed_url.scheme}://{parsed_url.netloc}"
        
        # Get base domain for consistent headers
        base_domain = '.'.join(parsed_url.netloc.split('.')[-2:])  # e.g., coomer.su
        
        # Handle /data/ prefix for media URLs
        if not bypass_rate_limit and stream and method.lower() == "get":
            path_parts = parsed_url.path.lstrip('/').split('/')
            if path_parts[0] != 'data':
                new_path = f"/data/{'/'.join(path_parts)}"
                url = f"{parsed_url.scheme}://{parsed_url.netloc}{new_path}"
                parsed_url = urlparse(url)  # Update parsed_url with new URL
        
        req_headers = {
            **self.headers,
            "Host": parsed_url.netloc,  # Keep full hostname with subdomain
            "Origin": f"{parsed_url.scheme}://{base_domain}",
            "Sec-Fetch-Site": "same-site" if parsed_url.netloc != base_domain else "same-origin",
            "Referer": f"{parsed_url.scheme}://{base_domain}/artists"
        }

        # Add cookies if present
        if self.session.cookies:
            cookie_string = '; '.join([f"{k}={v}" for k, v in self.session.cookies.items()])
            if cookie_string:
                req_headers['Cookie'] = cookie_string

        # Add any extra headers last to allow overrides
        if extra_headers:
            req_headers.update(extra_headers)

        domain = urlparse(url).netloc
        with self.domain_locks[domain]:
            # Get domain's error count and last success
            domain_stats = getattr(self, '_domain_stats', {}).setdefault(domain, {
                'errors': 0,
                'last_success': 0,
                'backoff': self.rate_limit_interval
            })

            elapsed = time.time() - self.domain_last_request[domain]
            success_elapsed = time.time() - domain_stats['last_success']

            # Calculate adaptive wait time
            base_wait = self.rate_limit_interval
            if method.lower() == "get" and stream:
                # Full rate limit for downloads
                wait_time = base_wait
            else:
                # Shorter for API requests
                wait_time = base_wait / 2

            # Add additional backoff if domain has recent errors
            if adaptive_delay and domain_stats['errors'] > 0:
                # Exponential backoff based on error count
                backoff = min(domain_stats['backoff'] * (1.5 ** domain_stats['errors']), 30.0)
                wait_time = max(wait_time, backoff)
                
                # Reset error count after sufficient successful time
                if success_elapsed > 60.0 and not bypass_rate_limit:
                    domain_stats['errors'] = max(0, domain_stats['errors'] - 1)
                    domain_stats['backoff'] = max(base_wait, domain_stats['backoff'] * 0.75)

            if elapsed < wait_time and not bypass_rate_limit:
                sleep_time = wait_time - elapsed
                if sleep_time > 5.0:  # Log long waits
                    logger.info(f"Rate limiting: waiting {sleep_time:.1f}s for {domain}")
                time.sleep(sleep_time)

            last_error = None
            for retry_attempt in range(max_retries + 1):
                try:
                    # Use different settings for downloads vs API calls
                    if method.lower() == "get" and stream:
                        # File download - no timeouts
                        resp = self.session.request(
                            method,
                            url,
                            headers=req_headers,
                            stream=True,
                            allow_redirects=True,
                            timeout=None
                        )
                    else:
                        # API requests - use timeout only for initial connection
                        resp = self.session.request(
                            method,
                            url,
                            headers=req_headers,
                            stream=False,
                            allow_redirects=True,
                            timeout=(30.0, None)  # (connect timeout, no read timeout)
                        )
                    # Update success stats
                    resp.raise_for_status()
                    self.domain_last_request[domain] = time.time()
                    domain_stats['last_success'] = time.time()
                    domain_stats['errors'] = max(0, domain_stats['errors'] - 1)
                    domain_stats['backoff'] = max(self.rate_limit_interval, domain_stats['backoff'] * 0.75)
                    return resp

                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 429:
                        # Get retry delay from headers or use exponential backoff
                        retry_after = int(e.response.headers.get('Retry-After',
                            min(30, domain_stats['backoff'] * (2 ** domain_stats['errors']))))
                        
                        self.log(f"Rate limited by server for {url}. Waiting {retry_after} seconds...", logging.WARNING)
                        domain_stats['errors'] += 1
                        domain_stats['backoff'] = max(domain_stats['backoff'], float(retry_after))
                        time.sleep(retry_after)
                        continue
                    
                    elif e.response.status_code == 403:
                        if not hasattr(self, '_403_counter'):
                            self._403_counter = 0
                        self._403_counter += 1
                        
                        if self._403_counter >= 1:
                            self.log(
                                f"\n[!] Received 403 Forbidden error for {url}.\n"
                                "    Apparently its due to the ddos protection, try using cookies if youre not already.\n",
                                logging.ERROR
                            )
                        else:
                            self.log(f"Access denied (403 Forbidden) for {url}", logging.WARNING)
                        # Don't retry 403s, record and break
                        last_error = e
                        break

                    # For other HTTP errors, retry with backoff
                    self.log(f"HTTP Error {e.response.status_code} requesting {url}: {e}", logging.ERROR)
                    last_error = e
                
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                    # Network/timeout errors can be retried
                    self.log(f"Connection/timeout error requesting {url}: {e}", logging.ERROR)
                    last_error = e
                
                except requests.exceptions.RequestException as e:
                    # Other request errors
                    self.log(f"Request error for {url}: {e}", logging.ERROR)
                    last_error = e

                if last_error:
                    # Update domain stats and calculate backoff
                    domain_stats['errors'] += 1
                    if retry_attempt < max_retries:
                        backoff = min(30.0, domain_stats['backoff'] * (2 ** domain_stats['errors']))
                        wait_time = min(self.retry_delay * (2 ** retry_attempt), backoff)
                        self.log(f"Retrying in {wait_time:.1f}s (attempt {retry_attempt + 1}/{max_retries})", logging.WARNING)
                        time.sleep(wait_time)

            # All retries exhausted or got a 403
            if last_error:
                self.log(f"Failed after {retry_attempt} retries: {last_error}", logging.ERROR)
                if isinstance(last_error, requests.exceptions.HTTPError):
                    self.log(traceback.format_exc(), logging.DEBUG)
                
                # Only handle retries for download requests
                if method.lower() == "get" and stream and not bypass_rate_limit:
                    # Save request details for retry
                    retry_info = {
                        "method": method,
                        "stream": stream,
                        "timeout": timeout,
                        "extra_headers": req_headers.copy()  # Make a copy of headers
                    }
                    
                    retry_success = False
                    if getattr(self, 'retry_immediately', False):
                        # Use full delay for immediate retry
                        wait_time = min(
                            self.retry_delay * 4,  # Use higher multiplier for immediate retry
                            30.0  # Still cap at 30 seconds
                        )
                        logger.info(f"Immediate retry for {url} after {wait_time:.1f}s delay")
                        time.sleep(wait_time)
                        
                        retry_resp = self.safe_request(
                            url,
                            method=method,
                            stream=stream,
                            extra_headers=retry_info['extra_headers'],
                            timeout=timeout,
                            bypass_rate_limit=True
                        )
                        retry_success = bool(retry_resp)
                        if retry_success:
                            return retry_resp
                    
                    # Store for retry at end if:
                    # 1. Not using immediate retry, or
                    # 2. Immediate retry failed
                    if not retry_success:
                        logger.info(f"Storing {url} for retry at end")
                        self.failed_downloads.append((
                            url,
                            self.current_profile if self.current_profile else "unknown",
                            retry_info
                        ))
            
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
                    hasher: Optional[hashlib._hashlib.HASH], chunk_size: Optional[int] = None) -> Optional[str]:
        """Write downloaded file in chunks with no timeout for reading."""
        """
        Write the file in chunks to a temporary file, then rename it.
        If cancelled, remove the partial file.
        Returns the checksum (in hexadecimal) if a hasher is provided.
        """
        tmp_path = final_path + ".tmp"
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

        try:
            import colorama
            colorama.init()
        except ImportError:
            pass
            
        # Get base filename
        filename = os.path.basename(final_path)
        
        # Calculate download position and file info
        thread_pos = getattr(threading.current_thread(), 'download_position', 0)
        position = f"\033[36m[{thread_pos}/{self.max_workers}]\033[0m" # Cyan color
        
        # Calculate size string
        size_str = f"{total_size/1024/1024:.1f}MB" if total_size else "??MB"
        
        # Create detailed progress bar format with colors and unicode
        # Construct progress bar format with dynamic widths
        bar_format = (
            "{desc:<" + str(DESC_WIDTH) + "." + str(DESC_WIDTH) + "} "  # Dynamic filename width
            f"{Colors.BLUE}{Symbols.BORDER_V}{Colors.RESET}"
            "{bar:" + str(BAR_WIDTH) + "}"  # Dynamic bar width
            f"{Colors.BLUE}{Symbols.BORDER_V}{Colors.RESET} "
            f"{Colors.YELLOW}{{percentage:>4.1f}}%{Colors.RESET} "
            f"{Colors.GREEN}{Symbols.DOWNLOAD}{Colors.RESET} {{rate_fmt:>11}} "  # Slightly shorter
            f"{Colors.MAGENTA}{Symbols.CLOCK}{Colors.RESET} {{remaining:<6}} "   # Shorter time
            f"{Colors.CYAN}{Symbols.DISK}{Colors.RESET} {{n_fmt:>7}}/{{total_fmt:<7}}"  # Shorter sizes
        )

        # Format position for display
        thread_pos = getattr(threading.current_thread(), 'download_position', 0)
        pos_str = f"{thread_pos}/{self.max_workers}"
        pos_color = f"{Colors.CYAN}{pos_str:>5}{Colors.RESET}"
        
        # Add position to description if available
        desc = f"{position}{filename}"
        
        # Create progress bar
        pbar = tqdm(
            total=total_size,
            initial=0,
            desc=f"{pos_color} {filename[:DESC_WIDTH-7]}",  # Account for position width
            bar_format=bar_format,
            ascii=" ▇█",           # Space, partial block, full block
            mininterval=0.1,        # Faster updates (100ms)
            maxinterval=0.2,        # Maximum update interval
            dynamic_ncols=False,    # We handle width ourselves
            ncols=TERM_WIDTH,       # Use full terminal width
            smoothing=0.01,         # Very smooth progress
            position=getattr(threading.current_thread(), 'download_position', 0),
            leave=False,            # Don't leave the bar when done
            unit='B',               # Show bytes
            unit_scale=True,        # Show units (KB, MB, etc)
            unit_divisor=1024       # Use 1024 for binary prefixes
        )
        
        # Register progress bar
        with self._bars_lock:
            self._active_bars.append(pbar)
        
        try:
            with open(tmp_path, 'wb') as f, pbar:
                try:
                    # Adjust chunk size based on file size
                    if total_size:
                        # Use larger chunks for bigger files
                        if total_size > 100 * 1024 * 1024:  # >100MB
                            chunk_size = 1024 * 1024  # 1MB chunks
                        elif total_size > 10 * 1024 * 1024:  # >10MB
                            chunk_size = 256 * 1024  # 256KB chunks
                        else:
                            chunk_size = 64 * 1024  # 64KB chunks
                    else:
                        chunk_size = 64 * 1024  # Default to 64KB
                    
                    # No socket timeout for reading file data
                    if hasattr(resp.raw, 'connection') and hasattr(resp.raw.connection, 'sock'):
                        resp.raw.connection.sock.settimeout(None)
                    
                    # Use iter_content with proper chunk size
                    for chunk in resp.iter_content(chunk_size=chunk_size):
                        if self.cancel_requested.is_set():
                            f.close()
                            os.remove(tmp_path)
                            self.log(f"Cancelled and removed partial file: {tmp_path}", logging.WARNING)
                            return None
                            
                        if not chunk:  # Filter out keep-alive chunks
                            continue
                            
                        # No need to reset socket timeout - we want continuous streaming
                            
                        try:
                            f.write(chunk)
                            if hasher:
                                hasher.update(chunk)
                            pbar.update(len(chunk))
                        except IOError as e:
                            self.log(f"IO error writing chunk to {tmp_path}: {e}", logging.ERROR)
                            self.log("Trying to handle disk write error...", logging.WARNING)
                            try:
                                # Force sync to disk
                                f.flush()
                                os.fsync(f.fileno())
                            except:
                                pass
                            raise
                            
                except requests.exceptions.ReadTimeout:
                    self.log(f"Read timeout downloading {os.path.basename(final_path)}", logging.ERROR)
                    raise
                except Exception as e:
                    self.log(f"Error downloading {os.path.basename(final_path)}: {e}", logging.ERROR)
                    raise
            
            # Only rename if the download completed successfully
            os.rename(tmp_path, final_path)
            return hasher.hexdigest() if hasher else None
            
        except Exception as e:
            self.log(f"Error writing file {final_path}: {e}", logging.ERROR)
            # Clean up temp file and progress bar on error
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            return None
            
        finally:
            # Always unregister progress bar
            with self._bars_lock:
                if pbar in self._active_bars:
                    self._active_bars.remove(pbar)

    def fetch_username(self, base_site: str, service: str, user_id: str) -> str:
        """Fetch the username for the profile (used for naming the folder)."""
        profile_url = f"{base_site}/api/v1/{service}/user/{user_id}/profile"
        resp = self.safe_request(profile_url, method="get", stream=False)
        try:
            return resp.json().get("name", user_id) if resp else user_id
        except Exception:
            return user_id

    def get_remote_file_size(self, url: str) -> Optional[int]:
        # Parse URL and handle n2/n3 subdomains for HEAD request
        parsed_url = urlparse(url)
        base_domain = '.'.join(parsed_url.netloc.split('.')[-2:])  # Get base domain (e.g., coomer.su)
        
        # Use same headers for HEAD request as download
        download_headers = {
            **self.headers,
            "Host": parsed_url.netloc,  # Use full hostname including subdomain
            "Origin": f"{parsed_url.scheme}://{base_domain}",
            "Referer": f"{parsed_url.scheme}://{base_domain}/artists"
        }
        if self.session.cookies:
            cookie_string = '; '.join([f"{k}={v}" for k, v in self.session.cookies.items()])
            if cookie_string:
                download_headers['Cookie'] = cookie_string

        resp = self.safe_request(
            url,
            method="head",
            stream=False,
            extra_headers=download_headers,
            timeout=10.0  # Short timeout for HEAD requests
        )
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
        attachment_index: int = 1,
        is_retry: bool = False,
        bypass_rate_limit: bool = False
    ) -> bool:
        """Download a single file. Returns True if download was successful."""
        if self.cancel_requested.is_set():
            if not bypass_rate_limit:
                # Store for retry with increased timeout
                retry_info = {
                    "method": "get",
                    "stream": True,
                    "timeout": 120.0,  # Double timeout for retry
                    "extra_headers": download_headers
                }
                self.failed_downloads.append((url, folder, retry_info))
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

        # Parse URL and handle n2/n3 subdomains
        parsed_url = urlparse(url)
        base_domain = '.'.join(parsed_url.netloc.split('.')[-2:])  # Get base domain (e.g., coomer.su)
        
        # Build headers for download request with larger buffer sizes
        download_headers = {
            **self.headers,
            "Host": parsed_url.netloc,  # Use full hostname including subdomain
            "Origin": f"{parsed_url.scheme}://{base_domain}",
            "Referer": f"{parsed_url.scheme}://{base_domain}/artists",
            "Accept-Encoding": "identity",  # Disable compression for direct streaming
            "Connection": "keep-alive"
        }

        # Add cookies if present
        if self.session.cookies:
            cookie_string = '; '.join([f"{k}={v}" for k, v in self.session.cookies.items()])
            if cookie_string:
                download_headers['Cookie'] = cookie_string

        resp = self.safe_request(url, extra_headers=download_headers)
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
        # Use the full folder name for both database and folders to maintain consistency
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
                    # Create Future with URL tracking
                    future = self.executor.submit(
                        self.download_file,
                        url, folder,
                        post_id=pid, post_title=ptitle,
                        attachment_index=attachment_index
                    )
                    # Store URL in the Future object for status messages
                    future.url = url
                    futures.append(future)
                    attachment_index += 1
            for future in as_completed(futures):
                if self.cancel_requested.is_set():
                    break
                success = future.result()
                if success:
                    successful_downloads += 1
                    filename = os.path.basename(future.url)
                    size = os.path.getsize(os.path.join(folder, filename))
                    size_str = f"{size/1024/1024:.1f}MB"
                    logger.info(f"{Colors.GREEN}{Symbols.CHECK}{Colors.RESET} Downloaded: {filename} ({Colors.CYAN}{size_str}{Colors.RESET})")
                else:
                    logger.error(f"{Colors.RED}{Symbols.CROSS}{Colors.RESET} Failed: {os.path.basename(future.url)}")
                    print()  # Add spacing after error
            
            # Print formatted summary using the new method
            self._print_download_summary(successful_downloads, total_downloads)
        else:
            # Sequential mode.
            for cat, items in grouped.items():
                folder = os.path.join(base_folder, cat)
                attachment_index = 1
                for url, pid, ptitle in items:
                    if self.cancel_requested.is_set():
                        break
                    success = self.download_file(url, folder, post_id=pid, post_title=ptitle, attachment_index=attachment_index)
                    if success:
                        successful_downloads += 1
                        filename = os.path.basename(url)
                        size = os.path.getsize(os.path.join(folder, filename))
                        size_str = f"{size/1024/1024:.1f}MB"
                        logger.info(f"{Colors.GREEN}{Symbols.CHECK}{Colors.RESET} Downloaded: {filename} ({Colors.CYAN}{size_str}{Colors.RESET})")
                    else:
                        logger.error(f"{Colors.RED}{Symbols.CROSS}{Colors.RESET} Failed: {os.path.basename(url)}")
                        print()  # Add spacing after error
                    attachment_index += 1

            # Print formatted summary using the new method
            self._print_download_summary(successful_downloads, total_downloads)

    def download_only_new_posts(self, media_list: List[MediaTuple], folder_name: str, file_type: str = 'all') -> None:
        """
        Download only new posts. If an existing URL is found, either stop or skip based on self.only_new_stop.
        """
        # Use the full folder name consistently here as well
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
                    # Create Future with URL tracking for new posts
                    future = self.executor.submit(
                        self._download_only_new_helper,
                        url, folder, pid, ptitle, attachment_index
                    )
                    # Store URL in the Future object
                    future.url = url
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
        # Parse URL and handle n2/n3 subdomains for helper
        parsed_url = urlparse(url)
        base_domain = '.'.join(parsed_url.netloc.split('.')[-2:])  # Get base domain (e.g., coomer.su)
        
        # Use consistent headers with proper subdomain handling
        download_headers = {
            **self.headers,
            "Host": parsed_url.netloc,  # Use full hostname including subdomain
            "Origin": f"{parsed_url.scheme}://{base_domain}",
            "Referer": f"{parsed_url.scheme}://{base_domain}/artists"
        }
        if self.session.cookies:
            cookie_string = '; '.join([f"{k}={v}" for k, v in self.session.cookies.items()])
            if cookie_string:
                download_headers['Cookie'] = cookie_string

        # Download with no read timeout
        resp = self.safe_request(
            url,
            extra_headers=download_headers,
            stream=True,
            bypass_rate_limit=True,  # Always bypass rate limit for retries
            max_retries=3
        )
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
            if not resp:  # If download failed
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

    def retry_failed_downloads(self) -> None:
        """
        Attempt to download any previously failed files.
        Uses exponential backoff between retries but no read timeouts during downloads.
        """
        if not self.failed_downloads:
            return
            
        # Clear previous progress bars
        with self._bars_lock:
            for bar in self._active_bars:
                try:
                    bar.clear()
                    bar.close()
                except:
                    pass
            self._active_bars.clear()

        total = len(self.failed_downloads)
        logger.info(f"\n{Colors.YELLOW}{Symbols.RETRY} Attempting final retry of failed downloads{Colors.RESET}")
        logger.info(f"{Colors.BLUE}{Symbols.BORDER_H * 80}{Colors.RESET}")
        success_count = 0
        
        for url, folder, retry_info in self.failed_downloads:
            logger.info(f"Final retry for: {url}")
            try:
                # Try downloading with increased delays between attempts
                for attempt in range(3):
                    logger.info(f"Retry attempt {attempt + 1}/3 for {url}")
                    try:
                        # Bypass rate limits on retries and use no timeout
                        if self.download_file(url, folder, is_retry=True, bypass_rate_limit=True):
                            success_count += 1
                            break
                    except Exception as e:
                        logger.error(f"Retry attempt {attempt + 1} failed: {e}")
                        if attempt < 2:  # Only sleep between retries
                            backoff = min(30.0, self.retry_delay * (2 ** attempt))
                            logger.info(f"Waiting {backoff:.1f}s before next retry...")
                            time.sleep(backoff)
                        continue
                
                if resp:
                    try:
                        os.makedirs(folder, exist_ok=True)
                        filename = os.path.basename(url.split('?')[0])
                        path = os.path.join(folder, filename)
                        
                        # Set up detailed progress bar with labels
                        bar_format = (
                            "{desc:<35.35} "     # Filename truncated to 35 chars
                            "|{bar:30}| "        # Longer progress bar (30 chars)
                            "{percentage:3.1f}% "
                            "• Speed: {rate_fmt:>12} "
                            "• ETA: {remaining:<8} "
                            "• Size: {total_fmt:>9}"
                        )
                        total_size = int(resp.headers.get('content-length', 0))
                        
                        with open(path, 'wb') as f:
                            # Use large chunk size for retries
                            chunk_size = 1024 * 1024  # 1MB chunks
                            for chunk in resp.iter_content(chunk_size=chunk_size):
                                if self.cancel_requested.is_set():
                                    f.close()
                                    os.remove(path)
                                    return
                                f.write(chunk)
                                pbar.update(len(chunk))
                        
                        success_count += 1
                        logger.info(f"Successfully downloaded on final retry: {filename}")
                    except Exception as e:
                        logger.error(f"Final retry failed for {url}: {e}")
                        logger.debug(traceback.format_exc())
                        if os.path.exists(path):
                            os.remove(path)
                
                time.sleep(self.retry_delay)  # Add delay between retries
            except Exception as e:
                logger.error(f"Error retrying {url}: {e}")
                logger.debug(traceback.format_exc())
                continue
        
        if success_count:
            logger.info(f"Recovered {success_count} of {len(self.failed_downloads)} failed downloads")

    def close(self) -> None:
        """Close the thread pool and the database connection."""
        try:
            # Clear any remaining progress bars
            with self._bars_lock:
                for bar in self._active_bars:
                    try:
                        bar.clear()
                        bar.close()
                    except:
                        pass
                self._active_bars.clear()
            
            # Try final retries of failed downloads before closing
            if self.failed_downloads and not self.cancel_requested.is_set():
                self.retry_failed_downloads()
            
            # Clean up terminal state
            print("\x1b[?25h")  # Show cursor
            
            self.executor.shutdown(wait=True)
            if self.db_conn:
                self.db_conn.close()
                
        except Exception as e:
            logger.debug(f"Error during cleanup: {e}")
            logger.debug(traceback.format_exc())


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
        choices=['coomer.su', 'coomer.party', 'kemono.su', 'kemono.party'],
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



def signal_handler(sig, frame) -> None:
    print("Ctrl+C received. Cancelling downloads...")
    if downloader:
        downloader.request_cancel()

# Global downloader instance for signal handler
downloader: Optional[DownloaderCLI] = None

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
        logger.error("Cannot attempt login without a valid base site.")
        return False

    # Use the correct API endpoint provided by user
    login_url = f"{base_site}/api/v1/authentication/login"
    # Use form data for login
    login_data = {
        "username": username,
        "password": password
    }

    try:
        logger.info(f"Sending login request to {login_url}...")
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
                # Attempt to parse JSON response, though we might not need the content
                user_data = response.json()
                logger.debug(f"Login successful (API response: {user_data})")
                # Cookies are handled automatically by the session
                logger.info(f"Session cookies updated after login.")
                return True
            except requests.exceptions.JSONDecodeError:
                # This might happen if the success response is empty or not JSON
                logger.warning("Login returned Status 200 but response was not valid JSON. Assuming success based on status code.")
                logger.debug(f"Login response content: {response.text}")
                logger.info(f"Session cookies updated after login.")
                return True # Assume success if status is 200
        else:
            # Handle non-200 status
            logger.error(f"Login failed with status code: {response.status_code}")
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
            
            logger.error(f"Server error message: {error_msg[:500]}")
            return False

    except requests.exceptions.Timeout:
        logger.error(f"Login request timed out after 30 seconds connecting to {login_url}.")
        logger.debug(traceback.format_exc())
        return False
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Login connection error to {login_url}: {e}")
        logger.error("Please check your network connection, proxy settings, and if the site is reachable.")
        logger.debug(traceback.format_exc())
        return False
    except requests.exceptions.RequestException as e:
        # Catch other potential requests library errors (e.g., invalid URL, SSL errors)
        logger.error(f"Login request failed for {login_url}: {e}")
        logger.debug(traceback.format_exc())
        return False
    except Exception as e:
        # Catch any other unexpected errors during the login process
        logger.error(f"An unexpected error occurred during login to {login_url}: {e}")
        logger.debug(traceback.format_exc())
        return False
def logout_from_site(downloader: DownloaderCLI, base_site: str) -> None:
    """
    Logout from the site
    """
    try:
        if not base_site:
            logger.warning("Cannot logout: No base site provided")
            return

        # Create URL with proper path
        logout_url = f"{base_site}/api/v1/authentication/logout"

        # Use proper headers
        headers = {
            **downloader.headers,
            "Host": urlparse(base_site).netloc,
            "Origin": base_site,
            "Referer": f"{base_site}/artists"
        }

        # Use direct session request to avoid recursion
        response = downloader.session.post(
            logout_url,
            headers=headers,
            timeout=30.0
        )
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
    favorites_url = f"{base_site}/api/v1/account/favorites?type=artist" # Changed from /v1/ to /api/v1/
    
    # Request the favorites list
    logger.info(f"Fetching favorites from {favorites_url}")
    resp = downloader.safe_request(favorites_url, method="get", stream=False)
    
    if not resp or not resp.ok:
        logger.error(f"Failed to fetch favorites: {resp.status_code if resp else 'No response'}")
        return []
        
    try:
        favorites = resp.json()
        logger.info(f"{Colors.GREEN}{Symbols.STAR}{Colors.RESET} Found {len(favorites)} favorited artists")
        
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
        # Add site name and handle popularizer if available
        site_name = urlparse(base_site).netloc.split('.')[0]  # get coomer or kemono
        folder_name = f"{site_name}_popular"
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
            logger.info(f"No posts found with tag: {tag}")
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
        all_posts = downloader.fetch_posts(base_site, user_id, service)  # always fetches entire profile by default
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


    # Handle signals gracefully
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    if hasattr(signal, 'SIGWINCH'):  # Not available on Windows
        def handle_resize(signum, frame):
            if downloader:
                # Clear existing bars
                with downloader._bars_lock:
                    for bar in downloader._active_bars:
                        try:
                            bar.clear()
                            bar.close()
                        except:
                            pass
                    downloader._active_bars.clear()
                
                # Update terminal dimensions
                global TERM_WIDTH
                TERM_WIDTH = shutil.get_terminal_size().columns
                
        signal.signal(signal.SIGWINCH, handle_resize)

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
        
        # Configure proxy if specified
        if args.proxy:
            logger.info(f"Using proxy: {args.proxy}")
            downloader.session.proxies = {
                'http': args.proxy,
                'https': args.proxy
            }

        # --- Determine Base Site (Needed for API calls) ---
        base_site = None
        supported_domains = ['coomer.su', 'coomer.party', 'kemono.su', 'kemono.party']

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
                if any(domain == site_domain for domain in supported_domains):
                    base_site = f"https://{site_domain}"
                    logger.info(f"Inferred base site from URL: {base_site}")
                else:
                     raise ValueError(f"Unsupported domain in URL: {site_domain}")
            except Exception as e:
                 raise ValueError(f"Invalid URL provided: {url} - {e}")
        elif args.site:
             # Use the explicitly provided --site argument
             if args.site in supported_domains:
                 base_site = f"https://{args.site}"
                 logger.info(f"Using specified base site: {base_site}")
             else:
                 # This case should be caught by argparse choices, but added for safety
                 raise ValueError(f"Unsupported site specified: {args.site}")
        elif args.input_file:
             # For input file without --site, we can't assume a single base site.
             # The base_site will be determined per-URL inside the processing loop.
             logger.info("Processing input file. Base site will be determined for each URL.")
             base_site = None  # Explicitly set to None, loop will handle it
        else:
             # This case should ideally not be reached due to argparse validation
             # (e.g., --favorites requires --site)
             raise ValueError("Cannot determine target site. Please provide --url or use --site with --favorites/--input-file.")

        # --- Authentication ---
        logged_in_session = False
        if args.login:
            logger.info(f"Attempting login as user: {args.username} on {base_site}...")
            if not base_site:
                 logger.error("Login requires a target site. Use --url or --site.")
                 sys.exit(1)
            success = login_to_site(downloader, base_site, args.username, args.password)
            if success:
                logger.info("Login successful.")
                logged_in_session = True
            else:
                logger.error("Login failed. Please check credentials.")
                sys.exit(1)
        elif args.cookies:
            # Cookies already set up in DownloaderCLI.__init__
            logger.debug("Using provided cookies for authentication")
        # Note: Authentication is optional unless using --favorites

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

        # Final cleanup - try to logout if needed
        if logged_in_session and args.login:
            try:
                logger.info("Logging out...")
                logout_from_site(downloader, base_site)
            except Exception as e:
                logger.warning(f"Error during logout: {e}")
                logger.debug(traceback.format_exc())

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
