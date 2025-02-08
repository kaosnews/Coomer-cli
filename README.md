# Coomer-cli

**Coomer-cli** is a Python command-line tool for downloading media (images, videos, documents, compressed files, and more) from sites like Coomer and Kemono. It features configurable download modes (concurrent or sequential), file naming strategies, rate limiting, checksum verification, and more.

## Features

- **Multi-type Downloads:** Supports images, videos, documents, compressed files, etc.
- **Profile Downloading:** Download an entire profile (all pages) or a single page.
- **Only-New Mode:** Option to download only new posts, stopping or skipping already downloaded files.
- **Download Modes:**  
  - **Concurrent:** Multiple files are downloaded in parallel.  
  - **Sequential:** Files are downloaded one by one (useful for video downloads on Coomer to avoid incomplete files).
- **Custom File Naming:** Choose from several file naming modes.
- **Checksum Verification:** Optionally calculate and verify SHA256 checksums for file integrity.
- **Rate Limiting & Domain Concurrency:** Controls the rate of requests to the same domain.

## Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/Emy69/Coomer-cli.git
   ```

2. **Change to the project directory:**

   ```bash
   cd Coomer-cli
   ```

3. **(Optional) Create and activate a virtual environment, then install dependencies:**

   ```bash
   python -m venv venv
   source venv/bin/activate   # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

## Usage

Run the CLI tool by specifying the URL of the profile you wish to download media from along with the desired options. If no arguments are provided, the script will display the help message explaining all options.

```bash
python coomer.py URL [options]
```

## Examples

- **Download images from a profile:**

  ```bash
  python coomer.py https://coomer.su/onlyfans/user/12345 -t images
  ```

- **Download videos in sequential mode (recommended for Coomer):**

  ```bash
  python coomer.py https://coomer.su/onlyfans/user/12345 -t videos -sv
  ```

- **Download only new posts from a profile:**

  ```bash
  python coomer.py https://kemono.party/fanbox/user/67890 -n
  ```

- **Download the entire profile with 5 workers:**

  ```bash
  python coomer.py https://coomer.su/onlyfans/user/12345 -e -w 5
  ```

## Command-Line Options

### Required

- **`url`**  
  The complete URL to download media from.  
  _Example:_ `https://coomer.su/onlyfans/user/12345`

### Download Options

- **`-d, --download-dir`**  
  Download directory (default: `./downloads`).

- **`-t, --file-type`**  
  File type to filter downloads. Options:  
  - `all` (default)  
  - `images`  
  - `videos`  
  - `documents`  
  - `compressed`  
  - `others`

- **`-p, --post-ids`**  
  Comma-separated list of post IDs to download.  
  _Example:_ `123,124,125`

- **`-e, --entire-profile`**  
  Download the entire profile (all pages) instead of a single post or page.

- **`-n, --only-new`**  
  Download only new posts. Stops at the first existing file unless the next option is used.

- **`-x, --continue-existing`**  
  In only-new mode, skip existing files instead of stopping.

- **`-k, --verify-checksum`**  
  Calculate and verify SHA256 checksums of downloaded files.

- **`-sv, --sequential-videos`**  
  Force sequential mode for videos (recommended for Coomer to ensure complete downloads).

### Performance & Networking Options

- **`-w, --workers`**  
  Maximum number of threads for parallel downloads (default: 5).  
  > **Note:** In sequential mode (or if sequential mode is forced via `-sv`), this option is ignored.

- **`-r, --rate-limit`**  
  Minimum interval (in seconds) between requests to the same domain (default: 2.0).

- **`-c, --concurrency`**  
  Maximum number of concurrent requests per domain (default: 2).

- **`-dm, --download-mode`**  
  Choose between:  
    - `concurrent` for parallel downloads (default)  
    - `sequential` for single sequential downloads

- **`-fn, --file-naming-mode`**  
  File naming mode options:  
    - `0`: Original name + index  
    - `1`: Post title + index + short MD5 hash  
    - `2`: Post title - postID_index  
  (default: `0`).

### Miscellaneous Options

- **`-v, --verbose`**  
  Enable debug logging.

## Contributing

Contributions are welcome! Feel free to fork the repository and submit pull requests for improvements or bug fixes.

## Support My Work

If you find this tool helpful, please consider supporting my efforts:

[![Donate with PayPal](https://img.shields.io/badge/Donate-PayPal-blue.svg?logo=paypal&style=for-the-badge)](https://www.paypal.com/paypalme/Emy699)  
[![Buy Me a Coffee](https://img.shields.io/badge/Buy%20Me%20a%20Coffee-FFDD00.svg?style=for-the-badge&logo=buy-me-a-coffee&logoColor=black)](https://buymeacoffee.com/emy_69)  
[![Support on Patreon](https://img.shields.io/badge/Support%20on%20Patreon-FF424D.svg?style=for-the-badge&logo=patreon&logoColor=white)](https://www.patreon.com/emy69)