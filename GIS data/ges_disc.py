import os
import sys
# Get the absolute path of the current script's directory
current_script_dir = os.path.dirname(os.path.abspath(__file__))
# Get the parent directory's path
parent_dir = os.path.join(current_script_dir, os.pardir)
# Add the parent directory to sys.path
sys.path.append(parent_dir)

print(sys.path)

import subprocess
from config.config import earthdata_config

def download_gesdisc_imerg(
    urls_txt,
    target_dir,
    username,
    password,
    cookie_file=os.path.expanduser("~/.urs_cookies"),
    curl_path="curl"
):
    """
    Downloads NASA GES DISC (e.g., IMERG) data requiring Earthdata authentication,
    using curl instead of wget.

    Parameters
    ----------
    urls_txt : str
        Path to text file with URLs (one per line).
    target_dir : str
        Directory to save downloaded files.
    username : str
        Your Earthdata username.
    password : str
        Your Earthdata password.
    cookie_file : str, optional
        Path to cookie file (default ~/.urs_cookies).
    curl_path : str, optional
        Path to curl executable.
    """

    os.makedirs(target_dir, exist_ok=True)
    os.makedirs(os.path.dirname(cookie_file), exist_ok=True)

    # Ensure cookie file exists
    if not os.path.exists(cookie_file):
        open(cookie_file, "a").close()

    # Base curl command template
    base_cmd = [
        curl_path,
        "-n",  # use .netrc if present (optional)
        "-L",  # follow redirects
        "--cookie", cookie_file,
        "--cookie-jar", cookie_file,
        # "--user", f"{username}:{password}",
        "--fail",  # fail on HTTP errors
        "--remote-name",  # save using the remote filename
        "--remote-header-name",  # use server-provided name if available
        "--insecure",  # skip SSL check if needed
    ]

    # Read URLs from file
    with open(urls_txt, "r") as f:
        urls = [line.strip() for line in f if line.strip() and not line.startswith("#")]

    for url in urls:
        print(f"➡️  Downloading: {url}")
        cmd = base_cmd + ["--output-dir", target_dir, url]
        try:
            result = subprocess.run(cmd, check=False)
            if result.returncode != 0:
                print(f"⚠️  Warning: curl returned code {result.returncode} for {url}")
        except Exception as e:
            print(f"❌ Error while downloading {url}: {e}", file=sys.stderr)

    print("\n✅ All downloads attempted. Check above messages for any failures.")


if __name__ == "__main__":
    # Example usage — replace with your info
    urls_txt = r"GIS data\subset_GPM_3IMERGHH_07_20251030_095849_.txt"
  # File containing IMERG download URLs
    target_dir = r'E:/Air Quality/GIS'
    username = earthdata_config.EARTHDATA_USERNAME
    password = earthdata_config.EARTHDATA_PASSWORD

    download_gesdisc_imerg(urls_txt, target_dir, username, password)
