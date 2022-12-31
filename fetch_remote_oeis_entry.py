"""Functionality to fetch a remote OEIS entry, optionally including its b-file."""

import urllib.request
import time
import logging
from typing import NamedTuple, Optional

logger = logging.getLogger(__name__)


class BadOeisResponse(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class FetchResult(NamedTuple):
    oeis_id: int
    timestamp: float
    main_content: str
    bfile_content: Optional[str]


def fetch_url(url: str) -> str:
    """Fetch URL as string."""
    with urllib.request.urlopen(url) as response:
        return response.read().decode(response.headers.get_content_charset() or 'utf-8')


def main_content_ok(content: str) -> bool:

    # A proper response has 5 header lines, content, and 2 footer lines:
    #
    # lines[0]     # Greetings from The On-Line Encyclopedia of Integer Sequences! http://oeis.org/
    # lines[1]     (empty line)
    # lines[2]     Search: id:aXXXXXX
    # lines[3]     Showing 1-1 of 1
    # lines[4]     (empty line)
    # lines[5:-2]  --- actual content directives are here ---
    # lines[-2]    (empty line)
    # lines[-1]    # Content is available under The OEIS End-User License Agreement: http://oeis.org/LICENSE
    #
    # We simply check the fourth line to assess the correctness of the response.

    lines = content.split("\n")
    content_ok = lines[3].strip() == "Showing 1-1 of 1"

    return content_ok


def fetch_remote_oeis_entry(oeis_id: int, fetch_bfile_flag: bool) -> FetchResult:

    # We fetch a raw version of the OEIS entry, which is easiest to parse.

    main_url  = "http://oeis.org/search?q=id:A{oeis_id:06d}&fmt=text".format(oeis_id = oeis_id)
    bfile_url = "http://oeis.org/A{oeis_id:06d}/b{oeis_id:06d}.txt".format(oeis_id = oeis_id)

    timestamp = time.time()

    main_content = fetch_url(main_url)

    if not main_content_ok(main_content):
        raise BadOeisResponse("OEIS server response indicates failure (url: {})".format(main_url))

    bfile_content = fetch_url(bfile_url) if fetch_bfile_flag else None

    return FetchResult(oeis_id, timestamp, main_content, bfile_content)
