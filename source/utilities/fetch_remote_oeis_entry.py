"""Functionality to fetch a remote OEIS entry, optionally including its associated b-file."""

import urllib.request
import time
from typing import NamedTuple, Optional


class BadOeisResponse(Exception):
    """This exception is raised when a network fetch of an OEIS entry fails."""


class FetchResult(NamedTuple):
    """This class represents the result of `a fetch_remote_oeis_entry` call."""
    oeis_id: int
    timestamp: float
    main_content: str
    bfile_content: Optional[str]


def _fetch_url(url: str) -> str:
    """Fetch the given URL as a string."""
    with urllib.request.urlopen(url, timeout=60.0) as response:
        raw = response.read()
    decoded = raw.decode(response.headers.get_content_charset() or 'utf-8')
    return decoded

def strip_main_content(content: str) -> bool:
    """Check if the main content of an OEIS entry appears to be okay.

    A properly formatted content as obtained from the server has 5 header lines, content, and 2 footer lines:

      lines[0]     # Greetings from The On-Line Encyclopedia of Integer Sequences! http://oeis.org/
      lines[1]     (empty line)
      lines[2]     Search: id:aNNNNNN (where the six characters NNNNNN are the numerical OEIS ID).
      lines[3]     Showing 1-1 of 1
      lines[4]     (empty line)
      lines[5:-2]  --- actual content directives are here ---
      lines[-2]    (empty line)
      lines[-1]    # Content is available under The OEIS End-User License Agreement: http://oeis.org/LICENSE

    All lines (including the last one) are terminated with a single newline character.

    Here, we check the fourth line to assess the correctness of the response. If okay, we strip the first five and
    last two lines, and return the result.
    """

    # The 'splitlines' method splits both on \n and on \u2028.
    # We keep the line endings so we can do perfect reconstruction after removinbg the header and footer.
    lines = content.splitlines(keepends=True)

    if lines[3] != "Showing 1-1 of 1\n":
        raise ValueError()

    lines = lines[5:-2]
    return "".join(lines)


def fetch_remote_oeis_entry(oeis_id: int, fetch_bfile_flag: bool) -> FetchResult:
    """Fetch OEIS entry main file and (optionally) the associated b-file."""

    # We fetch a raw version of the OEIS entry, which is easiest to parse.

    main_url  = "http://oeis.org/search?q=id:A{oeis_id:06d}&fmt=text".format(oeis_id = oeis_id)
    bfile_url = "http://oeis.org/A{oeis_id:06d}/b{oeis_id:06d}.txt".format(oeis_id = oeis_id)

    timestamp = time.time()

    main_content = _fetch_url(main_url)

    try:
        main_content = strip_main_content(main_content)
    except ValueError:
        raise BadOeisResponse("OEIS server response indicates failure (url: {})".format(main_url))

    bfile_content = _fetch_url(bfile_url) if fetch_bfile_flag else None

    return FetchResult(oeis_id, timestamp, main_content, bfile_content)
