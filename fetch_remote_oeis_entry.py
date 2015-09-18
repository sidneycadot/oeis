
import urllib.request
import time
import logging
import re

logger = logging.getLogger(__name__)

class OeisEntryEmptyError(Exception):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return self.message

def filter_main_content(text, oeis_id):

    # We are interested in the lines that start with a '%' followed by
    # a directive identification character, followed by a single space,
    # followed by an OEIS id ('Axxxxxx'), followed by directive data.
    #
    # The directive data will be either an empty string or a string
    # staring with a space character.

    directive_line_pattern = "(%.) A{oeis_id:06d}(.*)$".format(oeis_id = oeis_id)

    content = re.findall(directive_line_pattern, text, re.MULTILINE)

    if len(content) == 0:
        raise OeisEntryEmptyError("no valid content lines found")

    content = "\n".join(directive + directive_data for (directive, directive_data) in content)

    return content

class FetchResult:
    def __init__(self, oeis_id, timestamp, main_content, bfile_content):
        self.oeis_id       = oeis_id
        self.timestamp     = timestamp
        self.main_content  = main_content
        self.bfile_content = bfile_content

def fetch_remote_oeis_entry(oeis_id, fetch_bfile_flag):

    # We fetch a raw version of the OEIS entry, which is easy to parse.

    main_url  = "http://oeis.org/search?q=id:A{oeis_id:06d}&fmt=text".format(oeis_id = oeis_id)
    bfile_url = "http://oeis.org/A{oeis_id:06d}/b{oeis_id:06d}.txt".format(oeis_id = oeis_id) if fetch_bfile_flag else None

    urls = (main_url, bfile_url)

    timestamp = time.time()

    # Try to fetch the main page and the b-file, if requested.

    url_response_data = []

    for url in urls:
        if url is not None:
            with urllib.request.urlopen(url) as response:
                response_data = response.read().decode(response.headers.get_content_charset())
        else:
            response_data = None
        url_response_data.append(response_data)

    (main_content, bfile_content) = url_response_data

    main_content = filter_main_content(main_content, oeis_id)

    return FetchResult(oeis_id, timestamp, main_content, bfile_content)
