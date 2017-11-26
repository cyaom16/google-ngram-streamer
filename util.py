from string import ascii_lowercase, digits
from collections import namedtuple
from itertools import product
import requests
import os.path
import signal
import zlib


def get_indices(n=1):
    """
    Get the list of indices for Google Ngram Viewer

    # Arguments
        n: N-gram size

    # Outputs
        sorted list of indices

    """
    assert isinstance(n, int) and 0 <= n <= 5

    others = ['other', 'punctuation']
    if n == 1:
        letters = list(ascii_lowercase)
        others += ['pos']
    else:
        letters = [''.join(i) for i in product(ascii_lowercase, '_' + ascii_lowercase)]
        # All 5-gram collections do not have index qk
        if n == 5:
            letters.remove('qk')
        others += ['_ADJ_', '_ADP_', '_ADV_', '_CONJ_', '_DET_',
                   '_NOUN_', '_NUM_', '_PRON_', '_PRT_', '_VERB_']

    return sorted(list(digits) + letters + others)


def iter_content(filename, chunk_size=1024**2):
    assert isinstance(chunk_size, int) and chunk_size > 0

    with open(filename, 'rb') as f:
        buffer = f.read(chunk_size)
        while buffer:
            yield buffer
            buffer = f.read(chunk_size)


class NgramStreamer(object):
    def __init__(self, lang='eng-us', n=1, ver='20120701', idx=None, stream=True):
        """
        Ngram streamer
        Generate Record(line_number, ngram, year, match_count, volume_count)

        # Arguments
            lang: Language (str)
            n:    N-gram size (int)
            ver:  Version (str)
            idx:  Index, or indices (list)

        # Outputs
            yield meta (list), Record (namedtuple)
        """
        self.language = lang
        self.ngram_size = n
        self.version = ver
        self.indices = idx
        self.stream = stream
        self.Record = namedtuple('Record', ['line', 'ngram', 'year', 'match_count', 'volume_count'])

    def iter_index(self):
        session = requests.Session()

        self.indices = get_indices(n=self.ngram_size) if self.indices is None else self.indices

        url_template = 'http://storage.googleapis.com/books/ngrams/books/{}'
        file_template = 'googlebooks-{lang}-all-{n}gram-{ver}-{idx}.gz'

        for index in self.indices:
            file = file_template.format(lang=self.language,
                                        n=self.ngram_size,
                                        ver=self.version,
                                        idx=index)
            url = url_template.format(file)

            try:
                response = session.get(url, stream=True)
                assert response.status_code == 200

                yield file, index, response

            except AssertionError:
                print("Unable to connect to {}...".format(url))
                continue

    def iter_collection(self, chunk_size=1024**2):
        for file, index, response in self.iter_index():
            if self.stream:
                compressed_chunks = response.iter_content(chunk_size=chunk_size)
            else:
                data_path = os.path.join('data', file)
                if not os.path.isfile(data_path):
                    with open(data_path, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            f.write(chunk)
                compressed_chunks = iter_content(data_path, chunk_size=chunk_size)

            dec = zlib.decompressobj(32 + zlib.MAX_WBITS)
            last = b''
            count = 0
            for chunk in compressed_chunks:
                block = dec.decompress(chunk)
                lines = (last + block).split(b'\n')
                lines, last = lines[:-1], lines[-1]
                for line in lines:
                    decoded_line = line.decode('utf-8')
                    data = decoded_line.split('\t')
                    assert len(data) == 4

                    count += 1
                    yield index, self.Record(count, data[0], *map(int, data[1:]))

            assert not last


class KillerHandler:
    def __init__(self):
        """
        System Signal handler
        """
        self.kill_now = False
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        self.kill_now = True

