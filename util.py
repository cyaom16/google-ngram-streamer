from string import ascii_lowercase, digits
from collections import namedtuple
from itertools import product
# from time import sleep
import requests
import os.path
import zlib
# import sys


def get_indices(n=1):
    """
    Get the list of indices for Google Ngram Viewer


    # Arguments
        n: N-gram size

    # Outputs
        sorted list of indices

    """
    if n == 1:
        letters = list(ascii_lowercase)
        others = ['other', 'punctuation', 'pos']
    else:
        letters = [''.join(i) for i in product(ascii_lowercase, '_' + ascii_lowercase)]
        if n == 5:
            letters.remove('qk')
        others = ['_ADJ_', '_ADP_', '_ADV_', '_CONJ_', '_DET_',
                  '_NOUN_', '_NUM_', '_PRON_', '_PRT_', '_VERB_']

    return sorted(list(digits) + letters + others)


def iter_content(file, chunk_size=1024 ** 2):
    f = open(file, 'rb')
    buffer = f.read(chunk_size)
    while buffer:
        yield buffer
        buffer = f.read(chunk_size)
    # print("leftover")
    # yield d.flush()
    f.close()


class NgramStreamer(object):
    def __init__(self, lang='eng-us', n=1, ver='20120701', idx=None, stream=True):
        """
            Ngram streamer, yield generator record

            # Arguments
                lang: Language
                n:    N-gram size
                ver:  Version
                idx:  Index, or indices

            # Outputs
                yield meta, Record
        """
        self.language = lang
        self.ngram_size = n
        self.version = ver
        self.indices = idx
        self.stream = stream

        self.Record = namedtuple('Record', ['line_number', 'ngram', 'year', 'match_count', 'volume_count'])

    def iter_index(self):
        self.indices = get_indices(n=self.ngram_size) if self.indices is None else self.indices

        url_template = 'http://storage.googleapis.com/books/ngrams/books/{}'
        file_template = 'googlebooks-{lang}-all-{n}gram-{ver}-{idx}.gz'

        session = requests.Session()
        for idx in self.indices:
            file = file_template.format(lang=self.language, n=self.ngram_size, ver=self.version, idx=idx)
            url = url_template.format(file)
            meta = [self.language, self.ngram_size, self.version, idx]

            print("Downloading {}...".format(url))
            try:
                req = session.get(url, stream=self.stream)
                assert req.status_code == 200
                yield file, meta, req
            except AssertionError:
                print("Unable to connect to {}".format(url))
                continue

    def iter_collection(self, chunk_size=1024 ** 2):
        for file, meta, req in self.iter_index():
            if self.stream:
                compressed_chunks = req.iter_content(chunk_size=chunk_size)
            else:
                data_path = os.path.join("./data", file)
                if not os.path.isfile(data_path):
                    with open(data_path, 'wb') as f:
                        f.write(req.content)
                compressed_chunks = iter_content(data_path, chunk_size=chunk_size)

            dec = zlib.decompressobj(32 + zlib.MAX_WBITS)

            last = b''
            line_num = 0
            for chunk in compressed_chunks:
                block = dec.decompress(chunk)

                lines = (last + block).split(b'\n')
                lines, last = lines[:-1], lines[-1]

                for line in lines:
                    decoded_line = line.decode('utf-8')
                    data = decoded_line.split('\t')
                    if len(data) != 4:
                        print("Ngram data less than 4 fields!")
                        continue
                    line_num += 1
                    yield meta, self.Record(line_num, data[0], *map(int, data[1:]))

            if last:
                print("Decompression abnormal")
                continue
