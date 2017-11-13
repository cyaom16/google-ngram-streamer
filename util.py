from itertools import product, chain, groupby
from collections import namedtuple, Counter
from string import ascii_lowercase, digits
import requests
import zlib
# import sys


class NgramStreamer(object):
    #
    def __init__(self, lang='eng-us', n=1, ver='20120701', idx=None):
        #
        self.language = lang
        self.ngram_size = n
        self.version = ver
        self.index = idx
        self.meta = [self.language, self.ngram_size, self.version, self.index]

        self.Record = namedtuple('Record', ['line', 'ngram', 'year', 'match_count', 'volume_count', 'meta'])

    # Yield an iterable of indices
    def iter_index(self):
        if self.ngram_size == 1:
            letters = ascii_lowercase
            others = ['other', 'punctuation', 'pos']
        else:
            letters = [''.join(i) for i in product(ascii_lowercase, '_' + ascii_lowercase)]
            if self.ngram_size == 5:
                letters.remove('qk')

            others = ['_ADJ_', '_ADP_', '_ADV_', '_CONJ_', '_DET_',
                      '_NOUN_', '_NUM_', '_PRON_', '_PRT_', '_VERB_']

        return sorted(list(digits + letters) + others)

    #
    def iter_collection(self):
        session = requests.Session()

        self.index = self.iter_index() if self.index is None else self.index
        # if type(self.index) is not list:
        #     self.index = [self.index]

        # URL_TEMPLATE = 'http://localhost:8001/{}'
        url_template = 'http://storage.googleapis.com/books/ngrams/books/{}'
        file_template = 'googlebooks-{lang}-all-{n}gram-{ver}-{idx}.gz'

        for idx in self.index:
            file = file_template.format(lang=self.language, n=self.ngram_size, ver=self.version, idx=idx)
            url = url_template.format(file)
            self.meta[-1] = idx

            print("Downloading {}...".format(url))
            try:
                req = session.get(url, stream=True)
                assert req.status_code == 200

                yield file, url, req

            except AssertionError:
                print("Unable to connect to {}".format(url))
                continue

    def stream_collection(self, chunk_size=1024 ** 2):
        for file, url, req in self.iter_collection():
            count = 0
            dec = zlib.decompressobj(32 + zlib.MAX_WBITS)

            last = b''
            compressed_chunks = req.iter_content(chunk_size=chunk_size)

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
                    count += 1
                    yield self.Record(count, data[0], *map(int, data[1:]), self.meta)

            if last:
                print("Decompression abnormal")
                continue










