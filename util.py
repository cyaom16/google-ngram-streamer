from string import ascii_lowercase, digits
from itertools import product
import multiprocessing as mp
import requests
import signal
import gzip
import time
import csv
import os
import re


def get_indices(language='eng', gram_size=1):
    """ Get the whole list of indices for the selected collection

    Exclusions:
    4-gram:
        Only British English 4-gram collection do not have 'qz'

    5-gram:
        All 5-gram collections do not have index 'qk'
        American English: 'qz'
        British English:  'gq','lq','qg','qh','qs','vq','wq','xb','xq','xw','zq','zt','zz'

    Arguments
        language:  Language
        gram_size: N-gram size

    Returns
        sorted list of indices
    """
    others = ['other', 'punctuation']

    if gram_size == 1:
        letters = list(ascii_lowercase)
        others += ['pos']
    else:
        # All letter combinations
        letters = [''.join(i) for i in product(ascii_lowercase, '_' + ascii_lowercase)]

        if gram_size == 4 and language == 'eng-gb':
            letters.remove('qz')

        if gram_size == 5:
            letters.remove('qk')
            if language == 'eng-gb':
                gb5_exclude = ['gq', 'lq', 'qg', 'qh', 'qs', 'vq', 'wq', 'xb', 'xq', 'xw', 'zq',
                               'zt', 'zz']
                letters = [i for i in letters if i not in gb5_exclude]
            elif language == 'eng-us':
                letters.remove('qz')

        others += ['_ADJ_', '_ADP_', '_ADV_', '_CONJ_', '_DET_',
                   '_NOUN_', '_NUM_', '_PRON_', '_PRT_', '_VERB_']

    return sorted(list(digits) + letters + others)


class NgramStreamer(object):
    """ Google Ngram streamer

    Arguments
        language:  Language
        gram_size: N-gram size
        indices:   Indices
    """

    # Class constants
    version = '20120701'

    def __init__(self, language='eng', gram_size=1, indices=None):
        self.language = language
        self.gram_size = gram_size
        self.indices = indices
        if self.indices is None:
            self.indices = get_indices(language=self.language, gram_size=self.gram_size)

        self.curr_index = None

    def iter_index(self):
        """ Generator of index file

        Returns
            file:     Index file name
            response: Response object
        """
        session = requests.Session()

        url_template = 'http://storage.googleapis.com/books/ngrams/books/{}'
        file_template = 'googlebooks-{lang}-all-{n}gram-{ver}-{idx}.gz'

        for index in self.indices:
            # Current index in progress
            self.curr_index = index
            file_name = file_template.format(lang=self.language,
                                        n=self.gram_size,
                                        ver=self.version,
                                        idx=self.curr_index)
            url = url_template.format(file_name)
            try:
                response = session.get(url, stream=True)
                assert response.status_code == 200
                yield file_name, response
            except AssertionError:
                print("Unable to connect to", url)
                continue

    def iter_content(self, chunk_size=1024**2):
        """ Generator of file content

        Arguments
            chunk_size: Size of chunk to be read into the buffer

        Returns
            chunk: A block of data (lines) generated
        """
        for file, response in self.iter_index():
            file_path = os.path.join('data', file)
            # Download data for offline processing to avoid requests timeout
            if not os.path.isfile(file_path):
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        f.write(chunk)
            with gzip.open(file_path, 'rt') as f:
                chunk = f.read(chunk_size)
                while chunk:
                    # Since chunk could be ended in the middle of a line, we need to read a extra
                    # line with a newline character which ensures the lines are intact
                    chunk += f.readline()
                    yield chunk
                    chunk = f.read(chunk_size)

    def iter_record(self):
        """ Generator of line data

        Outputs
            count:  Line number
            record: Line record
        """
        count = 0
        for chunk in self.iter_content():
            records = chunk.splitlines()
            for record in records:
                count += 1
                yield count, record


class NgramParser(NgramStreamer):
    """ Parse the ngram records to filter out match targets (producer-consumer workflow)

    Arguments
        language:  Language
        gram_size: N-gram size
        version:   Version
        indices:   Indices
        max_size:  Max size for consumer queue
    """

    # Class constants
    targets = {'labour':        ('labour party',),
               'liberal':       ('liberal party',),
               'conservative':  ('conservative party',),
               'republican':    ('republican', 'republicans', 'gop'),
               'democrat':      ('democrat', 'democrats', 'democratic party'),
               'communism':     ('communism', 'communist', 'communists'),
               'mccarthyism':   ('mccarthyism',),
               'feminism':      ('feminism', 'feminist'),
               'technology':    ('technology',),
               'science':       ('science',),
               'economics':     ('economics',),
               'war':           ('war',),
               'computer':      ('computer', 'computers'),
               'electricity':   ('electricity',),
               'steam_engine':  ('steam engine', 'steam engines'),
               'socialism':     ('socialism', 'socialist', 'socialists'),
               'colonialism':   ('colonialism', 'colonialist', 'colonialists'),
               'fascism':       ('fascism', 'fascist', 'fascists'),
               'protectionism': ('protectionism', 'protectionist', 'protectionists')}

    pattern = re.compile(r'_[^\s]+')

    def __init__(self, language='eng', gram_size=1, indices=None, max_size=1000):
        super(NgramParser, self).__init__(language=language,
                                          gram_size=gram_size,
                                          indices=indices)
        manager = mp.Manager()
        self.queue = manager.Queue(maxsize=max_size)

    def parser(self, chunk):
        """ Producer function (worker): parse and match the records line by line with the
        targets. The results are inserted into the Manager's queue for consumption.

        Arguments
            chunk: A block of data (lines) yield from iter_content
        """
        records = chunk.splitlines()
        for record in records:
            data = record.split('\t')

            # For each record we remove the POS taggers attached to the words
            # e.g. Technology_NOUN -> Technology
            ngram_stem = self.pattern.sub('', data[0]).strip()
            if not ngram_stem:
                continue

            ngram_text = ngram_stem.lower()

            for group in self.targets:
                for tag in self.targets[group]:
                    if ngram_text.startswith(tag) or ngram_text.endswith(tag):
                        self.queue.put((group, data))
        return

    def writer(self):
        """ Consumer function (overseer): load the match records released from the Manager's
        queue to the corresponding CSV groups.
        """
        header = ['ngram', 'year', 'match_count', 'volume_count']
        csv_path = 'match_{lang}_{n}gram'.format(lang=self.language, n=self.gram_size)
        template = os.path.join(csv_path, '{group}.csv')

        if not os.path.exists(csv_path):
            print("Creating CSV directory:", csv_path)
            os.makedirs(csv_path)

        print("Writer PID: {}, PPID: {}".format(os.getpid(), os.getppid()))

        while True:
            item = self.queue.get()
            if item == 'kill':
                print("Kill received. Queue terminated")
                self.queue.task_done()
                break
            key, data = item
            file_name = template.format(group=key)
            with open(file_name, 'a') as f:
                csv_writer = csv.DictWriter(f, fieldnames=header, delimiter='\t')
                if not os.path.isfile(file_name):
                    csv_writer.writeheader()
                csv_writer.writerow({'ngram': data[0],
                                     'year': data[1],
                                     'match_count': data[2],
                                     'volume_count': data[3]})
            self.queue.task_done()

    @staticmethod
    def logger(file_name, index):
        with open(file_name, 'a') as f:
            f.write(index + "\n")
        print("Index '{}' logged".format(index))

    def run_async(self, pool_size=1, job_limit=5000):
        """ Run asynchronous with multiprocessing backend

        Setting up the job_limit is a good practice that caps the task queue of the pool within
        certain limit, to prevent out-of-memory problem in long running processes.

        Arguments
            pool_size: Number of processes in the pool
            job_limit: Limit of jobs in the pool
        """
        pool = mp.Pool(pool_size)

        # Spawn writer process
        overseer = pool.apply_async(self.writer)

        prev_index = ''
        log_file = 'log_{lang}_{n}gram.txt'.format(lang=self.language, n=self.gram_size)

        jobs = []
        for i, chunk in enumerate(self.iter_content()):
            # Log once an index has been processing in the pool
            if prev_index != self.curr_index:
                self.logger(log_file, prev_index)

            # Spawn parser processes
            job = pool.apply_async(self.parser, (chunk,))
            jobs.append(job)

            # Every 1000-chunk takes about 1G of data
            if i % 1000 == 1:
                print("Processed {} chunks".format(i))

            # Wait for pool task queue size below the job limit threshold
            while pool._taskqueue.qsize() > job_limit:
                print("Pool queue max out, waiting...")
                # If we wait for the last job to finish, it may take longer
                # job.wait()
                time.sleep(250)
                print("Total {} jobs currently in the pool.".format(pool._taskqueue.qsize()))

            prev_index = self.curr_index

        for job in jobs:
            job.get()

        # Writer queue 'kill' signal
        self.queue.put('kill')

        pool.close()
        pool.join()

        self.logger(log_file, prev_index)


class GraceKiller(object):
    def __init__(self):
        """ System Signal handler
        """
        self.kill_now = False
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        self.kill_now = True
