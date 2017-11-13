from util import *
from time import sleep
import os.path
import csv
import re


pos_tags = ['ADJ', 'ADP', 'ADV', 'CONJ', 'DET',
            'NOUN', 'NUM', 'PRON', 'PRT', 'VERB']

remove = '|'.join(pos_tags)
pa = re.compile(r'\b(' + remove + r')\b\s*')


"""
     Labour party
     Liberal party
     Conservative party
     Republican/Republicans/GOP
     Democrat/Democrats/Democratic party
     Communism/communist/communists
     McCarthyism
     Feminist/Feminism
     Technology
     Science
     Economics
     War
     computer/computers
     electricity
     steam engine/steam engines
     socialism/socialist/socialists
     colonialism/colonialist/colonialists
     fascism/fascist/fascists
     protectionism/protectionist/protectionists
"""
target_dict = {'labour': ('labour party',),
               'liberal': ('liberal party',),
               'conservative': ('conservative party',),
               'republican': ('republican', 'republicans', 'gop'),
               'democrat': ('democrat', 'democrats', 'democratic party'),
               'communism': ('communism', 'communist', 'communists'),
               'mccarthyism': ('mccarthyism',),
               'feminism': ('feminism', 'feminist'),
               'technology': ('technology',),
               'science': ('science',),
               'economics': ('economics',),
               'war': ('war',),
               'computer': ('computer', 'computers'),
               'electricity': ('electricity',),
               'steam engine': ('steam engine', 'steam engines'),
               'socialism': ('socialism', 'socialist', 'socialists'),
               'colonialism': ('colonialism', 'colonialist', 'colonialists'),
               'fascism': ('fascism', 'fascist', 'fascists'),
               'protectionism': ('protectionism', 'protectionist', 'protectionists')}

csv_template = "./ngram_match/ngram_match_{group}.csv"
header = ['ngram', 'year', 'match_count', 'volume_count']


streamer = NgramStreamer(lang='eng-us', n=5, ver='20120701')

log_file = "./log.txt"
file_exists = os.path.isfile(log_file)
if file_exists:
    with open(log_file, 'r') as f:
        log = [x.strip('\n') for x in f.readlines()]
    seen = set()
    seen_add = seen.add
    for i in log:
        idx, line = i.split()
    unique_idx = [line.split()[0] for line in log if not (line[0] in seen or seen_add(x))]

for record in streamer.stream_collection():
    # print(u'{ngram}\t{year}\t{match_count}\t{volume_count}'.format(**record._asdict()))
    s = record.ngram
    res = pa.sub('', s.replace('_', '')).lower()
    # print(res)

    for key in target_dict:
        csv_file = csv_template.format(group=key).replace(' ', '_')
        file_exists = os.path.isfile(csv_file)
        with open(csv_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=header, delimiter='\t')
            if not file_exists:
                writer.writeheader()

            for tag in target_dict[key]:
                if res.startswith(tag) or res.endswith(tag):
                    writer.writerow({'ngram': res,
                                     'year': record.year,
                                     'match_count': record.match_count,
                                     'volume_count': record.volume_count})
    if record.line % 1000 == 0:
        print("Already processed {}, and logged.".format(record.line + 1))
        with open(log_file, 'a') as f:
            f.write(" ".join((record.meta[-1], record.line)))



