# from time import sleep
from util import *
import os.path
# import sys
import csv
import re


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
csv_header = ['ngram', 'year', 'match_count', 'volume_count']

indices = None
log_indices = []
log_line = 0
log_file = "./log.txt"
if os.path.isfile(log_file):
    with open(log_file, 'r') as f:
        lines = [line.strip('\n').split() for line in f.readlines()]

    # Extract unique indices from the log file
    seen = set()
    seen_add = seen.add
    log_indices = [line[0] for line in lines if not (line[0] in seen or seen_add(line[0]))]
    indices = [i for i in get_indices(n=5) if i not in log_indices[:-1]]

    if len(lines):
        log_line = int(lines[-1][-1])

    if log_line > 0:
        print("Last logged line number", log_line)

# Remove POS tags in the text
pos_tags = ['ADJ', 'ADP', 'ADV', 'CONJ', 'DET', 'NOUN', 'NUM', 'PRON', 'PRT', 'VERB']
remove = '|'.join(pos_tags)
pa = re.compile(r'\b(' + remove + r')\b\s*')

print("Indices:", indices)
streamer = NgramStreamer(lang='eng-us', n=5, ver='20120701', idx=indices, stream=False)

for meta, record in streamer.iter_collection():
    # Skipping records until the last logged if necessary
    num = record.line_number
    if len(log_indices) and log_line and meta[-1] == log_indices[-1] and num <= log_line:
            continue

    s = record.ngram
    res = pa.sub('', s.replace('_X', '').replace('_', '')).lower()

    for key in target_dict:
        # Log ngram match in the given group
        csv_file = csv_template.format(group=key).replace(' ', '_')
        file_exists = os.path.isfile(csv_file)
        with open(csv_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=csv_header, delimiter='\t')
            if not file_exists:
                writer.writeheader()

            for tag in target_dict[key]:
                if res.startswith(tag) or res.endswith(tag):
                    writer.writerow({'ngram': res,
                                     'year': record.year,
                                     'match_count': record.match_count,
                                     'volume_count': record.volume_count})

    if num % 5000 == 1:
        print("Index: {}, processed {}.".format(meta[-1], num))
        with open(log_file, 'a') as f:
            # The last entry in the meta contains the index
            f.write(" ".join((meta[-1], str(num))) + "\n")

print("Complete!")
