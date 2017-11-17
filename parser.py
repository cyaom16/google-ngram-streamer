# from time import sleep
from util import get_indices, NgramStreamer, KillerHandler
import os.path
import sys
import csv
import re


LOG_FILE = "./log.txt"


def write_log(file, index, line):
    with open(file, 'a') as f:
        f.write(" ".join((index, str(line))) + "\n")
    return


killer = KillerHandler()

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

log_indices = []
log_line = 0

if os.path.isfile(LOG_FILE):
    print("Log found:\n")
    with open(LOG_FILE, 'r') as f:
        lines = [line.strip('\n').split() for line in f.readlines()]

    if len(lines):
        # Extract unique indices from the log file
        seen = set()
        seen_add = seen.add
        log_indices = [line[0] for line in lines if not (line[0] in seen or seen_add(line[0]))]
        print("Indices logged:", log_indices)
        log_line = int(lines[-1][-1])
        print("Last logged index: {} at line: {}".format(log_indices[-1], log_line))
    else:
        print("Empty log.")
else:
    print("No log exists.")

# Remove POS tags in the n-gram text
pos_tags = ['ADJ', 'ADP', 'ADV', 'CONJ', 'DET', 'NOUN', 'NUM', 'PRON', 'PRT', 'VERB']
remove = '|'.join(pos_tags)
pa = re.compile(r'\b(' + remove + r')\b\s*')

# Must include the last logged index in indices to-do
indices = [i for i in get_indices(n=5) if i not in log_indices[:-1]]
print("Indices to-do:", indices)

streamer = NgramStreamer(lang='eng-us', n=5, ver='20120701', idx=indices, stream=False)

prev_index = log_indices[-1]
prev_line = log_line
for meta, record in streamer.iter_collection():
    curr_line = record.line_number
    curr_index = meta[-1]

    # Skipping records until the last logged if necessary
    if len(log_indices) > 0 and log_line > 0:
        if curr_index == log_indices[-1] and curr_line <= log_line:
            continue

    # Log the previous index and its last line (total lines)
    if curr_index != prev_index and curr_line == 1:
        print("Previous index {} completed with total {} records.".format(prev_index, prev_line))
        write_log(LOG_FILE, prev_index, prev_line)

    s = record.ngram
    res = pa.sub('', s.replace('_X', '').replace('_', '')).lower()

    for key in target_dict:
        # Log ngram match in each given group
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
    # Log rate: every 10000 lines
    if curr_line % 10000 == 1:
        print("Index: {}, processed {}.".format(curr_index, curr_line))
        write_log(LOG_FILE, curr_index, curr_line)

    # Kill signal handler interrupt, write log upon received
    if killer.kill_now:
        print("Kill received. Index: {} line: {}.".format(curr_index, curr_line))
        write_log(LOG_FILE, curr_index, curr_line)
        sys.exit(0)

    prev_index = curr_index
    prev_line = curr_line

print("Complete!")

