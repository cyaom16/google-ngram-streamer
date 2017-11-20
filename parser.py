from util import get_indices, NgramStreamer, KillerHandler
# import multiprocessing as mp
import os.path
import sys
import csv
import re


LOG_FILE = "./log.txt"


def save_checkpoint(match_dict, log_fname, index, line):
    """
        CSV batch writer for match records and log
    """
    template = "./ngram_match/ngram_match_{group}.csv"
    header = ['ngram', 'year', 'match_count', 'volume_count']

    for group in match_dict:
        match_set = match_dict[group]
        if match_set:
            fname = template.format(group=group).replace(' ', '_')
            with open(fname, 'a', newline='') as csv_file:
                csv_writer = csv.DictWriter(csv_file, fieldnames=header, delimiter='\t')
                if not os.path.isfile(fname):
                    csv_writer.writeheader()
                for match_record in match_set:
                    csv_writer.writerow({'ngram': match_record[0],
                                         'year': match_record[1],
                                         'match_count': match_record[2],
                                         'volume_count': match_record[3]})
            match_set.clear()

    with open(log_fname, 'a') as log_file:
        log_file.write(" ".join((index, str(line))) + "\n")


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

# Initialize a set collection of match results
result_dict = dict.fromkeys(target_dict, set())


# Log parser
log_indices = []
log_line = 0
if os.path.isfile(LOG_FILE):
    print("Log found:\n")
    with open(LOG_FILE, 'r') as f:
        lines = [line.strip('\n').split() for line in f]

    if len(lines):
        # Extract unique indices from the log file
        seen = set()
        seen_add = seen.add
        log_indices = [line[0] for line in lines if not (line[0] in seen or seen_add(line[0]))]
        print("Indices logged:\n", log_indices)
        log_line = int(lines[-1][-1])
        print("Last record: index '{}', line {}.\n".format(log_indices[-1], log_line))
    else:
        print("Empty log.")
else:
    print("No log exists.")


# Remove POS tags in the n-gram text
pa = re.compile(r'_[^\s]+')

# Must include the last logged index in indices to-do
indices = [index for index in get_indices(n=5) if index not in log_indices[:-1]]
print("Indices to-do:\n", indices)

streamer = NgramStreamer(lang='eng-us', n=5, ver='20120701', idx=indices, stream=False)

prev_index = log_indices[-1]
prev_line = log_line
try:
    for meta, record in streamer.iter_collection():
        curr_line = record.line
        curr_index = meta[-1]

        # Skipping records until the last logged if necessary
        if len(log_indices) > 0 and log_line > 0:
            if curr_index == log_indices[-1] and curr_line <= log_line:
                continue

        # Save checkpoint at transition
        if curr_index != prev_index and curr_line == 1:
            save_checkpoint(result_dict, LOG_FILE, prev_index, prev_line)
            print("Index '{}' completed with total {} records.".format(prev_index, prev_line))

        # Modify record in tuple for hashing
        rec = (pa.sub('', record.ngram).lower(), record.year, record.match_count, record.volume_count)

        # Looking for match targets
        for group in target_dict:
            for tag in target_dict[group]:
                if rec[0].startswith(tag) or rec[0].endswith(tag):
                    result_dict[group].add(rec)

        # Save checkpoint every 500,000 lines
        if curr_line % 500000 == 1:
            save_checkpoint(result_dict, LOG_FILE, curr_index, curr_line)
            print("Index: '{}', processed {}.".format(curr_index, curr_line))

        # Kill signal handler
        if killer.kill_now:
            save_checkpoint(result_dict, LOG_FILE, curr_index, curr_line)
            print("Kill received. Index: '{}', line: {}.".format(curr_index, curr_line))
            sys.exit(0)

        prev_index = curr_index
        prev_line = curr_line

    print("Complete! Index '{}' with total {} records".format(prev_index, prev_line))

except Exception as e:
    print("\n--> Error: {}."
          "\nCheckpoint saved. Index: '{}', line: {}.".format(e.args[0], prev_index, prev_line))
    pass

save_checkpoint(result_dict, LOG_FILE, prev_index, prev_line)

