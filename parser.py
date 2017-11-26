from util import get_indices, NgramStreamer, KillerHandler
# import multiprocessing as mp
import os.path
import sys
import csv
import re


CSV_HEADER = ['ngram', 'year', 'match_count', 'volume_count']
LOG_FILE = 'log.txt'


def save_checkpoint(filename, index, line):
    """
    Progress logger
    """
    with open(filename, 'a') as f:
        f.write(" ".join((index, str(line))) + "\n")


def save_results(result_dict, header=None):
    """
    CSV batch writer for match records
    """
    assert isinstance(result_dict, dict)

    template = os.path.join('ngram_match', 'ngram_match_{group}.csv')
    for key in result_dict:
        match_set = result_dict[key]
        if match_set:
            filename = template.format(group=key).replace(' ', '_')
            with open(filename, 'a') as f:
                csv_writer = csv.DictWriter(f, fieldnames=header, delimiter='\t')
                if not os.path.isfile(filename):
                    csv_writer.writeheader()
                for match_record in match_set:
                    csv_writer.writerow({'ngram': match_record[0],
                                         'year': match_record[1],
                                         'match_count': match_record[2],
                                         'volume_count': match_record[3]})
            match_set.clear()


killer = KillerHandler()


target_groups = {'labour': ('labour party',),
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
match_results = {key: set() for key in target_groups}


# Log parser
log_indices = []
log_line = 0
if os.path.isfile(LOG_FILE):
    print("Log found:\n")
    with open(LOG_FILE, 'r') as log:
        lines = [line.strip('\n').split() for line in log]

    if len(lines):
        # Extract unique indices from the log file
        seen = set()
        seen_add = seen.add
        log_indices = [line[0] for line in lines if not (line[0] in seen or seen_add(line[0]))]
        print("Indices logged:", log_indices)
        log_line = int(lines[-1][-1])
        print("Last record: Index '{}' Line {}".format(log_indices[-1], log_line))
    else:
        print("Empty log.")
else:
    print("No log exists.")


# Must include the last logged index in indices to-do
indices = [index for index in get_indices(n=5) if index not in log_indices[:-1]]
print("Indices to-do from: '{}' to '{}'.".format(indices[0], indices[-1]))

streamer = NgramStreamer(lang='eng-us', n=5, ver='20120701', idx=indices, stream=False)

prev_index = indices[0]
prev_line = log_line

offset = True if len(log_indices) > 0 and log_line > 0 else False
if offset:
    print("Jumping records...")

# Remove POS tags in the n-gram text
pa = re.compile(r'_[^\s]+')


try:
    for curr_index, record in streamer.iter_collection():
        curr_line = record.line

        # Skipping records until the last logged if necessary
        if offset and curr_index == log_indices[-1] and curr_line <= log_line:
            continue

        # Save checkpoint at transition
        if curr_index != prev_index and curr_line == 1:
            save_results(match_results, header=CSV_HEADER)
            save_checkpoint(LOG_FILE, prev_index, prev_line)
            print("Index '{}' completed with {} records.".format(prev_index, prev_line))

        # Modify record in tuple for hashing
        record_ngram = pa.sub('', record.ngram).strip()
        if not record_ngram:
            continue

        rec = (record_ngram.lower(), record.year, record.match_count, record.volume_count)

        # Looking for match targets
        for group in target_groups:
            for tag in target_groups[group]:
                if rec[0].startswith(tag) or rec[0].endswith(tag):
                    match_results[group].add(rec)

        # Save checkpoint every 500,000 lines
        if curr_line % 500000 == 1:
            save_results(match_results, header=CSV_HEADER)
            print("Index '{}' Line {}.".format(curr_index, curr_line))

        # Kill signal handler
        if killer.kill_now:
            save_results(match_results, header=CSV_HEADER)
            save_checkpoint(LOG_FILE, curr_index, curr_line)
            print("Kill received. Index '{}' Line {}.".format(curr_index, curr_line))
            sys.exit(0)

        prev_index = curr_index
        prev_line = curr_line

    print("Complete! Index '{}' with {} records".format(prev_index, prev_line))

except Exception as e:
    print("\n--> Error:", e.args[0])
    pass

save_results(match_results, header=CSV_HEADER)
save_checkpoint(LOG_FILE, prev_index, prev_line)
print("Checkpoint saved. Index '{}' Line {}.".format(prev_index, prev_line))
