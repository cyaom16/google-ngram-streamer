from util import get_indices, NgramParser
import os.path
# import time


lang = 'eng-gb'
n = 5
log_file = 'log_{lang}_{n}gram.txt'.format(lang=lang, n=n)

# Log parser
log_indices = []
if os.path.isfile(log_file):
    print("Log found:\n")
    with open(log_file, 'r') as log:
        log_indices = log.read().splitlines()
    print("Indices logged:", log_indices)
else:
    print("No log exists.")

indices = [index for index in get_indices(lang=lang, n=n) if index not in log_indices and index]

print("Starting")
# time1 = time.time()
indices_todo = indices[:5]
print("Indices todo", indices_todo)

parser = NgramParser(language=lang, ngram_size=n, indices=indices_todo)
parser.run_async(pool_size=5)

# print("Time spent:", time.time() - time1)
