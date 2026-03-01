import time
from uuid import uuid4
from files import HyperscrapeFile
import os
from tqdm import tqdm
import argparse

parser = argparse.ArgumentParser(
                    prog='Filelist Generator',
                    description='Generates a filelist for Hyperscrape\'s coordinator',
                    epilog='Created by Hackerdude for Minerva')
parser.add_argument("myrient_index", help="Path to the index to generate file objects for", type=argparse.FileType('r'))
parser.add_argument("--ignore_file_list", help="Path to a list of files to ignore (in find command output format)", type=argparse.FileType('r'), action='append')
parser.add_argument("-r", "--reset", help="Reset and clear filelist", action="store_true")
args = parser.parse_args()

if (args.reset):
    print("Clearing files")
    os.remove("./file_state.bin")
    os.remove("./chunk_state.bin")
    os.remove("./file_hashes.bin")

import state

# Parse the ignore lists
print("Parsing ignore lists...")
ignore_lists: list[set] = []
for ignore_list in args.ignore_file_list:
    ignore_lists.append(set())
    print(f"Parsing {ignore_list.name}")
    start = time.time()
    pbar = tqdm()
    for line in iter(ignore_list.readline, ''): # Using iter is faster
        if (line == "."):
            line = line[1:]
        ignore_lists[-1].add(line.strip())
        pbar.update(1)
    pbar.close()

print("Parsing main list...")
full_list = set()
file_sizes = {}
pbar = tqdm()
for line in iter(args.myrient_index.readline, ''):
    split = line.strip().split(' ')
    path = ''.join(('./', ' '.join(split[1:])))
    full_list.add(path)
    file_sizes[path] = split[0]
    del split
    pbar.update(1)
pbar.close()

print("Subtracting sets")
for ignore_list in ignore_lists:
    full_list -= ignore_list

print("Generating list of files")
for file_path in tqdm(full_list):
    file_id = str(uuid4())
    state.files[file_id] = HyperscrapeFile(
        file_id,
        file_path,
        int(file_sizes[file_path]),
        f"https://myrient.erista.me/files/{file_path[2:]}",
        (1024*1024)*50 # 50MB chunks
    )
    state.file_worker_counts[file_id] = 0
    state.sorted_downloadable_files.append(file_id)

print("Saving data files...")
state.save_data_files()