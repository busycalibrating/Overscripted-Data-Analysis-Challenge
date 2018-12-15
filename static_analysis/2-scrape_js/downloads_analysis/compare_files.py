#! /usr/bin/env python3
import sys
import hashlib
import numpy as np
import pandas as pd
import glob
from pathlib import Path
import pickle
from pypeln import asyncio_task as aio
from os import path


##### Specify Directories ######################################################
# Parent directory
STORAGE_DIR = "/mnt/Data/UCOSP_DATA"

# Input directory
#CLEANED_URL_LIST = path.join(STORAGE_DIR, "resources/full_url_list_parsed")
FULL_URL_LIST = "full_data.pickle"
CLEANED_FILES = path.join(STORAGE_DIR, ("js_source_files" + "/*"))
#ALL_FILES = path.join(STORAGE_DIR, "1st_batch_js_source_files")

# Output directory
OUTPUT_FILE = "final_processed.pickle"

##### Load in dataset ##########################################################
#parquet_dir = Path(CLEANED_URL_LIST)
#input_data_cleaned = pd.concat(
#    pd.read_parquet(parquet_file)
#    for parquet_file in parquet_dir.glob('*.parquet')
#)
print("Retrieving list from: \'{}\'".format(CLEANED_FILES))
input_data_cleaned = list(glob.glob(CLEANED_FILES))

with open(FULL_URL_LIST, 'rb') as handle:
    input_data_full = pickle.load(handle)

# Sanity check
print("\nThere are {} urls found in the cleaned dataset:\n\t\'{}\'".format(
    len(input_data_cleaned), CLEANED_FILES))
print("\nThere are {} hashes found in the complete dataset:\n\t\'{}\'".format(
    len(input_data_full), FULL_URL_LIST))

# Generating new dataframe
def get_hash_from_file(filename):

    sha1 = hashlib.sha1()

    with open(filename, 'r') as f:
        data = f.read()
        sha1.update(data.encode('utf-8'))

    return sha1.hexdigest()

output_success = []
output_fails = []
counter_success = 0
counter_failed = 0

for filename in input_data_cleaned:
    if counter_success % 5000 == 0:
        print("{}/{}".format(counter_success, len(input_data_cleaned)))

    # Get source url
    #print(filename)
    raw_filename = filename.split('/')[-1]

    # Try to get the file hash, only works with utf-8 at the moment
    try:
        file_hash = get_hash_from_file(filename)

    except UnicodeDecodeError as e:
        print("Bad type:\n{}".format(e))
        counter_failed += 1
        output_fails.append(raw_filename)
        continue

    # Create an entry for the parent and append it
    parent_dict = {
                'parent_filename':raw_filename,
                'filename':raw_filename,
                'hash':get_hash_from_file(filename)
            }
    output_success.append(parent_dict)

    # Now search for all entries in the complete crawl with the same base url
    search = raw_filename.split('.txt')[0]
    #print(parent_dict)
    #print("\t> {}".format(search))
    for key in input_data_full:

        if key.startswith(search) and key != raw_filename:
            child_dict = {
                        'parent_filename':raw_filename,
                        'filename':key,
                        'hash':input_data_full[key]
                    }
            #print("\t\t> {}".format(key))
            #print(child_dict)
            output_success.append(child_dict)

    #print("\n\n")

    counter_success += 1


df = pd.DataFrame(output_success)
df.to_pickle(OUTPUT_FILE)

print(df.shape)
print("DONE")
