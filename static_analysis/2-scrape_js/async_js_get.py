#!/usr/bin/env python3
#
# Original code: Cristian Garcia, Sep 21 '18
#       https://medium.com/@cgarciae/making-an-infinite-number-of-requests-with-
#               python-aiohttp-pypeln-3a552b97dc95
#
# Script adapted by David Dobre Nov 20 '18:
#       Added parquet loading, iteration over dataframes, and content saving
#
# NOTE: You my need to increase the ulimit (3000 worked for me):
#
#           $ ulimit -n 3000
#
################################################################################

from aiohttp import ClientError, ClientSession, TCPConnector
import asyncio
import os, os.path
import pandas as pd
import ssl
import sys
import glob

from pathlib import Path
from pypeln import asyncio_task as aio

##### Specify Directories ######################################################
# Parent directory
STORAGE_DIR = "/mnt/Data/UCOSP_DATA"

# Input directory
URL_LIST = os.path.join(STORAGE_DIR, "resources/full_url_list_parsed/")

# Output directory
OUTPUT_DIR = os.path.join(STORAGE_DIR, "js_source_files/")

# Max number of workers
LIMIT = 20 # play with this value, higher values fail for me

# Not sure what this monkey business does
ssl.match_hostname = lambda cert, hostname: True


##### Load in dataset ##########################################################
parquet_dir = Path(URL_LIST)
input_data = pd.concat(
    pd.read_parquet(parquet_file)
    for parquet_file in parquet_dir.glob('*.parquet')
)

# Sanity check
print("There are {} urls found in the dataset.".format(input_data.shape[0]))

# Check for existing files in output directory
existing_files = [os.path.basename(x) for x in glob.glob(OUTPUT_DIR + "*.txt")]
print("There are {} existing files in {}.".format(
                                                len(existing_files),
                                                OUTPUT_DIR)
                                            )

# Remove those from the "to request" list as they're already downloaded
input_data = input_data[~input_data['filename'].isin(existing_files)]

# Append filename to the output folder and get the raw string value
input_data['filename'] = OUTPUT_DIR + input_data['filename']
input_data = input_data.values
print("> {} urls left to request.".format(input_data.shape[0]))


##### Async fetch ##############################################################
async def fetch(data, session):

    url = data[1]
    filename = data[2]

    try:
        async with session.get(url, timeout=2) as response:
            output = await response.read()
            print("{}\t{}".format(response.status, url))

            if (response.status == 200 and output):
                print("\tsuccess")

                with open(filename, "wb") as source_file:
                    source_file.write(output)
                return output

            return response.status

    # Catch exceptions
    except ClientError as e:
        print(e)
        return e

    except asyncio.TimeoutError as e:
        print(e)
        return e

    except ssl.CertificateError as e:
        print(e)
        return e

    except ssl.SSLError as e:
        print(e)
        return e

    except ValueError as e:
        print(e)
        return e

    except TimeoutError as e:
        print(e)
        return e

    except concurrent.futures._base.TimeoutError as e:
        print(e)
        return e


##### Iterate over each list entry #############################################
aio.each(
    fetch,              # worker function
    input_data,         # input arguments
    workers = LIMIT,    # max number of workers
    on_start = lambda: ClientSession(connector=TCPConnector(limit=None)),
    on_done = lambda _status, session: session.close(),
    run = True,
)

print(80*"#")
print("\nDONE")
