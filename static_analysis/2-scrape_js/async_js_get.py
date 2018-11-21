#!/usr/bin/env python3
#
# Original code: cgarcoae, Sep 21 '18
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

import aiohttp
from aiohttp import ClientSession, TCPConnector
import asyncio
import os, os.path
import pandas as pd
import ssl
import sys

from pathlib import Path
from pypeln import asyncio_task as aio

##### Specify Directories ######################################################
# Max number of workers
limit = 20

ssl.match_hostname = lambda cert, hostname: True

# Parent directories
storage_dir = "/mnt/Data/UCOSP_DATA"
pwd = os.path.dirname(os.path.realpath(__file__))

# Input directory
#url_list = os.path.join(storage_dir, "resources/sample_full_url_list_test/")
url_list = os.path.join(storage_dir, "resources/full_url_list_v2/")

# Output directory
output_dir = os.path.join(storage_dir, "js_source_files/")


##### Load in dataset ##########################################################
parquet_dir = Path(url_list)

input_data = pd.concat(
    pd.read_parquet(parquet_file)
        for parquet_file in parquet_dir.glob('*.parquet')
)

# Sanity check
print("Expecting {} url connections".format(input_data.shape[0]))

input_data['filename'] = output_dir + input_data['filename']
input_data = input_data.values


##### Async fetch ##############################################################
async def fetch(data, session):

    url = data[0]
    filename = data[1]

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

    except aiohttp.ClientError as e:
        print(e)
        return e

    except asyncio.TimeoutError as e:
        print(e)
        return e

    except ssl.CertificateError as e:
        print(e)
        return e

    except ValueError as e:
        print(e)
        return e


##### Iterate over each list entry #############################################
aio.each(
    fetch,
    input_data,
    workers = limit,
    on_start = lambda: ClientSession(connector=TCPConnector(limit=None)),
    on_done = lambda _status, session: session.close(),
    run = True,
)

print("\n\nDONE")
