import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import chain, cycle

import pandas as pd
import urllib3

logging.basicConfig(filename="app.log", encoding="utf-8")

with open("api_key.txt", "r") as f:
    KEY = f.read().strip()

BASE_HTML = "https://api.nasa.gov/neo/rest/v1/feed"


# handle not 200
def download(params, cmanager, base=BASE_HTML):
    values = {"start_date": params[0], "end_date": params[1], "api_key": params[2]}
    r = cmanager.request("GET", base, fields=values)
    reply = r.data.decode("utf-8")
    return json.loads(reply)


def dict2row(d):
    vals = {}
    # as time
    vals["close_approach_date"] = d["close_approach_data"][0][
        "close_approach_date_full"
    ]

    vals["object_name"] = d["name"]
    # float
    vals["object_size_max"] = float(
        d["estimated_diameter"]["kilometers"]["estimated_diameter_max"]
    )
    vals["object_size_min"] = float(
        d["estimated_diameter"]["kilometers"]["estimated_diameter_min"]
    )
    vals["absolute_magnitude_h"] = float(d["absolute_magnitude_h"])
    vals["miss_distance_lunar"] = float(
        d["close_approach_data"][0]["miss_distance"]["lunar"]
    )

    return vals


def postprocess_feed(feed):
    feed_list = []
    for day in feed["near_earth_objects"].keys():
        day_objects = feed["near_earth_objects"][day]
        for o in day_objects:
            feed_list.append(dict2row(o))

    return feed_list


def generate(start, end, npools=50, nthreads=50, sort_key="miss_distance_lunar"):

    date_range = (
        pd.date_range(start, end, freq="7D", normalize=True, inclusive="left")
        .strftime("%Y-%m-%d")
        .to_list()
    )
    date_range.append(end)
    params = list(zip(date_range, date_range[1:], cycle([KEY])))
    results = []

    cmanager = urllib3.PoolManager(num_pools=npools, maxsize=50)

    with ThreadPoolExecutor(nthreads) as exec:
        futurs = {exec.submit(download, p, cmanager): p[0] for p in params}
        for f in as_completed(futurs):
            start_d = futurs[f]
            try:
                data = f.result()
                results.append(data)
            except Exception as exc:
                logging.debug(f"{start_d} caused repr(exc)")
                continue
            else:
                logging.debug(f"{start_d} length: {len(data)}")

    feed_lists = list(map(postprocess_feed, results))
    object_list = list(chain.from_iterable(feed_lists))

    df = pd.DataFrame.from_records(object_list)

    df["close_approach_date"] = pd.to_datetime(df["close_approach_date"])
    df["miss_distance_lunar"] = df["miss_distance_lunar"].astype("float64")
    df["object_name"] = df["object_name"].astype("string")
    df = df.sort_values(sort_key)
    return df.to_html()


def main():
    return


if __name__ == "__main__":
    main()
