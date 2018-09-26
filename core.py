import urllib.request
import feedparser
import time
import json
import os
from multiprocessing import Pool
from tqdm import *
import re

results_per_worker = 1000
results_per_iteration = 1000
max_results = 100000
destination_directory = "datasets"
file_name = "dataset{}.json"
search_query = 'cat:cs.CV+OR+cat:cs.AI+OR+cat:cs.LG+OR+cat:cs.CL+OR+cat:cs.NE+OR+cat:stat.ML+OR+cat:cs.SE'

def worker(start):
    max = start + results_per_worker
    if (start == 0):
        name_count = "0"
    else:
        name_count = "{}".format(str(start))

    base_url = 'http://export.arxiv.org/api/query?'
    result_list = []

    replay_countdown = 3
    try:
        while start < max or replay:
            replay = False
            query = 'search_query=%s&start=%i&max_results=%i' % (search_query, start, results_per_iteration)
            feedparser._FeedParserMixin.namespaces['http://a9.com/-/spec/opensearch/1.1/'] = 'opensearch'
            feedparser._FeedParserMixin.namespaces['http://arxiv.org/schemas/atom'] = 'arxiv'
            response = urllib.request.urlopen(base_url + query).read()
            feed = feedparser.parse(response)
            for entry in feed.entries:
                data = str(entry.published)
                d_tag = []
                for element in entry.tags:
                    t_tag = re.split(';|,', element.term)
                    for i in t_tag:
                        d_tag.append(i.strip())

                temp_dic = {
                    "id": str(entry.id.split('/abs/')[-1]),
                    "date": str(data),
                    "title": str(entry.title).replace("\n ", ""),
                    "authors": [e.name for e in entry.authors],
                    "link": [item for item in entry.links if item.rel != 'alternate'],
                    "tag": d_tag,
                    "abstract": str(entry.summary).replace("\n", " ")
                }
                result_list.append(temp_dic)

            if len(result_list):
                f = open(os.path.join(destination_directory, file_name.format(name_count)), "a")
                json.dump(result_list, f, sort_keys=False, indent=4, ensure_ascii=False)
                result_list.clear()
                f.close()
                start = start + results_per_iteration
            else:
                if replay_countdown > 0:
                    time.sleep(3)
                    replay = True
                    replay_countdown = replay_countdown - 1
                else:
                    start = start + results_per_iteration

    except:
        if len(result_list):
            f = open(os.path.join(destination_directory, file_name.format(name_count)), "a")
            json.dump(result_list, f, sort_keys=False, indent=4, ensure_ascii=False)
            f.close()
    return None

def generate_dataset():
    os.makedirs(destination_directory, exist_ok=True)
    data = [i for i in range(0,max_results,results_per_worker)]

    with Pool() as p:
        max = max_results / results_per_worker
        with tqdm(total=max) as pbar:
            for i,value in tqdm(enumerate(p.imap(worker,data))):
                pbar.update()

generate_dataset()
