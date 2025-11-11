import requests
import re 
import json
import pandas

def write_jsonl(file_path, data):
    with open(file_path, 'w', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item) + '\n')

def get_info():
    root = "https://zissou.infosci.cornell.edu/convokit/datasets/subreddit-corpus/corpus-zipped/"
    response = requests.get(root)

    raw_list = response.text.split('\r\n')

    href_regex = r'(?<=href=")[a-zA-Z0-9\-~_.]+'
    file_size_regex = r'\d+$' 

    reddit_lexi_ranges = []
    for row in raw_list:
        m = re.search(href_regex, row)
        if m is not None:
            reddit_lexi_ranges.append(m.group(0))

    subreddit_size_dict = []
    for lexi_range in reddit_lexi_ranges: 
        path = f'{root}{lexi_range}'
        response = requests.get(path)
        raw_list = response.text.split('\r\n') 
        for row in raw_list:  
            m = re.search(href_regex, row)
            n = re.search(file_size_regex, row.strip())
            if m is not None and n is not None:
                json_dict =  {"corpus": m.group(0), "comp_size": int(n.group(0))}
                print(json_dict)
                subreddit_size_dict.append(json_dict)

    write_jsonl("summary.jsonl", subreddit_size_dict)


# get_info()
