import re
import os
import pandas as pd 
import time

regex_match = "\("
regex_reverse_match = "\]\(http"

root_path = '/Accounts/yuc3/.convokit/saved-corpora/subreddit-AskReddit'

in_dir_path = "" 
out_dir_path = f'{root_path}/filtered_paren'

subdirs = sorted([path for path in os.listdir(root_path) if os.path.isdir(f'{root_path}/{path}')])

for subdir in subdirs: 
    print(f"Filtering files in {root_path}/{subdir}...")
    files = os.listdir(f'{root_path}/{subdir}')
    out_path = f"{out_dir_path}/{subdir}"

    os.makedirs(out_path, exist_ok=True) 
    subdir_num = int(subdir.split("_")[-1])
    
    for i in range(1, min(len(files),500) + 1): 
        file = f"utterances{(subdir_num - 1) * 500 + i}.jsonl.fixed"
        file_path = f'{root_path}/{subdir}/{file}'

        try:
            df = pd.read_json(file_path, lines=True)
            texts_with_paren = df[df['text'].str.contains(regex_match)]
            texts_without_href = texts_with_paren[~texts_with_paren['text'].str.contains(regex_reverse_match)]
            texts_without_href.to_json(f'{out_path}/{file}', orient='records', lines=True)
        except Exception as e:
            exit(1) 

