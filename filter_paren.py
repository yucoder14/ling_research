import re
import os
import pandas as pd 
import time

regex_to_look_for = "\("

root_path = '/Accounts/yuc3/.convokit/saved-corpora/subreddit-AskReddit'
first_correct_file_path = f'{root_path}/splits_1/utterances1.jsonl'
first_correct_json_modified_time = os.path.getmtime(first_correct_file_path) 

out_root_path = f'{root_path}/filtered_paren'

subdirs = sorted([path for path in os.listdir(root_path) if os.path.isdir(f'{root_path}/{path}')])

for subdir in subdirs: 
    files = os.listdir(f'{root_path}/{subdir}')
    files.sort(key=lambda name: int(re.search(r'\d+', name).group(0)))
    out_path = f'{out_root_path}/{subdir}'
    os.makedirs(out_path, exist_ok=True) 
    for file in files: 
        file_path = f'{root_path}/{subdir}/{file}'
        # I have to do this nonsense because I wrongly formatted jsonl files
        # and it would take hours before everything finishes, so I'm going to sleep until the file's updated
        while os.path.getmtime(file_path) < first_correct_json_modified_time:
            # sleep for 30 minutes before checking again
            # essentially something akin to spin lock but much dumber
            time.sleep(60 * 30)

        # another while loop because the file may be in the middle of correction... 
        # I should use some atomic change scheme next time
        while True: 
            try: 
                df = pd.read_json(file_path, lines=True)
                texts_with_paren = df[df['text'].str.contains(regex_to_look_for)]
                texts_with_paren.to_json(f'{out_path}/{file}', orient='records', lines=True)
                break
            except Exception as e:
                time.sleep(60 * 30)
         

