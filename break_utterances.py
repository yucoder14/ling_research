import json
import ijson 
import os 


"""
    For breaking up large utterances.json file into chunks with specified 
    number of sentence per chunk. This is specifically built for breaking 
    up subreddit data when utterances.json is too big for convokit library 
    to handle. I guess the down side is that we can't use convokit's methods, 
    but what's the use if you can't even load the data into memory without 
    waiting an eternity and possibly risking of spawning OOMd

    I can then iterate through the files and load it into pandas dataframe 
    to then do nlp or whatever needs to be done on the data
"""

root = "/Accounts/yuc3/.convokit/saved-corpora/subreddit-AskReddit"
uge_json_path = f"{root}/utterances.json"
split_utterances_dir = f"{root}/splits"

directory_coutner = 0
utterance_counter = 0
num_file_per_dir = 500
sentences_per_chunk = 100000

chunk = []

with open(uge_json_path, 'rb') as f: 
    parser = ijson.parse(f)
    partial_json = {} 
    partial_meta = {}
   
    for prefix, event, value in parser: 
        if "meta" in prefix:
            if event == "start_map": 
                partial_meta = {} 
            elif event == "end_map": 
                partial_json["meta"] = partial_meta
            elif event == "map_key":
                pass
            else:
                partial_meta[prefix.split(".")[-1]] = value
        else:
            if event == "start_map": 
                partial_json = {}
            elif event == "end_map":
                if partial_json["text"] != "[removed]" and partial_json["text"] != "[deleted]" and len(partial_json["text"]) > 0:
                    chunk.append(partial_json)
                if (len(chunk) == sentences_per_chunk): 
                    if utterance_counter % num_file_per_dir == 0:  
                        directory_coutner += 1
                        os.makedirs(f'{split_utterances_dir}_{directory_coutner}', exist_ok=True)

                    utterance_counter += 1 

                    out_path = f'{split_utterances_dir}_{directory_coutner}/utterances{utterance_counter}.jsonl.fixed'
                    print(out_path, len(chunk))
                    with open(out_path, "w") as out_file:
                        for utterance in chunk:
                            out_file.write(f'{json.dumps(utterance)}\n') 

                    old_path = f'{split_utterances_dir}_{directory_coutner}/utterances{utterance_counter}.jsonl'
                    if os.path.exists(old_path):
                        os.remove(old_path) 
                    chunk = []
            elif event == "map_key":
                pass
            else:
                partial_json[prefix.split(".")[-1]] = value

out_path = f'{split_utterances_dir}_{directory_coutner}/utterances{utterance_counter}.jsonl'
print(out_path, len(chunk))
with open(out_path, "w") as out_file:
    for utterance in chunk:
        out_file.write(f'{str(utterance)}\n') 
