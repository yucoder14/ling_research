import json
import ijson 
import os 

root = "/Accounts/yuc3/.convokit/saved-corpora/subreddit-AskReddit"
uge_json_path = f"{root}/utterances.json"
split_utterances_dir = f"{root}/splits"

counter = 0
sentences_per_chunk = 100000

chunk = []

os.makedirs(split_utterances_dir, exist_ok=True)

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
                chunk.append(partial_json)
                if (len(chunk) == sentences_per_chunk): 
                    counter += 1 
                    out_path = f'{split_utterances_dir}/utterances{counter}.jsonl'
                    print(out_path, len(chunk))
                    with open(out_path, "w") as out_file:
                        for utterance in chunk:
                            out_file.write(f'{str(utterance)}\n') 
                    chunk = []
            elif event == "map_key":
                pass
            else:
                partial_json[prefix.split(".")[-1]] = value

#print(chunk[-1])
