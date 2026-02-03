import pandas as pd 
from enum import Enum

class SentenceType(Enum):
    SENTENCE = 0 
    NOUN_PHRASE = 1 
    VERB_PHRASE = 2 
    ADV_PHRASE = 3
    ADJ_PHRASE = 4
    NUMBER = 5 
    INTERJECTION = 6
    PARAGRAPH = 7
    OTHER = 8 

parsed_file = "testing.parquet"

df = pd.read_parquet(parsed_file)
test_file = df.attrs['source_data_path']

source_data = pd.read_json(test_file, lines=True)
texts = source_data["text"].values
subreddits = source_data["meta"].apply(lambda meta: meta["subreddit"])
permalinks = source_data["meta"].apply(lambda meta: meta["permalink"])

df["category"] = df["sentence_type"].map(SentenceType)
combined_df = df.groupby("context_text_id").agg(list)
print(df["context_text_id"].nunique())

#assert len(combined_df) == len(texts)
while True: 
    command = input(">> ").strip().split(" ")
    if command[0] == "exit":
        break
    elif command[0] == "list": 
        n = int(command[1])
        print(df[:n])
    elif command[0] == "query": 
        n = int(command[1])
        sentence_type = int(command[2])
        rows = df[df["sentence_type"] == sentence_type].sample(n=n) 
        print(rows)
    elif command[0] == "value_counts":   
        print(df["category"].value_counts())
    elif command[0] == "view": 
        row_id = int(command[1]) 
        row = combined_df.iloc[row_id]
#        row = rows[row_id]
        text = texts[row_id]
        subreddit = subreddits[row_id]
        permalink = permalinks[row_id]
        print("Meta")
        print("="*70)
        print(subreddit)
        print(permalink)
        print("Context")
        print("="*70)
        print(text)
        print()
        print("Searched Strings")
        print("="*70)
        for text, category in list(zip(row["text"], row["category"])):
            print("Text:", text, "Type:", category.name)

        


