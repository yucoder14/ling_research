import spacy
import pandas as pd
import math
import re
import itertools

import random

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

def classify(root_pos, left_contains_noun): 
    sentence_type = SentenceType.OTHER

    if root_pos in ("VERB", "AUX"): 
        if left_contains_noun:
            sentence_type = SentenceType.SENTENCE
        else:
            sentence_type = SentenceType.VERB_PHRASE 
    elif root_pos in ("NOUN", "PROPN", "PRON"):
        sentence_type = SentenceType.NOUN_PHRASE
    elif root_pos == "NUM":
        sentence_type = SentenceType.NUMBER
    elif root_pos == "INTJ":
        sentence_type = SentenceType.INTERJECTION
    elif root_pos == "ADV":
        sentence_type = SentenceType.ADV_PHRASE
    elif root_pos == "ADJ":
        sentence_type = SentenceType.ADJ_PHRASE

    return sentence_type.value

def parse_sentence(doc):
    results = []
    if len(list(doc.sents)) > 1: 
        results.append([doc, None, None, SentenceType.PARAGRAPH.value])
    else: 
        for sent in doc.sents:
            root = sent.root
            lefts = list(root.lefts)
            rights = list(root.rights)
            left_contains_noun = any([token.pos_ in ("NOUN", "PROPN", "PRON") for token in lefts])

            results.append([sent, root, root.pos_, classify(root.pos_, left_contains_noun)])

    return results

def parse_sentence_batch(nlp, batch):
    docs = list(nlp.pipe(batch, batch_size = len(batch)))
    batch_processed = list(itertools.chain(*list(map(parse_sentence, docs))))
    return batch_processed

def process_file_with_spacy(
    nlp, 
    in_path, 
    re_string, 
    out_path,       #csv
    chunksize=1000,
    batch_size=200
):
    parsed = []
    processed = 0

    batch_to_process = []
    text_ids = []
    text_id = 0
    with pd.read_json(in_path, lines=True, chunksize=chunksize) as reader:
        for chunk in reader:
            texts = chunk["text"].values
            texts_iter = (re.findall(search, text, flags=re.MULTILINE) for text in texts)

            for matches in texts_iter:
                if len(batch_to_process) >= batch_size: 
                    #process batch 
                    parsed.extend([[text_ids[i]] + data for i, data in enumerate(parse_sentence_batch(nlp, batch_to_process))])
                    processed += len(batch_to_process)
                    print(f"\033[2J\r{processed}")
                    # clear batch and add to batch
                    batch_to_process = []
                    text_ids = []

                batch_to_process.extend(matches)
                text_ids.extend([text_id]*len(matches))
                text_id +=1

            parsed.extend([[text_ids[i]] + data for i, data in enumerate(parse_sentence_batch(nlp, batch_to_process))])

    parsed_df = pd.DataFrame(parsed, columns=["context_text_id", "text", "root", "root_pos", "sentence_type"])
    parsed_df.to_csv(out_path, index=False)

if __name__ == "__main__":
    try:
        # Attempt to require the GPU
        spacy.require_gpu()
        print("GPU successfully activated.")
    except ValueError as e:
        # Handle the error if no GPU is found
        print(f"Error activating GPU: {e}")
        print("Falling back to CPU.")

    nlp = spacy.load("en_core_web_trf")

    test_file = "/Accounts/yuc3/.convokit/saved-corpora/subreddit-AskReddit/filtered_paren/split_1/utterance_1.jsonl"
    search = r'(?<=\()[a-zA-Z0-9 ,;."\'!?#@$%^&*-_+=]+(?=\))'

    process_file_with_spacy(nlp, test_file, search, "testing.csv", chunksize=1000, batch_size=300)

