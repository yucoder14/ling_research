import spacy
import pandas as pd
import math
import re
import itertools

from enum import Enum

data = [
    "Apples, pears, and bananas.",
    "Python, Java, C++, and Rust.",
    "We need screws, bolts, and nails.",
    
    "Click the button.",
    "Submit the form.",
    "Call me tomorrow.",
    
    "The file was deleted by the admin.",
    "The decision was made yesterday.",
    "The cake was eaten.",
    
    "The dog chased the cat.",
    "The user clicked the link.",
    "She played the piano.",

    "2012/5/6",
    "helped", 
    "cat",
    "was blown away",

    "I thought that he was the problem."
]

class SentenceType(Enum):
    SENTENCE = 0 
    NOUN_PHRASE = 1 
    VERB_PHRASE = 2 
    ADV_PHRASE = 3
    ADJ_PHRASE = 4
    NUMBER = 5 
    INTERJECTION = 6
    OTHER = 7 

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

    df = pd.read_json(test_file, lines=True) 

    texts = df["text"].values
    texts = list(map(lambda text: re.search(search, text, flags=re.MULTILINE).group(0), texts))
    batch_size = 200
    num_batches = math.ceil(len(texts) / batch_size)

    parsed = []

    processed = 0
    for i in range(num_batches):
        batch = texts[i * batch_size: (i + 1) * batch_size]
        parsed.extend(parse_sentence_batch(nlp, batch))
        processed += len(batch)
        print(f"\033[2J\r{processed}")

    parsed_df = pd.DataFrame(parsed, columns=["text", "root", "root_pos", "sentence_type"])
    parsed_df.to_csv("testing.csv", index=False)
