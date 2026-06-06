import spacy
import math
import re
import itertools
import argparse
import json
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import time

from enum import Enum

# punctuation marks that usually ends a sentence, may not be exhaustive list
sentence_enders = '\.|!|\?|;|\:'
# when splitting the sentences, keep the first sentence ender and discard rest
# including any following white spaces
sentence_separator = fr'(?<={sentence_enders})(?:{sentence_enders})*(?: |\\n)*'

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

def parse_filters(filter_path):
    with open(filter_path, "r") as file: 
        filter_dict = json.load(file)
    return filter_dict

def classify(root_pos, left_contains_noun):
    """
    Crude classification function. Classifies phrases by root's part of speech.
    """
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
    # should add prepositional phrases?

    return sentence_type.value

def parse_sentence(doc):
    results = []
    if len(list(doc.sents)) > 1:
        results.append([doc.text, None, None, SentenceType.PARAGRAPH.value, len(doc.text)])
    else:
        for sent in doc.sents:
            root = sent.root
            lefts = list(root.lefts)
            rights = list(root.rights)
            left_contains_noun = any([token.pos_ in ("NOUN", "PROPN", "PRON") for token in lefts])

            results.append([sent.text, str(root), str(root.pos_), classify(root.pos_, left_contains_noun), len(sent.text)])

    return results

def parse_sentence_batch(nlp, batch):
    docs = list(nlp.pipe(batch, batch_size = len(batch)))
    batch_processed = list(itertools.chain(*list(map(parse_sentence, docs))))
    return batch_processed

def get_boundary_words(left_docs, right_docs):
    result = []
    for left_doc, right_doc in zip(left_docs, right_docs):
        # these will be list of sentences
        left_sents = list(left_doc.sents)
        right_sents = list(right_doc.sents)

        # last word of the last sentence from the left
        left_boundary = (str(left_sents[-1][-1]), str(left_sents[-1][-1].pos_)) if len(left_sents) else (None, None)

        # first word of the first sentence from the right
        right_boundary = (str(right_sents[0][0]), str(right_sents[0][0].pos_)) if len(right_sents) else (None, None)

        result.append((left_boundary, right_boundary))

    return result

def parse_peripheral_batch(nlp, batch):
    lefts = []
    rights = []
    for left, right in batch:
        lefts.append(left)
        rights.append(right)

    left_docs = list(nlp.pipe(lefts, batch_size = len(batch)))
    right_docs = list(nlp.pipe(rights, batch_size = len(batch)))

    boundary_words = get_boundary_words(left_docs, right_docs)

    return boundary_words

def parse_file_with_spacy(
    nlp,
    in_path,
    r_search,
    r_split,
    out_path,       #parquet
    chunksize=5000,
    batch_size=1000
):
    parsed = []
    processed = 0

    batch_to_process_peripherals = []
    batch_to_process = []
    text_ids = []
    text_id = 0 # this id is just the row number of the source sentence in utterances_#.jsonl file
    with pd.read_json(in_path, lines=True, chunksize=chunksize) as reader:
        for chunk in reader:
            texts = chunk["text"].values
            matches_gen = (re.findall(r_search, text, flags=re.MULTILINE) for text in texts)

            # first split the text by whatever you are trying to match
            # then, for each chunk, split it into sentences before grabbing the first and last sentences
            # for each chunk; this makes it easy to find boundary sentences
            peripherals_gen = (
                [
                    (
                        sentences[0],
                        # because re.split works like the following:
                        #       >>> re.split(sentence_separator, "hello world!")
                        #       ['hello world!', '']
                        # i have to do a weird check for this particular edge case
                        sentences[-2] if len(sentences) > 1 and not len(sentences[-1]) else sentences[-1]
                    )
                    for split_chunk in re.split(r_split, text)
                    if (sentences:=re.split(sentence_separator, split_chunk.strip()))
                ]
                for text in texts
            )


            for matches, peripherals in zip(matches_gen, peripherals_gen):
                if len(batch_to_process) >= batch_size:
                    #process batch
                    parsed.extend(
                        [
                            [text_ids[i]] + data[0] + list(data[1]) + list(data[2])
                            for i, data in enumerate(
                                zip(parse_sentence_batch(nlp, batch_to_process),
                                    batch_to_process_peripherals,
                                    parse_peripheral_batch(nlp, batch_to_process_peripherals))
                            )
                        ]
                    )
                    processed += len(batch_to_process)
                    #print(f"\033[2J\r{processed} {text_id}")
                    # clear batch
                    batch_to_process = []
                    batch_to_process_peripherals = []
                    text_ids = []

                # add things to batch
                batch_to_process.extend(matches)            # matches may be a list, i.e., text contained multiple matches
                text_ids.extend([text_id]*len(matches))     # assign correct text_id to each matches
                text_id +=1                                 # increment text_id per text NOT per matches

                # i th match is surround by the last sentence of the (i - 1) th chunk and first sentence of i th chunk
                batch_to_process_peripherals.extend(
                        [
                            (peripherals[i - 1][-1], peripherals[i][0])
                            for i in range(1, len(matches)+1)
                        ]
                )

            # final batch
            parsed.extend(
                [
                    [text_ids[i]] + data[0] + list(data[1]) + list(data[2])
                    for i, data in enumerate(
                        zip(parse_sentence_batch(nlp, batch_to_process),
                            batch_to_process_peripherals,
                            parse_peripheral_batch(nlp, batch_to_process_peripherals))
                    )
                ]
            )

    parsed_df = pd.DataFrame(
        parsed,
        columns=[
            "context_text_id",
            "text",
            "root",
            "root_pos",
            "sentence_type",
            "num_words",
            "left_sentence",
            "right_sentence",
            "left_boundary_word",
            "right_boundary_word"
        ]
    )
    parsed_df.attrs["source_data_path"] = in_path

    parsed_df.to_parquet(out_path)

def parse_from_path(
    nlp,
    in_path,
    out_path,
    filter_path,
    chunksize=5000,
    batch_size=1000
):
    """
    Function to filter files by specified filters. Assumes that no new files are getting added
    to in_path as this function is running.

    Parameters:
        nlp: Spacy model
        in_path: path to directory that contains unfiltered data
            Assuming the directory is structured as:
                in_path
                    └─split#
                        └─utterances###.jsonl  -->  this is the file we are trying to filter
        out_path: path to directory that will contain filtered data
            Outpath will be structured in similar fashion as in_path structure:
                out_path
                    └─split#
                        └─utterances###.jsonl  -->  this is the filtered file
        filter_path: path to filter
        chunksize: chunk size when loading data into memory
        batch_size: batch size when parsing sentences
    Return:
        None
    """
    split_dirs = [f'{in_path}/{dir_name}' for dir_name in os.listdir(in_path) if "split" in dir_name]

    if len(split_dirs) == 0:
        print("Could not find split_# directories")
        return

    filters = parse_filters(filter_path)

    for split_dir in split_dirs:
        os.makedirs(f"{out_path}/{split_dir.split('/')[-1]}", exist_ok=True)
        files = os.listdir(split_dir)
        for file in files:
            in_path = f"{split_dir}/{file}"
            out_path = f"{out_path}/{split_dir.split('/')[-1]}/{file.split('.')[0]}.parquet"
            print("Parsing ", in_path)
            start = time.perf_counter()
            parse_file_with_spacy(
                nlp,
                in_path,
                filters['search'],
                filters['split'],
                out_path,
                chunksize=chunksize,
                batch_size=batch_size
            )
            end = time.perf_counter()
            print(f"Saved parsed file at {out_path}. Elapsed {(end - start) / 60} minutes")

def main():
    parser = argparse.ArgumentParser(
        prog="process_spacy",
        description="Parse the filtered convokit data using spacy"
    )

    parser.add_argument('-i', '--in_path', type=str, required=True)
    parser.add_argument('-o', '--out_path', type=str, required=True)
    parser.add_argument('-f', '--filter_path', type=str, required=True, help="path to json file containing list of filters")
    parser.add_argument('-g', '--use_gpu', action="store_true", help="indicate if input file is a jsonl file")
    parser.add_argument('-c', '--chunksize', type=int, default=5000, help="chunk size when loading data into memory")
    parser.add_argument('-b', '--batch_size', type=int, default=1000, help="batch size when parsing sentences")
    args = parser.parse_args()

    if args.use_gpu:
        try:
            # Attempt to require the GPU
            spacy.require_gpu()
            print("GPU successfully activated.")
        except ValueError as e:
            # Handle the error if no GPU is found
            print(f"Error activating GPU: {e}")
            print("Falling back to CPU.")

    try:
        nlp = spacy.load("en_core_web_trf")
    except Exception as e:
        print(e)
        print("run python -m spacy download en_core_web_trf to download the model")
        exit(1)

    in_path = args.in_path
    out_path = args.out_path

    if not os.path.exists(in_path):
        print("Invalid in path")
        exit(1)

    os.makedirs(out_path, exist_ok=True)

    if not os.path.exists(args.filter_path):
        print("Invalid filter path")
        exit(1)

    test_file = "/Accounts/yuc3/.convokit/saved-corpora/subreddit-AskReddit/filtered_paren/split_8/utterance_3552.jsonl"
    r_search = r'(?<=\()[a-zA-Z0-9 ,;."\'!?#@$%^&*-_+=]+(?=\))'
    r_split = r'\([a-zA-Z0-9 ,;."\'!?#@$%^&*-_+=]+\)'

    parse_from_path(nlp, in_path, out_path, args.filter_path, chunksize=args.chunksize, batch_size=args.batch_size)

if __name__ == "__main__":
    main()
