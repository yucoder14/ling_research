Overview
========

In this project, we are analyzing Convokit's [Reddit Corpus](https://convokit.cornell.edu/documentation/subreddit.html) 
to conduct an observational study in the use of punctuations in written contexts. 

This repository contains helpful scripts to parse convokit's Reddit corpus, as well as parsing sentences programmatically 
using spacy's natural language processing model. 

Dependencies
============

See [`requirements.txt`](requirements.txt)

For installing `spacy`, I used `conda install -c conda-forge spacy` 

Splitting/Filtering Corpus
==========================

See [`process_convokit_data.py`](process_convokit_data.py)

Split corpus 
------------

At the time of installation, utterances were organized in a giant `json` or `jsonl` file, making it infeasible to work 
with limited memory. Because the script parses the file as a file stream, split mode does not support multiprocessing. 

Example usage:

```
PYTHON=/path/to/python3
SCRIPT=/path/to/process_convokit_data.py

$PYTHON $SCRIPT -m split -i /path/to/subreddit/utterances.json -o /path/to/output/splits  	
```	

Filter corpus
-------------

The script assumes that you have already split the original corpus using the `split` mode. To filter the utterances, 
you need to provide a `json` file with a list of python regex strings. For example:

```
[ 
	{ "regex": "\\(http.+\\)" , "inverse": true },
	{ "regex": "\\([a-zA-Z0-9 ,;\\.\"'!?#@$%^&*-_+=]+\\)" , "inverse": false }
]
```

The script iterates through the filters and applies each of them, sequentially. 
If `"inverse"` value is set to `true`, the script will do an inverse regex search to drop any utterances 
that matches the regex string. In the example above, you first drop any utterances with links inside a pair of 
parentheses. Then, you search for utterances that contain parenthetical. Be sure the properly escape special 
characters for best results.

Filter mode does support multiprocessing to concurrently filter utterances that have been split across multiple files.

Example usage:

```
PYTHON=/path/to/python3
SCRIPT=/path/to/process_convokit_data.py
filter_path=/path/to/filter_paren.json
num_cores=4

$PYTHON $SCRIPT -m filter -i /path/to/input/splits -o /path/to/output/filtered_paren -f $filter_path -n $num_cores
```	

Full usage statement
--------------------

```
usage: process_convokit_data [-h] -m {split,filter} -i IN_PATH -o OUT_PATH [-f FILTERS] [-n NCPU] [-l]
process_convokit_data: error: the following arguments are required: -m/--mode, -i/--in_path, -o/--out_path
(korpora) (base) [yuc3@goblin2 ling_research]$ python3 process_convokit_data.py -h
usage: process_convokit_data [-h] -m {split,filter} -i IN_PATH -o OUT_PATH [-f FILTERS] [-n NCPU] [-l]

A tool to process giant convokit jsonl files; tailored specifically for splitting/parsing convokit data

optional arguments:
  -h, --help            show this help message and exit
  -m {split,filter}, --mode {split,filter}
                        specify which function to run
  -i IN_PATH, --in_path IN_PATH
  -o OUT_PATH, --out_path OUT_PATH
  -f FILTERS, --filters FILTERS
                        path to json file containing list of filters
  -n NCPU, --ncpu NCPU  choose number of cpus to use when in filter mode
  -l, --lines           indicate if input file is a jsonl file
```

Processing Corpus (WIP)
=======================

See [`process_spacy.py`](process_spacy.py).

Once you have split and filtered the utterances, you can now parse the utterances.
Saves the parsed files into a parquet.

Viewer (WIP)
============

See [`viewer.py`](viewer.py).

Primitive command line interface to sift through processed corpus.


