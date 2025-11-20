import argparse
import ijson
import json
import os 
import pandas as pd
import multiprocessing

def create_filter_list_from_json(json_path:str) -> list[tuple[str,bool]]: 
    """
    Function to create filter list from json file
    
    Parameters: 
        json_path: path to the json file containing filter
            should be in the following format:
            [{ "regex": "some_regex_str", "inverse": True/False },...] 

    Return: 
        list of tuples with (rstr, bool) 
    """
    print(json_path)

    with open(json_path, "r") as file: 
        filter_dict = json.load(file)
    return [(obj["regex"], obj["inverse"]) for obj in filter_dict]

def filter_data(df:pd.DataFrame, filters: list[tuple[str,bool]]) -> pd.DataFrame:
    """ 
    Function to filter elements of df with regex

    Parameters: 
        df: pandas dataframe containing unfiltered data 
        filters: list of tuples (rstr, bool), where bool signals reverse regex match for rstr 

    Return: 
        filtered df
    """

    # iterate through the filters and filter the dataframe 
    filtered_df = df 
    for rstr, inverse in filters: 
        if not inverse: 
            filtered_df = filtered_df[filtered_df['text'].str.contains(rstr)]
        else: 
            filtered_df = filtered_df[~filtered_df['text'].str.contains(rstr)]

    return filtered_df 

def split_data_from_path(
        in_path: str, 
        out_path: str, 
        num_sentences: int, 
        num_files: int, 
        filters: list[tuple[str,bool]] = None
        lines: bool = False
    ) -> None: 
    """
    Function to split giant json file into manageable chunks. Because it reads 
    the giant jsonl file byte by byte, I don't know how you would make this 
    run concurrently. 

    Parameters: 
        in_path: path to the giant jsonl file to parse and split 
        out_path: path to directory that will contain split files
        num_sentences: number of json lines per file 
        num_files: number of files per sub directory of out_path 
        filter: if not None, filter the data for chunked data
        lines: indicate if jsonl file

    Return:
        None
    """

    # in_path should be a file not dir
    if not os.path.exists(in_path) or not os.path.isfile(in_path): 
        print(in_path, "is invalid")
        return

    # create out_path if it does not exist 
    os.makedirs(out_path, exist_ok=True)

    with open(in_path, "r") as infile: 
        search_tag = '' if lines else 'item'

        items = ijson.items(infile, search_tag, multiple_variable=True) 

        sentence_count = 0
        file_count = 0
        dir_count = 0
        lines = []

        for item in items: 
            # only add lines that have relevant text
            if item["text"] != "[removed]" and item["text"] != "[deleted]" and len(item["text"]) > 0:
                sentence_count += 1
                lines.append(json.dumps(item))

            if (len(lines) == num_sentences): 
                # create split directory if needed
                if file_count % num_files == 0:  
                    dir_count += 1
                    os.makedirs(f'{out_path}/split_{dir_count}', exist_ok=True)

                # write to file
                file_count += 1
                tmp_path = f"{out_path}/split_{dir_count}/utterance_{file_count}.jsonl.tmp"
                file_path = f"{out_path}/split_{dir_count}/utterance_{file_count}.jsonl"
                with open(tmp_path, "w") as outfile:
                    for line in lines:
                        outfile.write(f"{line}\n")

                # atomically change file
                os.rename(tmp_path, file_path)

                # erase lines
                lines = []

def filter_worker(
        out_path: str, 
        split_dirs:list[str], 
        filters: list[tuple[str, bool]]
    ) -> None:
    """ 
    Helper function 
    """
    for split_dir in split_dirs: 
        os.makedirs(f"{out_path}/{split_dir.split('/')[-1]}", exist_ok=True)
        files = os.listdir(split_dir)
        for file in files: 
            df = pd.read_json(f'{split_dir}/{file}', lines=True)
            filtered_df = filter_data(df, filters)
            filtered_df.to_json(f"{out_path}/{split_dir.split('/')[-1]}/{file}", orient='records', lines=True)

def filter_data_from_path(
        in_path: str, 
        out_path: str, 
        filters: list[tuple[str,bool]],
        num_cores: int = 1
    ) -> None:
    """ 
    Function to filter files by specified filters. Assumes that no new files are getting added 
    to in_path as this function is running. 

    Parameters: 
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
        filters: list of tuples of (rstr, bool), where bool signals reverse regex match for rstr
        num_cores: number of cores for concurrent processing of data 
    Return:
        None
    """
    split_dirs = [f'{in_path}/{dir_name}' for dir_name in os.listdir(in_path) if "split" in dir_name] 

    if len(split_dirs) == 0: 
        print("Could not find split_# directories")
        return

    if num_cores > 1: 
        k = len(split_dirs) // num_cores
        args = [(out_path, split_dirs[i * k: (i + 1) * k], filters) for i in range(num_cores)] 
        with multiprocessing.Pool(processes=num_cores) as pool: 
            pool.starmap(filter_worker, args)
    else: 
        filter_worker(out_path, split_dirs, filters)

def main(): 
    parser = argparse.ArgumentParser(
        prog="process_convokit_data",
        description="A tool to process giant convokit jsonl files; tailored specifically for splitting/parsing convokit data" 
    )

    parser.add_argument('-m', '--mode', type=str, choices=['split', 'filter'], help="specify which function to run", required=True)
    parser.add_argument('-i', '--in_path', type=str, required=True)
    parser.add_argument('-o', '--out_path', type=str, required=True)
    parser.add_argument('-f', '--filters', type=str, help="path to json file containing list of filters")
    parser.add_argument('-n', '--ncpu', type=int, default=1, help="choose number of cpus to use when in filter mode")
    parser.add_argument('-l', '--lines', action="store_true", help="choose number of cpus to use when in filter mode")
    args = parser.parse_args()

    num_sentences = 100000
    num_files = 500

    in_path = args.in_path 
    out_path = args.out_path 

    if not os.path.exists(in_path):
        print("Invalid in path")
        exit(1)

    os.makedirs(out_path, exist_ok=True)

    filters = None
    if args.filters is not None: 
        if not os.path.exists(args.filters):
            print("Invalid filter path")
            exit(1)
        try: 
            filters = create_filter_list_from_json(args.filters)
        except Exception as e: 
            print("Failed to parse filters\n\t", e)
            exit(1)

    if args.mode == "split": 
        split_data_from_path(in_path, out_path, num_sentences, num_files, filters, args.lines)
    else:
        filter_data_from_path(in_path, out_path, filters, args.ncpu)

if __name__ == "__main__":
    main()
