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

    # just read json and making into a list of tuples

    pass

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

    pass

def split_data_from_path(
        in_path: str, 
        out_path: str, 
        num_sentences: int, 
        num_files: int, 
        filters: list[tuple[str,bool]] = None
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

    Return:
        None
    """

    # in_path should be a file not dir

    # create out_path if it does not exist 

    # do the ijson mess; might want check out if there's more stable way to do parsing

    pass

def filter_data_from_path(
        in_path: str, 
        out_path: str, 
        filters: list[tuple[str,bool]],
        num_cores: int = 1
    ) -> None:
    """ 
    Function to filter files by specified filters 

    Parameters: 
        in_path: path to directory that contains unfiltered data
        out_path: path to directory that will contain filtered data 
        filters: list of tuples of (rstr, bool), where bool signals reverse regex match for rstr
        num_cores: number of cores for concurrent processing of data 

    Return:
        None
    """

    # I should have a worker function and a process pool probably
    # I should assign different set of folders to each process for it to process

    pass

def main(): 
    parser = argparse.ArgumentParser(
        prog="process_convokit_data",
        description="A tool to process giant convokit jsonl files; tailored specifically for splitting/parsing convokit data" 
    )

    parser.add_argument('-m', '--mode', type=str, choices=['split', 'filter'], help="specify which function to run", required=True)
    parser.add_argument('-i', '--in_path', type=str, required=True)
    parser.add_argument('-o', '--out_path', type=str, required=True)
    parser.add_argument('-f', '--filter', type=str, help="path to json file containing list of filters")
    parser.add_argument('-n', '--ncpu', type=int, help="choose number of cpus to use when in filter mode")
    args = parser.parse_args()

if __name__ == "__main__":
    main()
