#! /bin/bash 

PYTHON=/Accounts/yuc3/korpora/bin/python3
SCRIPT=/Accounts/yuc3/ling_research/process_convokit_data.py

convokit_dir=/Accounts/yuc3/.convokit/saved-corpora
subreddits=(atheism gaming movies relationships todayilearned)
filter_path=/Accounts/yuc3/ling_research/filter_paren.json

for subreddit in ${subreddits[@]}; do 
	subreddit_path=$convokit_dir/subreddit-$subreddit
	echo $subreddit_path
	readarray -t utterances <<< $(ls $subreddit_path | grep utterance)
	if [ ${#utterances[@]} -eq 2 ]; then 
		$PYTHON $SCRIPT -m split -i $subreddit_path/utterances.json -o $subreddit_path/splits  	
	elif [ ${#utterances[@]} -eq 1 ]; then
		$PYTHON $SCRIPT -m split -i $subreddit_path/utterances.jsonl -o $subreddit_path/splits -l	
	else
		echo "could not fine utterances.json or utterances.jsonl file"
	fi
done

