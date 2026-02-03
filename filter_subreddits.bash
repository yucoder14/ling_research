#! /bin/bash 

CHECK_NODELIST=/Accounts/yuc3/ling_research/check_node.bash
PYTHON=/Accounts/yuc3/korpora/bin/python3
SCRIPT=/Accounts/yuc3/ling_research/process_convokit_data.py

convokit_dir=/Accounts/yuc3/.convokit/saved-corpora
subreddits=(atheism gaming movies relationships todayilearned AskReddit politics)
filter_path=/Accounts/yuc3/ling_research/filter_paren.json

PARTITION=SevenDay

for subreddit in ${subreddits[@]}; do 
	subreddit_path=$convokit_dir/subreddit-$subreddit
	num_cores=$(ls $subreddit_path/splits | wc -l)		
	readarray -d " " -t available_nodes <<< $($CHECK_NODELIST $PARTITION)
	len=${#available_nodes[@]} 
	node=${available_nodes[$(( $len - 1 ))]}
	echo srun -c$num_cores -p $PARTITION --nodelist=$node $PYTHON $SCRIPT -m filter -i $subreddit_path/splits -o $subreddit_path/filtered_paren -f $filter_path -n $num_cores &
done

