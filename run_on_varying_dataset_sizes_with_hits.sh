#!/bin/bash   
for NUMBER in 800 
do
        
CATEGORY_ID_FILE="./datasets/american_actors/aa_$NUMBER.txt"
RANDOM_ID_FILE="./datasets/american_actors/random_$NUMBER.txt"

IDS="./actor_ids.txt"
ACTOR_ONE_HOP_IDS="./actor_one_hop_ids.txt"

RESULTS_NO_HITS="./datasets/american_actors/features/level2/aa_and_random_features_$NUMBER.csv"
HITS_RESULTS="./datasets/american_actors/features/level2/aa_and_random_HITS_$NUMBER.csv"
RESULTS_WITH_HITS="./datasets/american_actors/features/level2/aa_and_random_features_with_HITS_$NUMBER.csv"
echo "python getFeaturesUsingDB.py $CATEGORY_ID_FILE $RANDOM_ID_FILE $RESULTS_NO_HITS $IDS $ACTOR_ONE_HOP_IDS "
python getFeaturesUsingDB.py $CATEGORY_ID_FILE $RANDOM_ID_FILE $RESULTS_NO_HITS $IDS $ACTOR_ONE_HOP_IDS 
python hits2.py $HITS_RESULTS $NUMBER american_actors_categories_catids_noDuplicates.txt $CATEGORY_ID_FILE $RANDOM_ID_FILE
python combine_csv_features.py $RESULTS_NO_HITS $HITS_RESULTS $RESULTS_WITH_HITS

done
