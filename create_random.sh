#!/bin/bash           
NUMBER="50"
for NUMBER in 50 100 200 300 400 500 600 700 800
do

shuf -n $NUMBER american_musical_theater_actors_ids_in_db.txt > ./datasets/american_musical_theater_actors/amt_$NUMBER.txt
shuf -n $NUMBER desserts_ids_in_db.txt > ./datasets/desserts/d_$NUMBER.txt
shuf -n $NUMBER graph_theory_ids_in_db.txt > ./datasets/graph_theory/gt_$NUMBER.txt

done

