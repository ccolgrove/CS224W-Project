#!/bin/bash   
for NUMBER in 50 100 200 300 400 500 600 700 780
do
head -n 1 golden_nonactor_800.csv > ./varying_size/aa_$NUMBER.csv
shuf -n $NUMBER golden_actor_800.csv >> ./varying_size/aa_$NUMBER.csv
shuf -n $NUMBER golden_nonactor_800.csv >> ./varying_size/aa_$NUMBER.csv
done
