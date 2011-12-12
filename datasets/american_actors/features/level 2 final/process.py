f = open('epic_combined.csv')
actor = open('golden_actor_800.csv', 'w')
nonactor = open('golden_nonactor_800.csv', 'w')
count = 0
for line in f.readlines():
    count += 1
    columns = line.split(',')
    print len(columns)
    if len(columns) == 11:
        if columns[1] != "Actor":
            nonactor.write(line)
        else:
            print count
            actor.write(line)
        
            
