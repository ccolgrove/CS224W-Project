import pymongo
c = pymongo.Connection("ec2-50-112-6-22.us-west-2.compute.amazonaws.com", 1000)

f = open("graph_theory_ids.txt")
f2 = open("graph_theory_ids_in_db.txt", 'w')
for line in f:
    z=c.wp.pages.find({'_id':int(line.strip())}).count()
    if z == 1:
        f2.write(line)

