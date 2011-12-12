import numpy
import networkx as nx
import pymongo
import csv
import sys

#data files
num_examples = 50
category_name = "actors"
category_list_file = "american_actors_categories_catids_noDuplicates.txt"
category_members = "random_actors.txt"
category_nonmembers = "random_nonActors.txt"

actors_to_remove = "datasets/graph_theory/gt_800.txt"
non_actors_to_remove = "datasets/graph_theory/random_800.txt"
#"datasets/american_actors/random_800.txt"

#num_examples = 4
#category_name = "test"
#category_list_file = "test_cat_ids.txt"
#category_members = "random_test.txt"
#category_nonmembers = "random_nontest.txt"

#output files
category_id_file = category_name + "_ids_revised.txt"
auth_scores_file = category_name + "_auth_scores_revised.txt"
hub_scores_file = category_name + "_hub_scores_revised.txt"
features_file = category_name + "_features_revised.csv"
category_one_hop_file = category_name + "_one_hop_ids.txt"

SERVER = "ec2-50-112-6-22.us-west-2.compute.amazonaws.com" #new
#SERVER = 'ec2-50-112-32-119.us-west-2.compute.amazonaws.com' #old
PORT = 1000

db = pymongo.Connection(SERVER, PORT).wp

def write_category_file():
	cat_file = open(category_list_file, 'r')
	categories = [unicode(line.strip()) for line in cat_file.readlines()]

	actors = db.pages.find({u'categories' : {'$in' : categories}})
	print actors.count()
	
	out = open(category_id_file, 'w')
	for actor in actors:
		out.write(str(actor['_id']) + '\n')
		
	out.close()
	cat_file.close()
	
def category_graph():
	actor_file = open(category_id_file,'r')
	actors = [int(line) for line in actor_file.readlines()]
	
	bad_file = open(actors_to_remove, 'r')
	bad_actors =  [int(line) for line in bad_file.readlines()]
	
	bad_file2 = open(non_actors_to_remove, 'r')
	bad_actors2 = [int(line) for line in bad_file2.readlines()]
	
	print len(bad_actors)
	print len(bad_actors2)
	
	setall = set(actors)
	set1 = set(bad_actors)
	set2 = set(bad_actors2)
	
	print "intersection 1", len(set1 & setall)
	print "intersection 2", len(set2 & setall)

	filtered = setall - set1 - set2
	
	print len(filtered)
	
	filtered_list = list(filtered)
	
	print len(filtered_list)
	
	#print len(actors)
	actors = db.pages.find({u'_id' : {'$in' : filtered_list}})
	
	graph = nx.DiGraph()
	for actor in actors:
		add_to_graph(actor, graph)
		
	return graph
	
def add_to_graph(actor, graph):
	id = actor[u'_id']
	in_edges = []
	out_edges = []
	if u'oe' in actor:
		out_edges = actor[u'oe']
	if u'ie' in actor:
		in_edges = actor[u'ie']
	graph.add_node(id)
	
	#print in_edges
	
	for edge in out_edges:
		graph.add_edge(id, int(edge))
	for edge in in_edges:
		graph.add_edge(id, int(edge))
		
def hits_score_for_node(id, hubs, auth):
	actor = db.pages.find_one({u'_id' : id})
	
	if not actor or not u'ie' in actor:
		#print "uh oh"
		return 0
		
	ids = [int(id) for id in actor[u'ie']]
	
	#print "num ids", len(ids)
	
	auth_sum = 0
	count = 0
	for id in ids:
		#print id
		if id in hubs:
			count += 1
			#print "in hubs"
			auth_sum += hubs[id]
		
	print "had", count, "ids in the graph for a score of ", auth_sum
	return auth_sum
	
def save_hits_info(hubs, auths):
	auths_out = open(auth_scores_file, 'w')
	for id in auths:
		auths_out.write(str(id) + ' ' + str(auths[id]) + '\n')
	auths_out.close()
	
	hubs_out = open(hub_scores_file, 'w')
	for id in hubs:
		hubs_out.write(str(id) + ' ' + str(auths[id]) + '\n')
	hubs_out.close()
	
def read_hubs():
	print "hubs file:", hub_scores_file
	hubs_reader = csv.reader(open(hub_scores_file), delimiter=' ')
	
	hubs_dict = {}
	
	for row in hubs_reader:
		hubs_dict[int(row[0])] = float(row[1])
		
	return hubs_dict
	
if __name__ == "__main__":
	args = sys.argv
	
	features_file = args[1]
	num_examples = int(args[2])
	category_list_file = args[3]
	category_members = args[4]
	category_nonmembers = args[5]
	
	actors_to_remove = args[6]
	non_actors_to_remove = args[7]
	
	print actors_to_remove
	print non_actors_to_remove

	write_category_file()
	graph = category_graph()
	
	print graph.number_of_nodes()
	
	#out = open(category_one_hop_file, 'w')
	
	#for node in graph.nodes():
	#	out.write(str(node) + "\n")
		
	#out.close()
	#print graph.number_of_nodes()
	actor_id_file = open(category_members, 'r')
	actor_ids =	[int(line) for line in actor_id_file.readlines()]
	actor_ids = actor_ids[0:num_examples]
	
	nonactor_id_file = open(category_nonmembers, 'r')
	nonactor_ids = [int(line) for line in nonactor_id_file.readlines()]
	nonactor_ids = nonactor_ids[0:num_examples]
	
	#print actor_ids, nonactor_ids
	
	try:
		print "reading from hits scores from file"
		hub = read_hubs()
		auth = {}
	except:
		print "couldn't find file... calculating hits scores"
		hub,auth = nx.hits(graph)
		save_hits_info(hub, auth)
	
	features = open(features_file, 'w')
	
	features.write("id,hits\n")
	print "Actors"
	for actor_id in actor_ids:
		print "actor id", actor_id
		auth_score = hits_score_for_node(actor_id, hub, auth)
		#print auth_score
		features.write(str(actor_id) + "," + str(auth_score) + "\n")
		
	print "Non actors"
	for nonactor_id in nonactor_ids:
		print "non actor id", nonactor_id
		auth_score = hits_score_for_node(nonactor_id, hub, auth)
		features.write(str(nonactor_id) + "," + str(auth_score) + "\n")
		
	features.close()
