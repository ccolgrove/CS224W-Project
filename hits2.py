import numpy
import networkx as nx
import pymongo
import csv

#data files
num_examples = 50
category_name = "actor"
category_list_file = "american_actors_categories_catids_noDuplicates.txt"
category_members = "random_actors.txt"
category_nonmembers = "random_nonActors.txt"

#num_examples = 4
#category_name = "test"
#category_list_file = "test_cat_ids.txt"
#category_members = "random_test.txt"
#category_nonmembers = "random_nontest.txt"

#output files
category_id_file = category_name + "_ids.txt"
auth_scores_file = category_name + "_auth_scores.txt"
hub_scores_file = category_name + "_hub_scores.txt"
features_file = category_name + "_features.csv"
category_one_hop_file = category_name + "_one_hop_ids.txt"

SERVER = "ec2-50-112-6-22.us-west-2.compute.amazonaws.com" #new
#SERVER = 'ec2-50-112-32-119.us-west-2.compute.amazonaws.com' #old
PORT = 1000

db = pymongo.Connection(SERVER, PORT).wp

def write_category_file():
	cat_file = open(category_list_file, 'r')
	categories = [unicode(line.strip()) for line in cat_file.readlines()]

	actors = db.pages.find({u'categories' : {'$in' : categories}})
	#print actors.count()
	
	out = open(category_id_file, 'w')
	for actor in actors:
		out.write(str(actor['_id']) + '\n')
		
	out.close()
	cat_file.close()
	
def category_graph():
	actor_file = open(category_id_file,'r')
	actors = [int(line) for line in actor_file.readlines()]
	#print len(actors)
	actors = db.pages.find({u'_id' : {'$in' : actors}})
	
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
		return 0
		
	ids = [int(id) for id in actor[u'ie']]
	
	auth_sum = 0
	for id in ids:
		#print id
		if id in hubs:
			#print "in hubs"
			auth_sum += hubs[id]
		
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
	
	category_name = args[1]
	num_examples = args[2]
	category_list_file = args[3]
	category_members = args[4]
	category_nonmembers = args[5]

	write_category_file()
	graph = category_graph()
	
	print graph.number_of_nodes()
	
	out = open(category_one_hop_file, 'w')
	
	for node in graph.nodes():
		out.write(str(node) + "\n")
		
	out.close()
	#print graph.number_of_nodes()
	actor_id_file = open(category_members, 'r')
	actor_ids =	[int(line) for line in actor_id_file.readlines()]
	actor_ids = actor_ids[0:num_examples]
	
	nonactor_id_file = open(category_nonmembers, 'r')
	nonactor_ids = [int(line) for line in nonactor_id_file.readlines()]
	nonactor_ids = nonactor_ids[0:num_examples]
	
	print actor_ids, nonactor_ids
	
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
	for actor_id in actor_ids:
		auth_score = hits_score_for_node(actor_id, hub, auth)
		#print auth_score
		features.write(str(actor_id) + "," + str(auth_score) + "\n")
		
	for nonactor_id in nonactor_ids:
		#auth_score = hits_score_for_node(nonactor_id, hub, auth)
		features.write(str(nonactor_id) + "," + str(auth_score) + "\n")
		
	features.close()