import numpy
import networkx as nx
import pymongo
import csv

SERVER = "ec2-50-112-6-22.us-west-2.compute.amazonaws.com" #new
#SERVER = 'ec2-50-112-32-119.us-west-2.compute.amazonaws.com' #old
PORT = 1000

db = pymongo.Connection(SERVER, PORT).wp

def write_actor_file():
	cat_file = open('american_actors_categories_catids_noDuplicates.txt', 'r')
	categories = [unicode(line.strip()) for line in cat_file.readlines()]

	actors = db.pages.find({u'categories' : {'$in' : categories}})
	print actors.count()
	
	out = open('actor_ids.txt', 'w')
	for actor in actors:
		out.write(str(actor['_id']) + '\n')
		
	out.close()
	cat_file.close()
	
def actor_graph():
	actor_file = open('actor_ids.txt','r')
	actors = [int(line) for line in actor_file.readlines()]
	print len(actors)
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
	for edge in out_edges:
		graph.add_edge(id, int(edge))
	for edge in in_edges:
		graph.add_edge(id, int(edge))
	
def hits_score_for_node(id, hubs, auth):
	actor = db.pages.find_one({u'_id' : id})
	
	if not actor or not u'ie' in actor:
		return -1
		
	ids = [int(id) for id in actor[u'ie']]
	
	auth_sum = 0
	
	for id in ids:
		if id in hubs:
			auth_sum += hubs[id]
		
	return auth_sum

def save_hits_info(hubs, auths):
	auths_out = open('auth_scores.txt', 'w')
	for id in auths:
		auths_out.write(str(id) + ' ' + str(auths[id]) + '\n')
	auths_out.close()
	
	hubs_out = open('hub_scores.txt', 'w')
	for id in hubs:
		hubs_out.write(str(id) + ' ' + str(auths[id]) + '\n')
	hubs_out.close()
	
def read_hubs():
	hubs_reader = csv.reader(open('hub_scores.txt'), delimiter=' ')
	
	hubs_dict = {}
	
	for row in hubs_reader:
		hubs_dict[int(row[0])] = float(row[1])
		
	return hubs_dict
	

if __name__ == "__main__":
	#write_actor_file()
	#graph = actor_graph()
	#print graph.number_of_nodes()
	actor_id_file = open('random_actors.txt', 'r')
	actor_ids =	[int(line) for line in actor_id_file.readlines()]
	
	print "running hits..."
	#hub,auth = nx.hits(graph)
	
	#auth = {1:1}
	#hub = {1:2}
	#save_hits_info(hub, auth)
	
	auth = {}
	hub = read_hubs()
	
	print "calculating auth scores...."
	for actor_id in actor_ids:
		auth_score = hits_score_for_node(actor_id, hub, auth)
		print actor_id, auth_score