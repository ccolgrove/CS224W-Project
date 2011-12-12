import numpy
import networkx as nx
import pymongo
import sys

SERVER = "ec2-50-112-6-22.us-west-2.compute.amazonaws.com" #new
#SERVER = 'ec2-50-112-32-119.us-west-2.compute.amazonaws.com' #old
PORT = 1000

db = pymongo.Connection(SERVER, PORT).wp

category_name = "actor"
category_list_file = "american_actors_categories_catids_noDuplicates.txt"


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

if __name__ == "__main__":
	category_name = sys.argv[1]
	category_list_file = sys.argv[2]
	category_id_file = category_name + "_ids.txt"
	category_one_hop_file = category_name + "_one_hop_ids.txt"

	write_category_file()
	graph = category_graph()
	out = open(category_one_hop_file, 'w')
	
	for node in graph.nodes():
		out.write(str(node) + "\n")
		
	out.close()