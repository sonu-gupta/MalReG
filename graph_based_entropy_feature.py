from __future__ import division
get_ipython().magic('matplotlib inline')
import json
import pymongo
import requests
from datetime import datetime
import pandas as pd
import operator
import numpy as np
import random
import networkx as nx
import matplotlib.pyplot as plt
from scipy.stats import variation
import multiprocessing
from multiprocessing import Pool, Process, Queue
from itertools import groupby, repeat
from pymongo import MongoClient
from collections import Counter
from networkx.algorithms import approximation as approx
from networkx.algorithms import connectivity

# Connect to MongoDB
client = MongoClient()
db = client.DB_NAME

# Connect to db tweets
which_dataset = group_collectionName.split('_')
file_name = which_dataset[1]
CONFIG_POOL_SIZE = 5

#pandas Dataframe
df = pd.DataFrame(columns=['groupID','avg_neighbor_degree_entropy','avg_degree_connectivity_entropy','eccentricity_entropy',
                            'label'])



def entropy(list_of_elements):
    if not list_of_elements:
        print "list empty"
        return 0
    entropy = 0
    total_elements = len(list_of_elements)
    c=Counter(list_of_elements)
    input_dict = dict(c)
    for key, value in input_dict.items():
        p = float(value)/total_elements
        p_ = float(1)/p
        log2_p = math.log(p_, 2)
        ent = p*log2_p
        entropy+=ent
    return entropy


def graph_features(rtg,no_of_members, label, sn):

    avg_neighbor_degree = nx.average_neighbor_degree(rtg, weight='weight')
    avg_neighbor_degree_entropy = entropy(avg_neighbor_degree.values())

    avg_degree_connectivity = nx.average_degree_connectivity(rtg, weight='weight')
    avg_degree_connectivity_entropy = entropy(avg_degree_connectivity.values())

    eccent = nx.eccentricity(rtg)
    eccentricity_entropy = entropy(eccent.values())

    return int(sn), avg_neighbor_degree_entropy, avg_degree_connectivity_entropy, eccentricity_entropy

def create_rtgraph(args):
    group, total_count = args
    user_id = group['pruned_group']
    label = group['label']
    sn = group['sn']
    print sn
    g  = nx.Graph()
    tweet_set = []
    g.add_nodes_from(user_id, bipartite=0)
    candidates = db[collectionName].distinct("source_tweet_id", {'rt_user_id': { '$in': user_id }})
    for c in candidates:
            tweet_set.append(c)
    g.add_nodes_from(tweet_set, bipartite=1)
    candidates = db[collectionName].find({'rt_user_id':{ '$in': user_id }}, {'rt_user_id': 1, 'source_tweet_id': 1})
    for c in candidates:
        if 'source_tweet_id' in c:
            user = c['rt_user_id'].encode('utf-8')
            tweet = c['source_tweet_id'].encode('utf-8')
            g.add_edges_from([(user,tweet)])
    top_nodes = set(n for n,d in g.nodes(data=True) if d['bipartite']==0)
    pg = bipartite.weighted_projected_graph(g, top_nodes, ratio=False)

    # Cleaning the Garbage
    candidates = None
    tweet_set = None
    c = None
    user = None
    tweet = None
    g.clear()
    top_nodes = None
    pg.remove_edges_from(pg.selfloop_edges())

    data  = graph_features(pg,len(user_id), label , sn)
    return data


if __name__ == '__main__':
    pool = multiprocessing.Pool(CONFIG_POOL_SIZE)
    mongo_query = {}
    groups = db[group_collectionName].find(mongo_query, no_cursor_timeout=True)
    total_count= groups.count()
    all_data = pool.map(create_rtgraph, ((group, total_count) for idx,group in enumerate(groups)))
    pool.close()
    pool.join()

    df = pd.DataFrame(all_data, columns=['groupID','avg_neighbor_degree_entropy','avg_degree_connectivity_entropy',
    'eccentricity_entropy', 'label'])

    df.to_csv('extracted_features/'+file_name+'_graph_based_entropy_feature.csv', sep=',', encoding='utf-8',index=False)
    df.to_pickle('extracted_features/'+file_name+'_graph_based_entropy_feature.pkl')

client.close()
