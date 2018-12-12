from __future__ import division
%matplotlib inline
import json
import pymongo
import random
import operator
from pymongo import MongoClient
import re, urlparse, httplib
import requests, time
import pandas as pd
import numpy as np
from datetime import datetime
from collections import Counter
from cosineSim import text_to_vector
from cosineSim import get_cosine
from itertools import groupby
from scipy.stats import variation
import networkx as nx
from networkx.algorithms import core, bipartite
import networkx.algorithms.components.connected
from multiprocessing import Pool, Process, Queue

# Connect to MongoDB
client = MongoClient()
db = client.DB_NAME

# Connect to db tweets
which_dataset = group_collectionName.split('_')
file_name = which_dataset[1]
CONFIG_POOL_SIZE = 5

#pandas Dataframe
df = pd.DataFrame(columns= ['groupID','digitsInScreenName_entropy', 'hashtagsInUserName_entropy',
        'specialCharacters_sname_entropy', 'specialCharacters_uname_entropy', 'url_in_bio_entropy',
        'mention_in_bio_entropy', 'hashtags_in_bio_entropy', 'length_screen_name_entropy','length_user_name_entropy',
    'rt_user_statuses_count_entropy', 'rt_user_listed_count_entropy', 'rt_user_favourites_count_entropy','label'])

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

def digitsInScreenName(screen_name):
    numbers = sum(c.isdigit() for c in screen_name)
    return numbers

def hashtagsInUserName(user_name):
    hashtags = {tag.strip("#") for tag in user_name.split() if tag.startswith("#")}
    if hashtags:
        return len(hashtags)
    return 0

def detectSpecialCharacters(name):
    specialcharsInnames = re.sub('[\w]+' ,'', name)
    # Length of special characters of usernames and screenames
    return len(specialcharsInnames)

def getUrlMentionsHashtags(string):
    if string == None:
        return 0,0,0
    urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', string)
    mentions = re.findall('(?<=^|(?<=[^a-zA-Z0-9-_\.]))@([A-Za-z]+[A-Za-z0-9]+)',string)
    hashtags = re.findall('#(\w+)', string)

    return len(urls), len(mentions), len(hashtags)


def rter_features(args):
    group, total_count = args
    user_id = group['pruned_group']
    label = group['label']
    sn = group['sn']
    print sn
    digitsInScreenName_list = []
    hashtagsInUserName_list = []
    specialCharacters_sname_list = []
    specialCharacters_uname_list = []
    url_in_bio_list = []
    mention_in_bio_list = []
    hashtags_in_bio_list = []
    length_screen_name_list = []
    length_user_name_list = []
    rt_user_statuses_count_list = []
    rt_user_listed_count_list = []
    rt_user_favourites_count_list = []

    for user in user_id:
        rter = db[collectionName].find({'rt_user_id': user}).limit(1)
        for i in rter:
            digitsInScreenName_list.append(digitsInScreenName(i['rt_user_screen_name']))
            hashtagsInUserName_list.append(hashtagsInUserName(i['rt_user_name']))
            specialCharacters_sname_list.append(detectSpecialCharacters(i['rt_user_screen_name']))
            specialCharacters_uname_list.append(detectSpecialCharacters(i['rt_user_name']))
            ul,ms,hs = getUrlMentionsHashtags(i['rt_user_description'])
            url_in_bio_list.append(ul)
            mention_in_bio_list.append(ms)
            hashtags_in_bio_list.append(hs)
            length_screen_name_list.append(len(i['rt_user_screen_name']))
            length_user_name_list.append(len(i['rt_user_name']))
            rt_user_statuses_count_list.append(i['rt_user_statuses_count'])
            rt_user_listed_count_list.append(i['rt_user_listed_count'])
            rt_user_favourites_count_list.append(i['rt_user_favourites_count'])


    digitsInScreenName_entropy = entropy(digitsInScreenName_list)

    hashtagsInUserName_entropy = entropy(hashtagsInUserName_list)

    specialCharacters_sname_entropy = entropy(specialCharacters_sname_list)

    specialCharacters_uname_entropy = entropy(specialCharacters_uname_list)

    url_in_bio_entropy = entropy(url_in_bio_list)

    mention_in_bio_entropy = entropy(mention_in_bio_list)

    hashtags_in_bio_entropy = entropy(hashtags_in_bio_list)

    length_screen_name_entropy = entropy(length_screen_name_list)

    length_user_name_entropy = entropy(length_user_name_list)

    rt_user_statuses_count_entropy = entropy(rt_user_statuses_count_list)

    rt_user_listed_count_entropy = entropy(rt_user_listed_count_list)

    rt_user_favourites_count_entropy = entropy(rt_user_favourites_count_list)


    return sn, digitsInScreenName_entropy, hashtagsInUserName_entropy,specialCharacters_sname_entropy, specialCharacters_uname_entropy,
    url_in_bio_entropy, mention_in_bio_entropy, hashtags_in_bio_entropy, length_screen_name_entropy,length_user_name_entropy,
    rt_user_statuses_count_entropy, rt_user_listed_count_entropy, rt_user_favourites_count_entropy, label


if __name__ == '__main__':
    pool = multiprocessing.Pool(CONFIG_POOL_SIZE)
    mongo_query = {}
    groups = db[group_collectionName].find(mongo_query, no_cursor_timeout=True)
    total_count= groups.count()
    all_data = pool.map(rter_features, ((group, total_count) for idx,group in enumerate(groups)))
    pool.close()
    pool.join()

    df = pd.DataFrame(all_data, columns=['groupID','digitsInScreenName_entropy', 'hashtagsInUserName_entropy',
        'specialCharacters_sname_entropy', 'specialCharacters_uname_entropy', 'url_in_bio_entropy',
        'mention_in_bio_entropy', 'hashtags_in_bio_entropy', 'length_screen_name_entropy','length_user_name_entropy',
    'rt_user_statuses_count_entropy', 'rt_user_listed_count_entropy', 'rt_user_favourites_count_entropy','label'])

    df.to_csv('extracted_features/'+file_name+'_rt_based_entropy_feature.csv', sep=',', encoding='utf-8',index=False)
    df.to_pickle('extracted_features/'+file_name+'_rt_based_entropy_feature.pkl')


client.close()
