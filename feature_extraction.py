from __future__ import division
import pymongo
from pymongo import MongoClient
import re
from datetime import datetime
import requests, time
import numpy as np
import dateutil.parser
import datetime
import math
import pandas as pd
import matplotlib.pyplot as plt
import multiprocessing
import statistics as st
from multiprocessing import Pool, Process, Queue

# Connect to MongoDB
client = MongoClient()
db = client.DB_NAME

# Connect to tweets db
collectionName = TWEET_DB
group_collectionName = GROUP_DB

CONFIG_POOL_SIZE = 5

#pandas Dataframe
df = pd.DataFrame(columns=['groupID', 'inter_posting_time_compactness', 'retweeting_time_distribution_sd',
'retweeting_time_distribution_mean', 'retweeting_time_distribution_cov', 'cov_response_time', 
'user_creation_time_distribution_sd', 'user_creation_time_distribution_mean', 'user_creation_time_distribution_cov', 'label'])

def gridCalculation(pairsGroupWise_x, pairsGroupWise_y, lengthOfGroup):
    # Square grid calculations
    xVals = np.array(pairsGroupWise_x)
    yVals = np.array(pairsGroupWise_y)

    grid = 0
    
    gridx = np.linspace(0, lengthOfGroup, lengthOfGroup)
    gridy = np.linspace(0, lengthOfGroup, lengthOfGroup)

    #print len(gridx)
    #print len(gridy)
    
    
    if len(gridx) < 2:
        grid = 0
        
    elif len(gridy) < 2:
        grid = 0
        
    else:
        grid,_,_ = np.histogram2d(xVals, yVals, bins=[gridx, gridy])
    return grid
    
def density_features(created_time_arr):
    postingTime_sorted = sorted(created_time_arr)
    deltaArr = []
    for i, x in enumerate(postingTime_sorted):

        if i == len(postingTime_sorted)-1:
            break
        deltaArr.append((postingTime_sorted[i+1] - x).total_seconds())
    
    for (p, item) in enumerate(deltaArr):
        if item < 1:
            deltaArr[p] = 2
            
    pairsOfDelta_x = []
    pairsOfDelta_y = []
    pairsGroupWise_x = []
    pairsGroupWise_y = []
    
    for i in xrange(len(deltaArr) - 1):
        current_item, next_item = deltaArr[i], deltaArr[i + 1]
        pairsOfDelta_x.append(math.log(current_item))
        pairsOfDelta_y.append(math.log(next_item)) 
    
        
    pairsGroupWise_x.extend(pairsOfDelta_x)
    pairsGroupWise_y.extend(pairsOfDelta_y)
    
    pairsGroupWise_x = [int(i) for i in pairsGroupWise_x]
    pairsGroupWise_y = [int(i) for i in pairsGroupWise_y]

    
    min_x, max_x = min(pairsGroupWise_x), max(pairsGroupWise_x)
    min_y, max_y = min(pairsGroupWise_y), max(pairsGroupWise_y)
    m = max(max_x - min_x + 1, max_y - min_y + 1)

    #density_grid = np.zeros(5)
    
    density_grid = gridCalculation(pairsGroupWise_x, pairsGroupWise_y, m)


    if type(density_grid) is np.ndarray:
        density = (density_grid.max()/sum(map(sum,density_grid)))
    else:
        density = 0
        
    return density
    
 def retweeting_time_dispersion(all_members_arr):

    all_std_arr = []
    all_diff_arr = []
    for a_list in all_members_arr:
        sorted_list = sorted(a_list)
        diff_arr = []
        
        for i, x in enumerate(sorted_list):
            if i == len(sorted_list)-1:
                break
            diff_arr.append((sorted_list[i+1] - x).total_seconds())
        all_diff_arr.append(diff_arr)
            

    for diff in all_diff_arr:
        all_std_arr.append(st.pstdev(diff))
        

    s_dev = st.pstdev(all_std_arr)
    m_ean = st.mean(all_std_arr)
    cov = s_dev/float(m_ean)

    return s_dev, m_ean, cov
    
def creation_time_dispersion(rt_user_created_time_arr):
    t_diff_list = []
    sorted_time_list = sorted(rt_user_created_time_arr)
    for i, x in enumerate(sorted_time_list):
        if i == len(sorted_time_list)-1:
            break
        t_diff_list.append((sorted_time_list[i+1] - x).total_seconds())
        
    sde_v = st.pstdev(t_diff_list)
    mea_n = st.mean(t_diff_list)
    cov = sde_v/float(mea_n)

    return sde_v, mea_n, cov
    
    
 def features(args):
    group, total_count = args
    user_id = group['group_ids']
    label = group['label']
    sn = group['sn']
    print sn
    
    source_tweet_created_time_arr = []
    source_user_created_time_arr = []
    all_user_rt_tt_diff = []
    rt_tweet_created_time_arr = []
    rt_user_created_time_arr = []
    all_members_rting_arr = []
    
    for a_user in user_id:
    
        #print a_user
        single_member_rting_arr = []
        single_user_rt_tt_diff = []
        data = db[collectionName].find({'rt_user_id': a_user}, {'source_tweet_created_time':1, 'source_user_created_at':1, 'rt_tweet_created_time':1}, no_cursor_timeout=True)
        for d in data:
            rt = datetime.datetime.strptime(dateutil.parser.parse(d['rt_tweet_created_time']).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
            rt_tweet_created_time_arr.append(rt)
            single_member_rting_arr.append(datetime.datetime.strptime(dateutil.parser.parse(d['rt_tweet_created_time']).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S'))
            tt = datetime.datetime.strptime(dateutil.parser.parse(d['source_tweet_created_time']).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
            single_user_rt_tt_diff.append((rt - tt).total_seconds())
            all_user_rt_tt_diff.append(st.median(single_user_rt_tt_diff))
        all_members_rting_arr.append(single_member_rting_arr)
            
        creation_data = db[collectionName].find({'rt_user_id': a_user}, {'rt_user_created_at':1}, no_cursor_timeout=True).limit(1)
        for cd in creation_data:
            rt_user_created_time_arr.append(datetime.datetime.strptime(dateutil.parser.parse(cd['rt_user_created_at']).strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S'))
          
    ipt_d = density_features(rt_tweet_created_time_arr)
    rter_creation_d_std, rter_creation_mean, rter_creation_d_cov = creation_time_dispersion(rt_user_created_time_arr)
    s_td, me_an, co_v = retweeting_time_dispersion(all_members_rting_arr)
    
    cov_of_response_times = st.pstdev(all_user_rt_tt_diff)/float(st.mean(all_user_rt_tt_diff))
    return sn, ipt_d, s_td, me_an, co_v, cov_of_response_times, rter_creation_d_std, rter_creation_mean, rter_creation_d_cov,label
    
 if __name__ == '__main__':
    pool = multiprocessing.Pool(CONFIG_POOL_SIZE)
    mongo_query = {}
    groups = db[group_collectionName].find(mongo_query, no_cursor_timeout=True)
    total_count= groups.count()
    all_data = pool.map(features, ((group, total_count) for idx,group in enumerate(groups)))
    pool.close()
    pool.join()   
    
    df = pd.DataFrame(all_data, columns=['groupID', 'inter_posting_time_compactness', 'retweeting_time_distribution_sd',
'retweeting_time_distribution_mean', 'retweeting_time_distribution_cov', 'cov_response_time', 
'user_creation_time_distribution_sd', 'user_creation_time_distribution_mean', 'user_creation_time_distribution_cov', 'label'])

    df.to_pickle('extracted_features/feature.pkl')
    
client.close()    
