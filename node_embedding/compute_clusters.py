import os
import sys
import os.path as path
import shelve
import numpy as np
import pandas as pd
import math
import re
import sklearn as sk
import sklearn.cluster as sk_cluster
import json
import sqlite3
import random
import time
import meshio
import pickle as pkl

from scipy.spatial import ConvexHull
from sklearn.decomposition import KernelPCA

db_version = 'v2.0'
scale = 2000

cluster_counts = [5, 10, 15, 25, 50, 100]

source_file = sys.argv[1]
p_value = sys.argv[3]
q_value = sys.argv[4]
rwl_value = sys.argv[5]

if float(p_value) == 1:
    p_str = ''
else:
    p_str = 'p' + str(p_value) + '_'
if float(q_value) == 1:
    q_str = ''
else:
    q_str = 'q' + str(q_value) + '_'
if float(rwl_value) == 40:
    rwl_str = ''
else:
    rwl_str = 'rwl' + str(rwl_value) + '_'



def get_range(file, start, stop):
    if start == 1:
        data = pd.read_csv(file, nrows = stop)
    else:
        data = pd.read_csv(file, nrows = stop).tail(n=stop-start+1)
    return data

def read_file(file, start, stop, cull = False, nsfw = True, threads = True, cluster = False):
    data = get_range(file, start, stop)
    if cull:
        mean = data[['x', 'y', 'z']].to_numpy().mean(0)

        distances = ((data[['x', 'y', 'z']].to_numpy()-mean)**2).sum(1)
        dist_mean = distances.mean()
        dist_std = distances.std()
        stdevs_mean = (distances-dist_mean)/dist_std
        data = data.assign(dist=stdevs_mean)
        data = data.sort_values(by = 'dist').head(n=int(data.shape[0]*0.999))
        data = data.drop('dist', axis = 1)

    if nsfw:
        nsfw = pd.read_csv('output/nsfw_' + db_version + '.csv')
        nsfw.columns = ['subreddit', 'nsfw', 'threads', 'percentNsfw']
        data = data.join(nsfw[['subreddit', 'threads', 'percentNsfw']].set_index('subreddit'), on = 'subreddit')
        if not threads:
            data = data.drop('threads')
        else:
            data = data.assign(threads = (np.where(data.threads.isnull(), 0, data.threads)))
            data = data.assign(percentNsfw = np.where(data.percentNsfw.isnull(), 0, data.percentNsfw)*100)
    return data

def getpointdata(start, stop):
    file = 'cluster_data/cluster_' + db_version + '_' + p_str + q_str + rwl_str + str(stop) + '.csv'
    
    if path.exists(file):
        data = read_file(file, start, stop, cull = True, nsfw = True, cluster = True)
    else:
        data = get_range(source_file, start, stop)
        data.columns = ['subreddit', 'x', 'y', 'z']

        nsfw = pd.read_csv('output/nsfw_' + db_version + '.csv')
        nsfw.columns = ['subreddit', 'nsfw', 'threads', 'percentNsfw']
        data = data.join(nsfw[['subreddit', 'threads']].set_index('subreddit'), on = 'subreddit')
        data = data.assign(log_threads = np.log10(np.where(data.threads.isnull(), 0, data.threads)+1))
        data = data.drop('threads', axis = 1)

        mean = data[['x','y','z']].to_numpy().mean(0)
        distances = ((data[['x','y','z']].to_numpy()-mean)**2).sum(1)
        data = data.assign(dist = distances)
        data = data.drop('dist', axis = 1)

        for i in cluster_counts:
            print(i)
            cluster = sk_cluster.KMeans(n_clusters=i, n_init=50).fit(data[['x','y','z']], data['log_threads'])
            data['cluster_'+str(i)] = cluster.labels_
        data.to_csv(file, index = False)
        data = read_file(file, start, stop, cull = True, nsfw = True, cluster = True)

    x_min = min(data['x'])
    x_max = max(data['x'])
    y_min = min(data['y'])
    y_max = max(data['y'])
    z_min = min(data['z'])
    z_max = max(data['z'])

    data = data.assign(x = lambda dataframe: dataframe['x'].map(lambda x: (x - float(x_min))*scale/(x_max-x_min)-scale/2))
    data = data.assign(y = lambda dataframe: dataframe['y'].map(lambda y: (y - float(y_min))*scale/(y_max-y_min)-scale/2))
    data = data.assign(z = lambda dataframe: dataframe['z'].map(lambda z: (z - float(z_min))*scale/(z_max-z_min)-scale/2))
    data = data.loc[:, ~data.columns.str.contains('^Unnamed')].sort_values('subreddit')
    return data

def processarguments(start, stop, cluster_hulls = ''):
    start = float(re.sub(r'start:', '', start))
    stop = float(re.sub(r'stop:', '', stop))
    cluster_hulls = re.sub(r'cluster_hulls:', '', cluster_hulls)
    return start, stop, cluster_hulls
 
def get(start, stop, cluster_hulls):
    data = getpointdata(start, stop)

    if cluster_hulls == 'none':
        return json.loads(data.to_json(orient='records').__str__())

    clusterlist = {}
    for i in cluster_counts:
        hull_data = []
        for cluster in data['cluster_'+str(i)].unique():
            subset = data[data['cluster_'+str(i)]==cluster]
            if subset.shape[0] <= 2:
                continue
            mean = subset[['x','y','z']].mean(0).to_numpy()
            distances = ((subset[['x','y','z']].to_numpy()-mean)**2).sum(1)
            dist_mean = distances.mean()
            dist_std = distances.std()
            if dist_std == 0:
                print(str(subset))
                print(mean)
                print(distances)

            stdevs_mean = (distances-dist_mean)/dist_std
            subset = subset.assign(dist = stdevs_mean)
            subset = subset[subset['dist'] <= 2]
            points = subset[['x','y','z']].to_numpy()
            try:
                hull = ConvexHull(points)
            except:
                continue

            center = hull.points.mean(0)
            simplexlist = []
            for simplex in hull.simplices:
                x = hull.points[simplex[0]]
                y = hull.points[simplex[1]]
                z = hull.points[simplex[2]]

                if np.dot(np.cross(y-x, z-x),(x+y+z)/3-center) < 0:
                    simplexlist.append([simplex[0],simplex[2],simplex[1]])
                else:
                    simplexlist.append(simplex)

            hull.simplices = np.vstack(simplexlist)

            
            mesh = meshio.Mesh(hull.points, cells = [("triangle", hull.simplices)])
            mesh.write("tmp.obj")
            with open("tmp.obj", "r") as f:
                file = f.read()
            
            hull_data.append({'id' : str(int(cluster)), 'obj' : file})
        clusterlist.update({str(i) : hull_data})

    data_list = json.loads(data.to_json(orient = 'records'))
    new_data_list = []
    for row in data_list:
        new_data_list.append({
            'subreddit' : row['subreddit'],
            'x' : row['x'],
            'y' : row['y'],
            'z' : row['z'],
            'log_threads' : row['log_threads'],
            'cluster' : [value for key, value in row.items() if key.startswith("cluster_")],
            'threads' : row['threads'],
            'percentNsfw' : row['percentNsfw']
        })

    if cluster_hulls == 'only':
        result = clusterlist
    else:
        result = {
            "clusterCounts" : cluster_counts,
            "data" : new_data_list,
            "clusters" : clusterlist
        }
    return re.sub(r"'", r'"', re.sub(r'\\n', r'\n', result.__str__()))

print(get(1, int(sys.argv[2]), "yes"))


