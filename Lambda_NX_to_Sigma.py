# Version 2.7 and modified for AWS Lambda
# This responds to search requests and queries the server and returns a JSON from NetworkX package


import pymysql
import sys
from flask import json
import networkx as nx
from networkx.readwrite import json_graph
from random import randint as rand
import logging
import rds_config
from warnings import filterwarnings


#Supresses warnings
filterwarnings("ignore", category=pymysql.Warning)

#Database connection information stored in private file
rds_host = rds_config.host
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Creates new network X graph
NG = nx.Graph()

# Fucntion for creating Network X graphs
def graphing(pair, node, word):
    word = word.lower()

    # Primary list and tertiary pair lists are split up for location placement
    primary_list = [k.replace(" " + word, '') for k, v in pair.items() if v[1] == 'P']
    tertiary_list = [k.split() for k, v in pair.items() if v[1] == 'T']

    num = 1
    # Function used for creating networkX graphs
    NG = nx.Graph()

    # Creates edges by adding node pairs and weights
    for k, v in pair.items():
        p = k.split(" ", 1)
        w = v[0]/2
        NG.add_edge(p[0], p[1], id=num, weight=w, color='#bcdbf6', size=1)  # Creates edges
        num = num + 1

    maxval = max(node.values(), key=lambda x: x)

    prim_co_ord = {}

    NG.add_node(word, size=maxval, label=word, x=3, y=3, color='#F6851F',
                borderColor='#bcdbf6', borderWidth=2)

    # Creates nodes
    for p in primary_list:
        val = node[p]

        # Normalizing size function
        v = ( (val / (maxval*0.6) * 12) + 12 )
        # Randomly generates some co-ordinates
        co_ord_x = rand(90, 110) / float(100)
        co_ord_y = rand(50, 200) / float(100)

        prim_co_ord[p] = [co_ord_x, co_ord_y]

        NG.add_node(p, size=v, label=p, x=co_ord_x, y=co_ord_y, color='#F6851F',
                    borderColor='#bcdbf6', borderWidth=2)

    for t in tertiary_list:
        u = t[0]                                    # Label
        w = t[1]                                    # Name parent node
        val = node[u]                               # Count number used to determine size
        v = ((val / (maxval * 0.6) * 12) + 12)      # Normalizing size function

        # Randomly generates some co-ordinates based on primary co-ordinates
        tert_co_ord_x = prim_co_ord[w][0] + (rand(-5, 5) / float(100))
        tert_co_ord_y = prim_co_ord[w][1] + (rand(-5, 5) / float(100))

        # NX module for creating nodes similar to JSON
        NG.add_node(u, size=v, label=u, x=tert_co_ord_x, y=tert_co_ord_y, color='#F6851F',
                    borderColor='#bcdbf6', borderWidth=2)

    # converts the node to JSON format
    fixNG = json_graph.node_link_data(NG)

    # Fixes the network so that edges use node names instead of integers
    fixNG['links'] = [

        {
            'source': fixNG['nodes'][link['source']]['id'],
            'target': fixNG['nodes'][link['target']]['id'],
            'id': link['id'], 'size': link['size'], 'color':'#bcdbf6'
        }

        for link in fixNG['links']]

    fixNG = str(json.dumps(fixNG))
    rtnNG = fixNG.replace('links', 'edges')                 # Changes links to edges

    return rtnNG

# Function that parses the data into dictionaries
# Primary and tertiary key entries are used to determine the level of nodes that a word is related to
def parse(listSQL, word):
    pairDict = {}
    nodeDict = {}

    # Determines (P)rimary key pair entries and returns the highest 10
    pLst = [l for l in listSQL if l[3] == 'P']
    pLst = sorted(pLst, key=lambda x: x[2], reverse=True)[:10]

    m_pLst = [n[0] for n in pLst]

    # Determines (T)ertiary key pair entries and returns the highest 20 IF they are found also in the primary list
    tLst = [l for l in listSQL if l[3] == 'T' and l[1] in set(m_pLst)]
    tLst = sorted(tLst, key=lambda x: x[2], reverse=True)[:20]


    for l in tLst:
        pLst.append(l)

    for lst in pLst:

        x = lst[0].lower()                   # Variables for checking existence
        y = lst[1].lower()
        z = lst[2]

        if x and y:
            key = (x+" "+y)                   # Constructing key in dictionary to match
            varkey = (y+" "+x)                # Alternate order of words, since don't want two edges joining them
            val = lst[2], lst[3]
            val = list(val)                   # Constructing a list to enter into the dict

            # Construction of Pairdict is used for edge generation
            # Checks first tto see if the entry already exists
            if key in pairDict:
                pass
            elif varkey in pairDict:
                pass
            else:
                pairDict[key] = val

            # Construction of nodeDict used for nodes
            if x in nodeDict:
                nodeDict[x] += z
            else:
                nodeDict[x] = z

            if y in nodeDict:
                nodeDict[y] += z
            else:
                nodeDict[y] = z

    return graphing(pairDict, nodeDict, word)

# Fetches from database
def dbfetch(word):

    resList = []

    try:
        # Establishes db connection
        conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=10)
        logger.info("SUCCESS: Connection to RDS mysql instance succeeded")

        # Three SQL queries are made in sequence
        SQL_0 = """drop table if exists temp;"""
        SQL_a = """create temporary table temp (select distinct n_name from links where m_key = '{}');""".format(word)
        SQL_b = """select n_name, m_key, count(*) as weight, 'P' as class from links where m_key = '{}' group by n_name
                         union
                                select links.n_name, links.m_key, count(*) as weight, 'T' as class from links,
                                    temp where links.m_key = temp.n_name group by n_name, m_key having count( * ) > 2 order by m_key """.format(word)


        # Simple returns the results of the SQL query into a list and then closes connection when finished
        with conn.cursor() as cur:
            cur.execute(SQL_0)
            cur.execute(SQL_a)
            cur.execute(SQL_b)
            cur.close()
            for row in cur:
                resList.append(row)

        conn.close()

        return resList

    except:
        logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
        sys.exit()


# Initial call of function within AWS Lambda
def trigger(event, context):

    # Catches the search query
    text = event['message']
    processed_text = text.upper()

    # Calls functions to fetch from the database and parse into graph format
    out = (parse(dbfetch(processed_text), processed_text))

    if out is not None:
        return out

    else:
        return "That word is not in the database"



