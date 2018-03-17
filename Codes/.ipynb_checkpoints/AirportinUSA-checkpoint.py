
# coding: utf-8

# ## Experimenting with Regular Expression
# 
# #### Matches commas in a string but not within double quotes
# #### ? = match 0 or 1
# #### * match 0 or more
# #### dollar sign :match the end of string
# #### [ ] : range or variance
# #### ^ match the begin of string
# #### { x } : expecting x amount 

# In[1]:


import re
class Utils():
    Comma_Delim = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')


    


# In[2]:


import sys
print(sys.version)


# In[3]:


from pyspark import SparkContext, SparkConf


# In[4]:


def split_comma(line: str):
    split = Utils.Comma_Delim.split(line)
    return "{}, {}".format(split[1],split[2])
    
    


# In[5]:


if __name__=="__main__":
    conf = SparkConf().setAppName("airport").setMaster("local[2]")
    sc = SparkContext.getOrCreate()
    airports = sc.textFile('/Users/ashishdwivedi/anaconda3/envs/ml-lib/Spark/Codes/in/airports.text')
    
    airportInUSA=airports.filter(lambda line : Utils.Comma_Delim.split(line)[3]=="\"United States\"")
    
    airportNameandCityName = airportInUSA.map(split_comma)
    airportNameandCityName.saveAsTextFile('/Users/ashishdwivedi/anaconda3/envs/ml-lib/Spark/Codes/out/USA_Airports.text')

