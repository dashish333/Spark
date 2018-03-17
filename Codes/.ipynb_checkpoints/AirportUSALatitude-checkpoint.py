#
# The columns of the airport text are : Airport ID,Name of Airport,Main city of airport,Country,code,code,lat,longit,
#
# This code is a part of udemy course and is done as exercise
#

## writing a class to find the possible delimiter

import re
class Utils():
	Comma_Delim = re.compile(''',(?=(?:[^"]*"[^"]*")*[^"]*$)''')


## writng the function which gives output the desired content

def split_Comma(line: str):
	split = Utils.Comma_Delim.split(line) 
	return "{}, {}".format(split[1],split[6])


####### MAIN ######

if __name__=="__main__":
	sc=SparkContext.getOrCreate()
	airports = sc.textFile('/Users/ashishdwivedi/anaconda3/envs/ml-lib/Spark/Codes/in/airports.text')
	## filter operation is done over RDD and function is passed which returns another RDD
	airportInUSA = airports.filter(lambda line : Utils.Comma_Delim.split(line)[3] == "\"United States\"")
	## map
	airportNameandlatitude = airportInUSA.map(split_Comma)
	airportNameandlatitude.saveAsTextFile('/Users/ashishdwivedi/anaconda3/envs/ml-lib/Spark/Codes/out/USA_Airports_Latitude.text')


