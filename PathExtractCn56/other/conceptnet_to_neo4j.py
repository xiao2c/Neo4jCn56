"""
 This script takes in as a parameter, the location of ConceptNets CSV dump, and generates a CSV file that can be
 imported into a Neo4J database.

 ConceptNet's csv dump can be found: https://github.com/commonsense/conceptnet5/wiki/Downloads
"""
import csv, os, json

# Location for the output nodes.csv file
nodes_location = "nodes.csv"
# Location of the relationships.csv file
relationship_location = "relationships.csv"

nodes = []
relationships = []

with open("conceptnet-assertions-5.6.0.csv", "r") as f:
	rows = csv.reader(f, delimiter="\t")
	for i, row in enumerate(rows):
		print("Dev2 Progress: " + str(i*100.0/32755210) + "%")
		rel = row[1]
		start = row[2]
		end = row[3]
		info = row[4]
		if start.startswith("/c/en/") and end.startswith("/c/en/") and rel.startswith("/r/"):
			idata = json.loads(info)
			weight = None
			surfaceEnd = None
			surfaceStart = None
			surfaceText = None
			dataset = None
			if "weight" in idata:
				weight = idata["weight"]
			if "surfaceEnd" in idata:
				surfaceEnd = idata["surfaceEnd"]
			if "surfaceStart" in idata:
				surfaceStart = idata["surfaceStart"]
			if "surfaceText" in idata:
				surfaceText = idata["surfaceText"]
			if "dataset" in idata:
				dataset = idata["dataset"]
			sname = start.split("/")[3]
			ename = end.split("/")[3]
			rname = rel.split("/")[2]
			#print("i="+str(i)+", "+sname+";"+ename+";"+rname+"; "+str(dataset)+", "+str(surfaceEnd)+", "+str(surfaceStart)+", "+str(surfaceText)+", "+str(weight))
			#if [start, sname, "Concept"] not in nodes:
			nodes.append([start, sname, "Concept"])
			#if [end, ename, "Concept"] not in nodes:
			nodes.append([end, ename, "Concept"])
			relationships.append([start, end, rname, weight, surfaceText, surfaceStart, surfaceEnd, dataset])
			#duplicate nodes included in the nodes list, remove duplicate line from nodes.csv is required. (sort filename | uniq -u)

with open("nodes.csv", "w") as f:
	writer = csv.writer(f)
	writer.writerow(["uri:ID", "name", ":LABEL"])
	for n in nodes:
		writer.writerow(n)

with open("relationships.csv", "w") as f:
	writer = csv.writer(f)
	writer.writerow([":START_ID",":END_ID",":TYPE", "weight","surfaceText","surfaceStart","surfaceEnd","dataset"])
	for r in relationships:
		writer.writerow(r)

