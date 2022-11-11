import json
import re
import itertools
import string

# A hack to avoid having to pass 'sc' around
dummyrdd = None
def setDefaultAnswer(rdd): 
    global dummyrdd
    dummyrdd = rdd

def task1(postsRDD):
  return postsRDD.filter(lambda x: x['tags'] != None and 'postgresql-9.4' in x['tags']) \
								 .map(lambda x: (x['id'], x['title'], x['tags']))

def task2(postsRDD):
	return postsRDD.filter(lambda x: x['tags'] != None) \
								 .map(lambda x: {x['id']: list(filter(None, re.split(r'[<>]+', x['tags'])))}) \
								 .flatMap(lambda a: [(x, z) for x, y in a.items() for z in y])

def task3(postsRDD):
	return postsRDD.filter(lambda x: x['tags'] != None) \
								 .map(lambda x: [x['creationdate'][0:4], list(filter(None, re.split(r'[<>]+', x['tags'])))]) \
								 .reduceByKey(lambda a, b: a + b) \
								 .mapValues(lambda a: sorted(list(set(a)))[0:5]) \
								 .map(lambda b: [b[0], b[1]])

def task4(usersRDD, postsRDD):
	t = usersRDD.map(lambda a: (a['id'], a['displayname']))
	v = postsRDD.map(lambda a: (a['owneruserid'], (a['id'], a['title'])))
	return t.join(v).map(lambda a: (a[0], a[1][0], a[1][1][0], a[1][1][1]))

def task5(postsRDD):
	return postsRDD.filter(lambda a: a['title'] != None and a['tags'] != None) \
								 .map(lambda a: (a['title'], a['tags'])) \
								 .map(lambda a: (list(filter(None, a[0].split(" "))), list(filter(None, re.split(r'[<>]+', a[1]))))) \
								 .flatMap(lambda a: itertools.product(a[0], a[1])) \
								 .map(lambda a: (a, 1)) \
								 .reduceByKey(lambda a, b: a + b)

def task6(amazonInputRDD):
	return amazonInputRDD.map(lambda a: a.split(" ")) \
											 .map(lambda a: (a[0][4:len(a[0])], a[1][7:len(a[1])], a[2]))

def task7(amazonInputRDD):
	q = amazonInputRDD.map(lambda a: a.split(" ")) \
										.map(lambda a: (a[0], float(a[2]))) \
										.groupByKey() \
										.mapValues(sum)
	t = amazonInputRDD.map(lambda a: a.split(" ")) \
										.map(lambda a: (a[0], 1)) \
										.groupByKey() \
										.mapValues(sum)
	return q.join(t).mapValues(lambda a: a[0]/a[1])

def task8(amazonInputRDD):
	q = amazonInputRDD.map(lambda a: a.split(" ")).map(lambda a: (a[1], a[2])).groupByKey().mapValues(list)
	return q.map(lambda a: (a[0], a[1])).map(lambda a: (a[0], max(a[1], key=a[1].count)))	

def task9(logsRDD):
	return logsRDD.map(lambda a: a.split(" ")).map(lambda a: (a[3][8:12], 1)).reduceByKey(lambda a, b: a + b);

def task10_flatmap(line):
	line = re.sub(r"[^(\s\w)+]", r'', line)
	return re.split(r"[\W]+", line)

def task11(playRDD):
	return playRDD.map(lambda a: (list(filter(None, a.split(" "))), a)) \
								.map(lambda a: (a[0][0], (a[1], len(a[0])))) \
								.filter(lambda a: a[1][1] > 10)

def task12(nobelRDD):
	k = nobelRDD.map(lambda a: (a['category'],a['laureates'])) \
							.flatMap(lambda a: [(a[0], i) for i in a[1]]) \
							.map(lambda a: (a[0], a[1]['surname'])) \
							.groupByKey().mapValues(list)
	return k

def task13(logsRDD, l):
	return logsRDD.map(lambda a: a.split(" ")) \
								.map(lambda a: (a[0], a[3][1:12])) \
								.groupByKey().mapValues(list) \
								.filter(lambda a: all(elem in a[1] for elem in l)) \
								.map(lambda a: a[0])

def task14(logsRDD, day1, day2):
	t = logsRDD.map(lambda v: v.split(" ")).map(lambda a: (a[3][1:12], (a[0], a[6].replace("\"", "")))).groupByKey().mapValues(list)
	
	m = t.filter(lambda a: a[0] == day1).flatMap(lambda a: (a[1])).groupByKey().mapValues(list)
	n = t.filter(lambda a: a[0] == day2).flatMap(lambda a: (a[1])).groupByKey().mapValues(list)

	return m.cogroup(n) \
					.map(lambda a: (a[0], (list(a[1][0]), list(a[1][1])))) \
					.filter(lambda a: a[1][0] and a[1][1]) \
					.map(lambda a: (a[0], (a[1][0][0], a[1][1][0])))
					
def task15(bipartiteGraphRDD):
    return bipartiteGraphRDD.groupByKey().mapValues(list).map(lambda t: (len(t[1]), 1)).reduceByKey(lambda a,b: a + b)

def task16(nobelRDD):
    return nobelRDD.flatMap(lambda a: a['laureates']) \
									 .filter(lambda a: 'motivation' in a.keys()) \
									 .map(lambda a: a['motivation']) \
									 .flatMap(lambda a: list(zip(a.split(" "), a.split(" ")[1:]))) \
									 .map(lambda a: (a, 1)) \
									 .reduceByKey(lambda a,b: a + b)
