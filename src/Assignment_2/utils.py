import pyspark
from pyspark import SparkContext, SparkConf
def create_SparkConf():
    conf=SparkConf().setAppName("Assignment_2").setMaster("local[*]")
    sc=SparkContext(conf=conf)
    return sc
import itertools
def myParse(line):
    line = line.replace(' -- ', ', ')
    line = line.replace('.rb: ', ', ')
    line = line.replace(', ghtorrent-', ', ')
    return line.split(', ', 4)
def getRDD(sc):
    textFile = sc.textFile("C:/Users/Welcome/PycharmProjects/Spark_Repo/Spark_Repo/resourses/ghtorrent-logs.txt")
    parsedRDD = textFile.map(myParse)
    return parsedRDD
def parseRepos(x):
    try:
        split = x[4].split('/')[4:6]
        joinedSplit = '/'.join(split)
        result = joinedSplit.split('?')[0]
    except:
        result = ''
    x.append(result)
    return x
def count_rowrdd(rowrdd):
    return rowrdd.count()
def no_warn_rdd(rowrdd):
    numWarns = rowrdd.filter(lambda x: x[0] == "WARN")
    return numWarns.count()
def api_clint(rowrdd):
    # Filters out rows without enough elements (about 50 rows)
    filteredRdd = rowrdd.filter(lambda x: len(x) == 5)

    # Only look at api_client calls
    apiRdd = filteredRdd.filter(lambda x: x[3] == "api_client")

    # Add another column with the repo if can find one, otherwise ''
    reposRdd = apiRdd.map(parseRepos)
    # Filter out rows without repo
    removedEmpty = reposRdd.filter(lambda x: x[5] != '')
    # Group by repo and count
    uniqueRepos = removedEmpty.groupBy(lambda x: x[5])
    return uniqueRepos.count()

def HTTP(rowrdd):
    # Filters out rows without enough elements (about 50 rows)
    filteredRdd = rowrdd.filter(lambda x: len(x) == 5)

    # Only look at api_client calls
    apiRdd = filteredRdd.filter(lambda x: x[3] == "api_client")
    # Group by, count and find max
    usersHttp = apiRdd.groupBy(lambda x: x[2])
    usersHttpSum = usersHttp.map(lambda x: (x[0], x[1].__len__()))
    count_failed_http=(usersHttpSum.max(key=lambda x: x[1]))
    return count_failed_http
def failed_HTTP(rowrdd):
    # Filters out rows without enough elements (about 50 rows)
    filteredRdd = rowrdd.filter(lambda x: len(x) == 5)

    # Only look at api_client calls
    apiRdd = filteredRdd.filter(lambda x: x[3] == "api_client")

    # filter failed http requests
    onlyFailed = apiRdd.filter(lambda x: x[4].split(' ', 1)[0] == "Failed")
    # Group by, count, find max
    usersFailedHttp = onlyFailed.groupBy(lambda x: x[2])
    usersFailedHttpSum = usersFailedHttp.map(lambda x: (x[0], x[1].__len__()))
    max_HTTP=(usersFailedHttpSum.max(key=lambda x: x[1]))
    return max_HTTP

# Get hour of the day from timestamp and add it
def appendAndReturn(x, toAdd):
    x.append(toAdd)
    return x
def hours_count(rowrdd):
    # Filters out rows without enough elements (about 50 rows)
    filteredRdd = rowrdd.filter(lambda x: len(x) == 5)
    # Split date to hour only
    onlyHours = filteredRdd.map(lambda x: appendAndReturn(x, x[1].split('T', 1)[1].split(':', 1)[0]))
    # Group by, count, find max
    groupOnlyHours = onlyHours.groupBy(lambda x: x[5])
    hoursCount = groupOnlyHours.map(lambda x: (x[0], x[1].__len__()))
    no_hours=(hoursCount.max(key=lambda x: x[1]))
    return no_hours
def active_Repo(rowrdd):
    # Filters out rows without enough elements (about 50 rows)
    filteredRdd = rowrdd.filter(lambda x: len(x) == 5)

    # Only look at api_client calls
    apiRdd = filteredRdd.filter(lambda x: x[3] == "api_client")

    # Add another column with the repo if can find one, otherwise ''
    reposRdd = apiRdd.map(parseRepos)
    # Filter out rows without repo
    removedEmpty = reposRdd.filter(lambda x: x[5] != '')
    # Group by repo and count
    # Group by, count, find max
    activityRepos = removedEmpty.groupBy(lambda x: x[5])
    countActivityRepos = activityRepos.map(lambda x: (x[0], x[1].__len__()))
    count_active_repo=(countActivityRepos.max(key=lambda x: x[1]))
    return count_active_repo

