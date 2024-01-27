# Databricks notebook source
data = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj.txt")


data.take(10)


def createUserFriendStructure(x):
    userId = x.split()[0]
    try:
        friendId = list(x.split()[1].split(','))
    except:
        friendId = []
    return (userId, friendId)


#(user, [list of friends])
userFriendsStructure = data.map(lambda x: createUserFriendStructure(x))
userFriendsStructure.count()


#create ((Id1, Id2), 0) if already friends ans ((Id1, Id2), 1) if not 
import itertools
def createFriendStructure(row):
    print('here')
    userId = row[0]
    friends = row[1]
 
    connections = []
    for friendId in friends:
        key = (userId, friendId)
        if int(userId) > int(friendId):
            key = (friendId, userId)
        connections.append((key , 0))
    
    for friendIds in itertools.combinations(friends, 2):
        key = (friendIds[0], friendIds[1])
        if int(friendIds[0]) > int(friendIds[1]):
            key = (friendIds[1], friendIds[0])
        connections.append((key, 1))
    return connections



mutualFriendStructure = userFriendsStructure.flatMap(lambda x: createFriendStructure(x))
mutualFriendStructure.count()


notFriendsWithMutualFriends = mutualFriendStructure.groupByKey().filter(lambda x: 0 not in x[1]).map(lambda x: (x[0], sum(x[1])))
notFriendsWithMutualFriends.count()


def friendsWithFriendIDAndCount(row):
    users = row[0]
    count = row[1]
    recommendation1 = (users[0], (users[1], count))
    recommendation2 = (users[1], (users[0], count))
    return [recommendation1, recommendation2]


userWiseMutualFriendList = notFriendsWithMutualFriends.flatMap(lambda x: friendsWithFriendIDAndCount(x))
userWiseMutualFriendList.take(5)


def getTop10MutualFriends(rows):
    try:
        rows = list(rows)
        rows.sort(key = lambda x: (-x[1], x[0]))
        return list(map(lambda x: x[0], rows))[:10]
    except:
        print("error")


#groupbykey and sort, select first 10
recommendations = userWiseMutualFriendList.groupByKey().map(lambda x: (x[0], getTop10MutualFriends(x[1])))


recommendations.take(10)


recommendations.collect()


formatted_recommendations = recommendations.map(lambda x: f"{x[0]}\t{', '.join(x[1])}")


formatted_recommendations.take(10)


#to store in a file
finalRecommendations = recommendations.sortByKey()


finalRecommendations.saveAsTextFile("/FileStore/tables/Recommendations.txt")
