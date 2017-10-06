#!/usr/bin/python

#implementation of the map function
def map(rowIndex,line):

    returnDic = {} # the return pairs (k,v)
    elements = line.split(',') #we split the line into elements by ',' separator (from the csv file)

    #iterate through each element of the line (the row)
    #the key will be the index of the element, and the value will be a tuple (rowIndex, value of element)
    #I think it is possible to have custom values instead of only Int by implementing the Writeable Interface 
    for i,element in enumerate(elements):
        returnDic[i] = [rowIndex,element]

    return returnDic

    #example return for ["1,2,3,4","5,6,7,8"]: 
    #{0: [0, '1'], 1: [0, '2'], 2: [0, '3'], 3: [0, '4']}
    #{0: [1, '5'], 1: [1, '6'], 2: [1, '7'], 3: [1, '8']}	

#implementation of the reduce function
def reduce(key,values):
    #After the sort and shuffle phase, we should have something like
    #{0: [[0,'1'],[1,'5']]}
    #...
    #....
    #{3: [[1,'8'],[0,'4']]}
    #they are sorted by key (columnIndex -> rowIndex)	
    returnList = []

    #we first sort the list of tuples by their first element(rowIndex -> columnIndex)
    values = sorted(values)
    for value in values:
	#We then append second element of the tuples (the matrix cell value) to the return list
        returnList.append(value[1])

    return returnList

#In the end, we should have from
# 1 2 3 4         1 5
# 5 6 7 8    ==>  2 6
#                 3 7
#                 4 8

#test mapping function
mat = ["1,2,3,4","5,6,7,8"]

for idx,line in enumerate(mat):
    returnMap =  map(idx,line)
    print(returnMap)

