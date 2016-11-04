import csv, threading, queue, os, time

posToString = {0: 'year', 1: 'quarter', 2: 'month', 3: 'dayofmonth', 4: 'dayofweek', 5: 'flightdate', 6: 'uniquecarrier', 7: 'airlineid', 8: 'carrier', 9: 'tailnum', 10: 'flightnum', 11: 'originairportid', 12: 'originairportseqid', 13: 'origincitymarketid', 14: 'origin', 15: 'origincityname', 16: 'originstate', 17: 'originstatefips', 18: 'originstatename', 19: 'originwac', 20: 'destairportid', 21: 'destairportseqid', 22: 'destcitymarketid', 23: 'dest', 24: 'destcityname', 25: 'deststate', 26: 'deststatefips', 27: 'deststatename', 28: 'destwac', 29: 'crsdeptime', 30: 'deptime', 31: 'depdelay', 32: 'depdelayminutes', 33: 'depdel15', 34: 'departuredelaygroups', 35: 'deptimeblk', 36: 'taxiout', 37: 'wheelsoff', 38: 'wheelson', 39: 'taxiin', 40: 'crsarrtime', 41: 'arrtime', 42: 'arrdelay', 43: 'arrdelayminutes', 44: 'arrdel15', 45: 'arrivaldelaygroups', 46: 'arrtimeblk', 47: 'cancelled', 48: 'cancellationcode', 49: 'diverted', 50: 'crselapsedtime', 51: 'actualelapsedtime', 52: 'airtime', 53: 'flights', 54: 'distance', 55: 'distancegroup', 56: 'carrierdelay', 57: 'weatherdelay', 58: 'nasdelay', 59: 'securitydelay', 60: 'lateaircraftdelay', 61: 'firstdeptime', 62: 'totaladdgtime', 63: 'longestaddgtime', 64: 'divairportlandings', 65: 'divreacheddest', 66: 'divactualelapsedtime', 67: 'divarrdelay', 68: 'divdistance', 69: 'div1airport', 70: 'div1airportid', 71: 'div1airportseqid', 72: 'div1wheelson', 73: 'div1totalgtime', 74: 'div1longestgtime', 75: 'div1wheelsoff', 76: 'div1tailnum', 77: 'div2airport', 78: 'div2airportid', 79: 'div2airportseqid', 80: 'div2wheelson', 81: 'div2totalgtime', 82: 'div2longestgtime', 83: 'div2wheelsoff', 84: 'div2tailnum', 85: 'div3airport', 86: 'div3airportid', 87: 'div3airportseqid', 88: 'div3wheelson', 89: 'div3totalgtime', 90: 'div3longestgtime', 91: 'div3wheelsoff', 92: 'div3tailnum', 93: 'div4airport', 94: 'div4airportid', 95: 'div4airportseqid', 96: 'div4wheelson', 97: 'div4totalgtime', 98: 'div4longestgtime', 99: 'div4wheelsoff', 100: 'div4tailnum', 101: 'div5airport', 102: 'div5airportid', 103: 'div5airportseqid', 104: 'div5wheelson', 105: 'div5totalgtime', 106: 'div5longestgtime', 107: 'div5wheelsoff', 108: 'div5tailnum', 109: 'filler', 110: 'serialid'}
stringToPos = {'year': 0, 'quarter': 1, 'month': 2, 'dayofmonth': 3, 'dayofweek': 4, 'flightdate': 5, 'uniquecarrier': 6, 'airlineid': 7, 'carrier': 8, 'tailnum': 9, 'flightnum': 10, 'originairportid': 11, 'originairportseqid': 12, 'origincitymarketid': 13, 'origin': 14, 'origincityname': 15, 'originstate': 16, 'originstatefips': 17, 'originstatename': 18, 'originwac': 19, 'destairportid': 20, 'destairportseqid': 21, 'destcitymarketid': 22, 'dest': 23, 'destcityname': 24, 'deststate': 25, 'deststatefips': 26, 'deststatename': 27, 'destwac': 28, 'crsdeptime': 29, 'deptime': 30, 'depdelay': 31, 'depdelayminutes': 32, 'depdel15': 33, 'departuredelaygroups': 34, 'deptimeblk': 35, 'taxiout': 36, 'wheelsoff': 37, 'wheelson': 38, 'taxiin': 39, 'crsarrtime': 40, 'arrtime': 41, 'arrdelay': 42, 'arrdelayminutes': 43, 'arrdel15': 44, 'arrivaldelaygroups': 45, 'arrtimeblk': 46, 'cancelled': 47, 'cancellationcode': 48, 'diverted': 49, 'crselapsedtime': 50, 'actualelapsedtime': 51, 'airtime': 52, 'flights': 53, 'distance': 54, 'distancegroup': 55, 'carrierdelay': 56, 'weatherdelay': 57, 'nasdelay': 58, 'securitydelay': 59, 'lateaircraftdelay': 60, 'firstdeptime': 61, 'totaladdgtime': 62, 'longestaddgtime': 63, 'divairportlandings': 64, 'divreacheddest': 65, 'divactualelapsedtime': 66, 'divarrdelay': 67, 'divdistance': 68, 'div1airport': 69, 'div1airportid': 70, 'div1airportseqid': 71, 'div1wheelson': 72, 'div1totalgtime': 73, 'div1longestgtime': 74, 'div1wheelsoff': 75, 'div1tailnum': 76, 'div2airport': 77, 'div2airportid': 78, 'div2airportseqid': 79, 'div2wheelson': 80, 'div2totalgtime': 81, 'div2longestgtime': 82, 'div2wheelsoff': 83, 'div2tailnum': 84, 'div3airport': 85, 'div3airportid': 86, 'div3airportseqid': 87, 'div3wheelson': 88, 'div3totalgtime': 89, 'div3longestgtime': 90, 'div3wheelsoff': 91, 'div3tailnum': 92, 'div4airport': 93, 'div4airportid': 94, 'div4airportseqid': 95, 'div4wheelson': 96, 'div4totalgtime': 97, 'div4longestgtime': 98, 'div4wheelsoff': 99, 'div4tailnum': 100, 'div5airport': 101, 'div5airportid': 102, 'div5airportseqid': 103, 'div5wheelson': 104, 'div5totalgtime': 105, 'div5longestgtime': 106, 'div5wheelsoff': 107, 'div5tailnum': 108, 'filler': 109, 'serialid': 110}

mapFrom = 'originairportid'
mapTo = 'arrdel15'

mapperExitFlag = 0
reducerExitFlag = 0
activeFlag = 0

mapLock = threading.Lock()
reduceLock = threading.Lock()
finalReduceLock = threading.Lock()

mappingTasks = queue.Queue()
reducingTasks = queue.Queue()
finalReduceTasks = queue.Queue()


class Mapper(threading.Thread):

    def __init__(self, thID, name, q):
        threading.Thread.__init__(self)
        self.thID = thID
        self.name = name
        self.q = q

    def __str__(self):
        return 'Mapper'

    def run(self):
        map_values(self.name, self.q)


class Reducer(threading.Thread):

    def __init__(self, thID, name, q):
        threading.Thread.__init__(self)
        self.thID = thID
        self.name = name
        self.q = q

    def __str__(self):
        return 'Reducer'

    def run(self):
        reduce(self.name, self.q)


def map_values(threadName, q):
    """
    Mapper function
    Get file from queue, read the csv dataset and map one value to another.
    Create a list containing (key, value) pairs as tuples and put them in the reducer work queue

    :param threadName: Name of the thread running the function
    :param q: Work queue to acquire work from
    :return: List of key value pairs as tuples
    """
    while not mapperExitFlag:
        if not q.empty():
            mapLock.acquire()
            file = q.get()
            mapLock.release()

            result = []

            with open(file, 'r') as csv_file:
                csvreader = csv.reader(csv_file, delimiter=',', quotechar='"')
                for line in csvreader:
                    key = line[stringToPos[mapFrom]]

                    try:
                        # value = int(float(line[stringToPos[mapTo]]))
                        tup = (key, 1)
                        result.append(tup)
                    except ValueError:
                        pass  # print('Could not convert "%s"' % line[stringToPos[mapTo]])

                reduceLock.acquire()
                reducingTasks.put(result)
                reduceLock.release()


def reduce(threadName, q):
    """
    Reducer function
    Get list of key value pairs from the work queue and reduce to unique keys.
    Write the result to a file

    :param threadName: Name of the thread running this function
    :param q: Work queue to acquire work from
    :return:
    """
    it = 1
    while not reducerExitFlag:
        if not q.empty():
            reduceLock.acquire()
            map_list = q.get()
            reduceLock.release()

            kv_pair = {}
            reduced_list = []
            for key, value in map_list:
                if key in kv_pair:
                    kv_pair[key] = kv_pair[key] + value
                else:
                    kv_pair[key] = value

            output = os.path.join('output', '%s.output.%i.txt' % (threadName, it))
            with open(output, 'w+') as outfile:
                for entry in kv_pair:
                    reduced_list.append((entry, kv_pair[entry]))
                    outstring = "%s: %i\n" % (entry, kv_pair[entry])
                    outfile.write(outstring)

            it += 1

            finalReduceLock.acquire()
            finalReduceTasks.put(reduced_list)
            finalReduceLock.release()


def task_handler():
    """
    Main function. Put all csv files in the work queue then create and initiate threads
    When all work is done, signal threds to exit and then wait for them to complete their tasks

    :return:
    """
    global mapperExitFlag, reducerExitFlag, activeFlag

    mapperExitFlag = 0
    reducerExitFlag = 0
    fileCount = 1

    mappers = []
    reducers = []
    for i in range(0, 5):
        mappers.append((i, 'Mapper-%i' % i))
    for i in range(0, 3):
        reducers.append((i, 'Reducer-%i' % i))

    mapper_pool = []
    reducer_pool = []
    print('Creating threads')
    for thid, name in mappers:
        mapper_pool.append(Mapper(thid, name, mappingTasks))

    for thid, name in reducers:
        reducer_pool.append(Reducer(thid, name, reducingTasks))

    mapLock.acquire()
    print('Putting work in queue:')
    for file in os.listdir('dataset'):
        path = os.path.join('dataset', file)
        if os.path.isfile(path):  # and fileCount < 5:
            print("\tAdding %s to queue" % path)
            mappingTasks.put(path)
            fileCount += 1

    mapLock.release()

    print("Initiating mappers\n")
    for thread in mapper_pool:
        thread.start()

    # Wait for something to appear in the reducer work queue
    while reducingTasks.empty():
        pass

    print("Waiting for mapping queue to empty...\n")
    while not mappingTasks.empty():
        pass

    print('Signal exit flag to mappers')
    mapperExitFlag = 1

    # Wait for mappers to finish
    for thread in mapper_pool:
        thread.join()

    print("Initiating reducers\n")
    for thread in reducer_pool:
        thread.start()

    while not reducingTasks.empty():
        pass

    print('Signal exit flag to reducers\n')
    reducerExitFlag = 1

    for thread in reducer_pool:
        thread.join()

    final_reduce()

    print('Work done, exiting.')


def final_reduce():
    """
    Reduce output from Reducers to one single output when all other tasks are done
    :return:
    """
    finalReduceLock.acquire()

    final_result = {}

    while not finalReduceTasks.empty():
        map_list = finalReduceTasks.get()

        for key, value in map_list:
            if key in final_result:
                final_result[key] = final_result[key] + value
            else:
                final_result[key] = value

    finalReduceLock.release()

    output = os.path.join('output', 'Reducer-final.output.txt')
    with open(output, 'w+') as outfile:
        for entry in final_result:
            outstring = "%s: %i\n" % (entry, final_result[entry])
            outfile.write(outstring)


def main():
    task_handler()
    print("Elapsed time: %1.2fs" % time.process_time())

if __name__ == "__main__":
    main()
