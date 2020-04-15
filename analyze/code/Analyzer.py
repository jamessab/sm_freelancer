import os
import sys
sys.path.append("../..")
import pickle
import time
import talib as ta
import json
import datetime
import gc
import itertools
from modules.RootPath import RootPath
from strategies.Strategy_MACD import Strategy_MACD
from strategies.Strategy_DIP_BUY_OPENING_FASTER import Strategy_DIP_BUY_OPENING_FASTER
from strategies.Strategy_DIP_BUY_OPENING import Strategy_DIP_BUY_OPENING
from strategies.Strategy_MACD_2 import Strategy_MACD_2
import multiprocessing
from dateutil.parser import parse
from modules.Context import Context
from modules.PriceData import PriceData
from analyze.code.Analyzer_Thread_2 import Analyzer_Thread_2

class Analyzer:

    def __init__(self, symbol):

        startDatetime = parse('3-1-2019 7:30')
        endDatetime = parse('4-14-2019 13:45')

        self.context = Context(symbol, startDatetime, endDatetime)
        self.priceData = PriceData(symbol, startDatetime, endDatetime)

        self.sellEOD = True
        self.writeAudit = True

        self.numThreads = 1

        self.reportsDir = RootPath().rootPath + "/codebase/reports/2019_single_ind"

        self.limitPercStart = 1.02
        self.limitPercEnd = 1.06
        self.stoplossPercStart = 0.98
        self.stoplossPercEnd = 0.94
        self.pricePercChange = 0.01

        #####
        self.simulatorQueue = multiprocessing.Queue()

        self.lock = multiprocessing.RLock()


        self.strategies = []

        self.strategies.append(Strategy_DIP_BUY_OPENING_FASTER(self.context, self.priceData, 0, True, False, 1, 1, 5,  21, 21, 21, 1, 1, 5, 21, 21, 21))


    def getStockSignals(self, strategies):
        pickleDir = RootPath().strategyResultsPickleDir

        strategyResults = {}
        for strategy in strategies:

            print("Getting stock signals: " + strategy.key)

            a = int(round(time.time() * 1000))

            key = strategy.getFileKey()
            if os.path.isfile(pickleDir + key):
                print("Loading strategy pickle: " + str(pickleDir + key))
                pickleFile = open(pickleDir + key, "rb")
                stocksToBuy = pickle.load(pickleFile)
                pickleFile.close()
                print("Loaded")
            else:
                print("Strategy results pickle not found. Creating...")
                stocksToBuy = strategy.getStockSignals()

                pickleFile = open(pickleDir + key, "wb")
                pickle.dump(stocksToBuy, pickleFile, protocol=pickle.HIGHEST_PROTOCOL)
                pickleFile.flush()
                pickleFile.close()

            a1 = int(round(time.time() * 1000))
            print("Done getting stock signals for: " + str(strategy.key) + ", time: " + str(a1 - a))

            strategyResults[strategy.key] = stocksToBuy

        return strategyResults

    def startAnalyzer(self):

        strategyCombinations = []

        #for i in range(1, len(self.strategies) + 1):
        #for i in range(2, 3): # one combo
        for i in range(1, 2):
            combinations = list((list(tup) for tup in list(itertools.combinations(self.strategies, i))))

            for combo in combinations:
                strategyCombinations.append(combo)

        self.strategyResults = self.getStockSignals(self.strategies)

        print("Deleting server_strategies")

        startDateStr = f"{self.context.startDatetime.month:02d}-{self.context.startDatetime.day:02d}-{self.context.startDatetime.year} {self.context.startDatetime.hour:02d}-{self.context.startDatetime.minute}"
        endDateStr = f"{self.context.endDatetime.month:02d}-{self.context.endDatetime.day:02d}-{self.context.endDatetime.year} {self.context.endDatetime.hour:02d}-{self.context.endDatetime.minute}"

        # Set the report name
        self.name = ""
        for strat in self.strategies:
            self.name = self.name + str(strat.key) + "_"
        self.reportFilename = self.reportsDir + "/summary_" + self.name + startDateStr + "_" + endDateStr + ".csv"
        #del self.strategies

        self.totalToProcess = 0

        #
        # generate limit / stoploss tuples for buys
        #
        currStoploss = self.stoplossPercStart
        currLimit = self.limitPercStart
        self.limitStoplossTuples = []
        while currLimit <= self.limitPercEnd:
            while currStoploss >= self.stoplossPercEnd:
                currLimit = round(currLimit, 2)
                currStoploss= round(currStoploss, 2)
                self.limitStoplossTuples.append( ( currLimit, currStoploss) )
                currStoploss = currStoploss - self.pricePercChange
            currStoploss = self.stoplossPercStart
            currLimit = currLimit + self.pricePercChange

        self.createConsumers()

        # The list of server_strategies, such as the combination of EMA and RSI
        processingStartMillis = int(round(time.time() * 1000))
        for strategyList in strategyCombinations:

            indicatorParamStrategyKey = []
            indicatorParams = []

            for strategy in strategyList:
                if len(self.strategyResults[strategy.key].keys()) == 0:
                    print("Empty key set. Means the strategy didnt run correctly. Fatal error. Exiting.")
                    exit(0)
                indicatorParams.append(list(self.strategyResults[strategy.key].keys()))
                indicatorParamStrategyKey.append(strategy.key)
            self.allIndicatorParamsCombinations = list((list(tup) for tup in list(itertools.product(*indicatorParams))))

            print("Number queue items to process: " + str(len(self.allIndicatorParamsCombinations)))

            for indicatorParam in self.allIndicatorParamsCombinations:
                self.simulatorQueue.put((indicatorParam, indicatorParamStrategyKey))


        # Poison pill
        for i in range(self.numThreads):
            print("Adding posion pill")
            self.simulatorQueue.put(None)

        # Done processing here
        try:
            for c in self.consumers:
                print("Waiting")
                c.join()
                print("Done waiting")
        except Exception as e:
            print("Skipping join on dead thread")


    def createConsumers(self):
        print("Spawning consumers: " + str(self.numThreads))
        self.consumers = []

        for i in range(self.numThreads):
            print("SPAWNING CONSUMER")
            spawnStart = datetime.datetime.now()

            #p = Analyzer_Thread(self.context, self.strategyResults, self.priceData, self.simulatorQueue, self.limitStoplossTuples, self.reportFilename, lock, self.sellEOD, self.writeAudit, self.reportsDir)

            strategyKeys = []
            for strat in self.strategies:
                strategyKeys.append( (strat.key, strat.getFileKey()) )
            p = Analyzer_Thread_2(self.context, strategyKeys, self.simulatorQueue, self.limitStoplossTuples, self.reportFilename, self.lock, self.sellEOD, self.writeAudit, self.reportsDir)

            spawnEnd = datetime.datetime.now()
            print("Spawn time: " + str(datetime.timedelta(seconds=(spawnEnd - spawnStart).total_seconds())))

            self.consumers.append(p)

        for w in self.consumers:
            spawnStart = datetime.datetime.now()
            w.start()
            spawnEnd = datetime.datetime.now()
            print("Start time: " + str(datetime.timedelta(seconds=(spawnEnd - spawnStart).total_seconds())))


if __name__ == "__main__":
    excludeList = []
    symbolList = []
    stockFile = RootPath().rootPath + "/codebase/simulator/data/stock_lists/high_volume_top_100.txt"

    with open(stockFile) as csvFileInput:
        allLines = csvFileInput.readlines()

        for stock in allLines:
            try:

                if stock.startswith("'"):
                    break

                if stock.startswith("#"):
                    continue

                stock = str(stock).rstrip()
                symbolList.append(stock)
            except Exception as e:
                print("Error: " + str(e))
                exit(1)

    cnt = 0
    for symbol in symbolList:
        if symbol in excludeList:
            print("Skipping excluded symbol: " + str(symbol))
            continue

        cnt = cnt + 1
        analyzerStartDatetime = datetime.datetime.now()

        print("Processing: " + str(cnt) + " / " + str(len(symbolList)))
        analyzer = Analyzer(symbol)
        analyzer.startAnalyzer()

        analyzerEndDatetime = datetime.datetime.now()
        endTime = int(round(time.time() * 1000))

        print("Simulation runtime in seconds: " + str(datetime.timedelta(seconds=(analyzerEndDatetime - analyzerStartDatetime).total_seconds())))

        del analyzer

