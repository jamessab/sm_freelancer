import sys
import os
sys.path.append("..")

import pickle

from Report import Report
import pstats
from dateutil.parser import parse
from datetime import timedelta
from modules.TimeModule import TimeModule
from OpeningOrder import OpeningOrder
from multiprocessing import Process
import datetime
import cProfile
import time
from enum import Enum
from modules.RootPath import RootPath
from modules.PriceData import PriceData

class OrderType(Enum):
    OPENING_BUY = 1
    OPENING_SELL = 2
    CLOSING_BUY = 3
    CLOSING_SELL = 4

class Analyzer_Thread(Process):

    def __init__(self, context, strategyKeys, simulatorQueue, limitStoplossTuples, reportFilename, lock, sellEOD, writeAudit, totalNumProcessedValue, reportsDir):
        print("+IN INIT")
        super(Analyzer_Thread, self).__init__()

        self.context = context
        self.strategyResults = {}

        self.strategyKeys = strategyKeys

        self.reportsDir = reportsDir

        # need to load the strat results nows

        self.simulatorQueue = simulatorQueue
        self.lock = lock
        self.useProfiling = False
        self.limitStoplossTuples = limitStoplossTuples
        self.sellEOD = sellEOD
        self.writeAudit = writeAudit
        self.totalNumProcessedValue = totalNumProcessedValue
        self.reportFilename = reportFilename

        self.totalSimulationsRun = 0
        self.numSimulationsProcessed = 0

        self.numIter = 0
        self.maxLen = 0
        self.avgLen = []


        print("-INIT")

    def run(self):
        print("+RUN: " + str(datetime.datetime.now()) + ", " + str(os.getpid()) + ": " + str(int(round(time.time() * 1000))) + ": THREAD STARTED.")

        self.priceData = PriceData(self.context.symbol, self.context.startDatetime, self.context.endDatetime)
        self.timeModule = TimeModule(self.context.startDatetime, self.context.endDatetime)

        pickleDir = RootPath().strategyResultsPickleDir

        strategyResults = {}
        for strategyTup in self.strategyKeys:
            print("Getting stock signals: " + strategyTup[0])
            if os.path.isfile(pickleDir + strategyTup[1]):
                print("Loading strategy pickle: " + str(pickleDir + strategyTup[1]))
                pickleFile = open(pickleDir + strategyTup[1], "rb")
                stocksToBuy = pickle.load(pickleFile)
                pickleFile.close()
                print("Loaded")
            self.strategyResults[strategyTup[0]] = stocksToBuy

        self.numSimulationsProcessed = 0

        self.totalTime = 0

        while True:
            #print("waiting for queue item")
            queueElement = self.simulatorQueue.get()
            if queueElement is None:
                # Poison pill means shutdown
                #print(str(datetime.datetime.now()) + ': {}: Exiting thread'.format(str(os.getpid())))
                break

            self.numSimulationsProcessed = self.numSimulationsProcessed + 1
            #print(str(datetime.datetime.now()) + ", " + str(os.getpid()) + ": " + " processing")
            self.totalSimulationsRun = self.totalSimulationsRun + 1

            indicatorParam = queueElement[0]
            indicatorParamStrategyKey = queueElement[1]

            #print("got queue item: " + str(indicatorParam) + " : " + str(indicatorParamStrategyKey))

            self.createCombinedBuySell(indicatorParamStrategyKey, indicatorParam)

            startSimTime = int(round(time.time() * 1000))

            if self.useProfiling and (self.totalSimulationsRun % 50) == 0:
                cProfile.runctx('self.runSimulation(indicatorParam, indicatorParamStrategyKey)', globals(), locals(), 'restats')
                p = pstats.Stats('restats')
                p.sort_stats('tottime').print_stats(10)
            else:
                self.runSimulation(indicatorParam, indicatorParamStrategyKey)

            endSimTime = int(round(time.time() * 1000))
            self.totalTime = self.totalTime + int(endSimTime - startSimTime)

            with self.lock:
                self.totalNumProcessedValue.value = self.totalNumProcessedValue.value + 1

            if (self.totalSimulationsRun % 50) == 0:
                processedPerSecond = self.numSimulationsProcessed / (self.totalTime / 1000)
                processedPerMinute = self.numSimulationsProcessed / ((self.totalTime / 1000)) * 60
                print(str(os.getpid()) + ": Processed: avg per second: " + str(processedPerSecond) + ", per minute: " + str(processedPerMinute))

        print("Num simulations processed in thread: " + str(self.numSimulationsProcessed))


    def runSimulation(self, indicatorParam, indicatorParamStrategyKey):
        #print("Running::: " + self.context.symbol + ": " + str(indicatorParam))

        self.timeModule.resetTimeModule()
        self.openOrders = {}
        self.closedOrders = []
        self.openOrderCnt = 0

        while self.timeModule.currentDatetime <= self.context.endDatetime:
            #print(str(self.timeModule.currentDatetime))
            # skip after hours and weekend trading
            if self.timeModule.currentDatetime.weekday() >= 5 or (self.timeModule.currentDatetime.hour >= self.context.endDatetime.hour and self.timeModule.currentDatetime.minute >= self.context.endDatetime.minute):
                self.closeAllPositionsEOD()
                newDateTmp = self.timeModule.currentDatetime + timedelta(days=+1)
                newDate = parse(str(newDateTmp.year) + "-" + str(newDateTmp.month) + "-" + str(newDateTmp.day) + " " + str(self.context.startDatetime.hour) + ":" + str(self.context.startDatetime.minute))
                self.timeModule.setDate(newDate)
                continue

            currPrice = self.priceData.prices[self.timeModule.currentDatetime].mid

            self.checkForOpeningOrders(currPrice)

            self.checkForClosingOrders(indicatorParamStrategyKey, indicatorParam, currPrice)

            self.timeModule.addMin()


        self.finish(indicatorParamStrategyKey, indicatorParam)



    def checkForOpeningOrders(self, currPrice):
        self.numIter = self.numIter +1
        try:
            if self.timeModule.currentDatetime in self.combinedBuys:
                # create order and add to list
                self.createOpeningOrder(OrderType.OPENING_BUY, currPrice)
            elif self.timeModule.currentDatetime in self.combinedSells:
                self.createOpeningOrder(OrderType.OPENING_SELL, currPrice)
        except Exception as e:
            print("Error: " + str(e))


    def createOpeningOrder(self, orderType, currPrice):
        for limitStoplossTuple in self.limitStoplossTuples:
            if orderType == OrderType.OPENING_BUY:
                openingOrder = OpeningOrder(self.timeModule.currentDatetime, self.context.symbol, 1, currPrice, limitStoplossTuple[0], limitStoplossTuple[1])
            elif orderType == OrderType.OPENING_SELL:
                openingOrder = OpeningOrder(self.timeModule.currentDatetime, self.context.symbol, -1, currPrice, limitStoplossTuple[1], limitStoplossTuple[0])

            self.openOrderCnt = self.openOrderCnt + 1
            self.openOrders[self.openOrderCnt] = openingOrder


    def checkForClosingOrders(self, currLimit, currStoploss, currPrice):
        lowPrice = self.priceData.prices[self.timeModule.currentDatetime].low
        highPrice = self.priceData.prices[self.timeModule.currentDatetime].high

        for openingOrderKey in list(self.openOrders.keys()):
            if lowPrice < self.openOrders[openingOrderKey].stoplossPrice:
                # Loss
                percPL = self.openOrders[openingOrderKey].stoplossPerc - 1

                closingOrderDict = { 'openingOrder': self.openOrders[openingOrderKey], 'date': self.timeModule.currentDatetime, 'price': lowPrice, 'percPL': percPL}
                self.closedOrders.append(closingOrderDict)
                del self.openOrders[openingOrderKey]
            elif highPrice > self.openOrders[openingOrderKey].limitPrice:
                # Profit
                percPL = self.openOrders[openingOrderKey].limitPerc - 1
                closingOrderDict = { 'openingOrder': self.openOrders[openingOrderKey], 'date': self.timeModule.currentDatetime, 'price': highPrice, 'percPL': percPL }

                self.closedOrders.append(closingOrderDict)
                del self.openOrders[openingOrderKey]


    def closeAllPositionsEOD(self):
        currPrice = self.priceData.prices[self.timeModule.currentDatetime].mid

        for openingOrderKey in list(self.openOrders.keys()):
            if self.openOrders[openingOrderKey].quantity == -1:
                # Short
                percPL = round((self.openOrders[openingOrderKey].price - currPrice) / currPrice, 4)
                closingOrderDict = { 'openingOrder': self.openOrders[openingOrderKey], 'date': self.timeModule.currentDatetime, 'price': currPrice, 'percPL': percPL }

                self.closedOrders.append(closingOrderDict)
                del self.openOrders[openingOrderKey]
            else:
                # Long
                percPL = round((currPrice - self.openOrders[openingOrderKey].price) / self.openOrders[openingOrderKey].price, 4)
                closingOrderDict = { 'openingOrder': self.openOrders[openingOrderKey], 'date': self.timeModule.currentDatetime, 'price': currPrice, 'percPL': percPL }
                self.closedOrders.append(closingOrderDict)
                del self.openOrders[openingOrderKey]

    def finish(self, indicatorParamStrategyKey, indicatorParams):
        Report(self.context, self.reportsDir).generateEndOfSimulationReportData(self.closedOrders, self.writeAudit, indicatorParamStrategyKey, indicatorParams)


    def createCombinedBuySell(self, indicatorParamStrategyKey, indicatorParam):
        # Intersects all of the indicator buy signal times. This way we only buy/sell if all of the specified indicators say so

        self.combinedBuys = set()
        self.combinedSells = set()
        cnt = 0
        for strategyKey in indicatorParamStrategyKey:
            try:
                if cnt == 0:
                    self.combinedBuys = set(self.strategyResults[strategyKey][indicatorParam[cnt]]['buys'])
                else:
                    self.combinedBuys = self.combinedBuys.intersection( set(self.strategyResults[strategyKey][indicatorParam[cnt]]['buys']))

            except Exception as e:
                print("Exception2: " + str(e))

            cnt = cnt + 1
