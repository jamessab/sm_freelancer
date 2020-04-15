'''

The MACD indicator cache CURRENTLY only goes from fastPeriod=7, slowPeriod=12, signalPeriod=2 ... to fastPeriod=7, slowPeriod=12, signalPeriod=2 in increments of 5

'''

import sys
sys.path.append("..")

import talib as ta
import numpy as np
from modules.TimeModule import TimeModule

class Strategy_MACD_2():

    key = "MACD"

    def __init__(self, context, priceData, macdLengthIncrease, macdStartLength, macdEndLength, macdLimitLineDecrease, macdLimitLineStart, macdLimitLineEnd, macdDaysBackIncrease, macdDaysBackStart, macdDaysBackEnd):

        self.context = context
        self.priceData = priceData
        self.macdLengthIncrease = macdLengthIncrease
        self.macdStartLength = macdStartLength
        self.macdEndLength = macdEndLength
        self.macdDaysBackIncrease = macdDaysBackIncrease
        self.macdDaysBackStart = macdDaysBackStart
        self.macdDaysBackEnd = macdDaysBackEnd
        self.macdLimitLineStart = macdLimitLineStart
        self.macdLimitLineEnd = macdLimitLineEnd
        self.macdLimitLineDecrease = macdLimitLineDecrease

        self.keyPrefix = context.symbol + "_strategy_results_" + self.context.startDatetime.strftime("%m-%d-%Y") + "_" + self.context.endDatetime.strftime("%m-%d-%Y")


    def getFileKey(self):
        key = self.keyPrefix + "_" + self.key + "_" + str(self.macdLengthIncrease) + "_" + str(self.macdStartLength) + "_" + str(self.macdEndLength) + "_" + str(self.macdDaysBackIncrease) + "_" + str(self.macdDaysBackStart) + "_" +  str(self.macdDaysBackEnd) + "_" + str(self.macdLimitLineStart) + "_" + str(self.macdLimitLineEnd) + "_" + str(self.macdLimitLineDecrease) + ".pickle"
        return key

    def getStockSignals(self):

        macdSignalPeriod = self.macdStartLength
        macdFastPeriod = macdSignalPeriod + self.macdLengthIncrease
        macdSlowPeriod = macdFastPeriod + self.macdLengthIncrease
        macdLimitLine = self.macdLimitLineStart
        daysBack = self.macdDaysBackStart

        returnStrategyResults = {}

        self.timeModule = TimeModule(self.context.startDatetime, self.context.endDatetime)
        self.timeModule.createDeltaCache()

        numLoopsRuns = 0
        numBuys = 0
        numMinsRun = 0

        while self.timeModule.currentDatetime <= self.context.endDatetime:

            if (self.timeModule.currentDatetime.time() < self.context.tradingHoursStart.time() and self.timeModule.currentDatetime.time() > self.context.tradingHoursEnd.time()) or self.timeModule.currentDatetime.weekday() >= 5:
                try:
                    self.timeModule.addMin()
                    continue
                except Exception as e:
                    numMinsRun = numMinsRun + 1
                    self.timeModule.addMin()
                    continue

            if self.timeModule.currentDatetime.hour == 7 and self.timeModule.currentDatetime.minute == 30:
                print(str(self.timeModule.currentDatetime) + ", numMinsRun: " + str(numMinsRun))

            hist = self.priceData.createHistory(self.timeModule.currentDatetime, macdSlowPeriod * 5)['close']

            while macdLimitLine >= self.macdLimitLineEnd:
                while macdSlowPeriod <= self.macdEndLength:
                    while macdFastPeriod <= self.macdEndLength and macdFastPeriod < macdSlowPeriod:
                        while macdSignalPeriod <= self.macdEndLength and macdSignalPeriod < macdFastPeriod:
                            #while daysBack <= self.macdDaysBackEnd:

                            numLoopsRuns = numLoopsRuns + 1

                            indicatorParamKey = str(macdFastPeriod) + "~" + str(macdSlowPeriod) + "~" + str(macdSignalPeriod) + "~" + str(macdLimitLine) + "~" + str(daysBack)
                            if indicatorParamKey not in returnStrategyResults:
                                returnStrategyResults[indicatorParamKey] = {}
                                returnStrategyResults[indicatorParamKey]['buys'] = set()
                                returnStrategyResults[indicatorParamKey]['sells'] = set()

                            if self.createStrategyResult(hist, macdFastPeriod, macdSlowPeriod, macdSignalPeriod, daysBack, macdLimitLine):
                                returnStrategyResults[indicatorParamKey]['buys'].add(self.timeModule.currentDatetime)

                                numBuys = numBuys + 1

                            macdSignalPeriod = macdSignalPeriod + self.macdLengthIncrease

                        macdSignalPeriod = self.macdStartLength
                        macdFastPeriod = macdFastPeriod + self.macdLengthIncrease

                    macdSignalPeriod = self.macdStartLength
                    macdFastPeriod = self.macdStartLength
                    macdSlowPeriod = macdSlowPeriod +self.macdLengthIncrease

                macdSignalPeriod = self.macdStartLength
                macdFastPeriod = self.macdStartLength
                macdSlowPeriod = macdFastPeriod + self.macdLengthIncrease
                macdLimitLine = macdLimitLine - self.macdLimitLineDecrease

            macdSignalPeriod = self.macdStartLength
            macdFastPeriod = macdSignalPeriod + self.macdLengthIncrease
            macdSlowPeriod = macdFastPeriod + self.macdLengthIncrease
            macdLimitLine = self.macdLimitLineStart

            numMinsRun = numMinsRun + 1
            self.timeModule.addMin()


        print("Loops run: " + str(numLoopsRuns))
        print("Num Buys: " + str(numBuys))
        print("Num minutes runs: " + str(numMinsRun))

        return returnStrategyResults

    def createStrategyResult(self, hist, macdFastPeriod, macdSlowPeriod, macdSignalPeriod, daysBack, macdLimitLine):
        try:
            indicatorRes, signalRes, histRes = ta.MACD(hist, macdFastPeriod, macdSlowPeriod, macdSignalPeriod)
            indicator = indicatorRes[-1]
            signal = signalRes[-1]

            if not np.isnan(indicator) and not np.isnan(signal) and signal < indicator and indicator <= macdLimitLine:
                return True
        except Exception as e:
            print("Excpe: " + str(e))
            pass

        return False
