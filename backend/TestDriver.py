import LogInterpreter
import pandas as pd 
import os
import GetWellData
""" df = pd.read_excel("backend/data/log.xlsx", header=19)
logSeriesA = df["Lithologic log A"]
logSeriesB = df["Lithologic log B"]

for entryA, entryB in zip(logSeriesA, logSeriesB):
    aIsLog = LogInterpreter.isLog(entryA)
    bIsLog = LogInterpreter.isLog(entryB)

    logInfo = ""

    if aIsLog and bIsLog:
        logInfo = max(entryA, entryB, key=len)
    if aIsLog:
        logInfo = entryA
    elif bIsLog:
        logInfo = entryB

    #if len(logInfo) > 0:
        #print(logInfo)

 """

GetWellData.getWellData(-78, 42, -77, 43)