import sys
import pandas as pd
from shapely.geometry import Polygon
import geopandas as gpd
import LogInterpreter
import time

REMARKS = "C185"
MAT_DISCR = "C097"
LAT = "C909"
LNG = "C910"

def insideBounds (lat, lng, minLong, minLat, maxLong, maxLat):
    if lat > minLat and lat < maxLat and lng > minLong and lng < maxLong:
        return True
    return False

def getWellData (minLong, minLat, maxLong, maxLat):
    startTime = time.time()
    print(f'{minLong}, {minLat}, {maxLong}, {maxLat}')

    bbox = Polygon([(minLong, minLat), (maxLong, minLat), (maxLong, maxLat), (minLong, maxLat)])

    dtypes = {}
    df = pd.read_table("backend/data/well_logs.subf", delimiter="\t", header=21, encoding = "ISO-8859-1", skiprows=[22])
    tTime0 = time.time() - startTime;
    #inBoundsDf = df[df.apply(lambda x: insideBounds(x[LAT], x[LNG], minLong, minLat, maxLong, maxLat), axis=1)]

    tTime1 = time.time() - startTime;
    gpdDF = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[LNG], df[LAT]))
    tTime2 = time.time() - startTime;
    mask = gpdDF.within(bbox)
    tTime3 = time.time() - startTime;
    print (mask)

    maskedDF = gpdDF[mask]
    tTime4 = time.time() - startTime;

    print (maskedDF)
    

    logSeriesA = df[REMARKS]
    logSeriesB = df[MAT_DISCR]

    #for index, row in df.iterrows():

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

        

if __name__ == "__main__":
    xmin, ymin, xmax, ymax = sys.argv[1:5]
    getWellData(xmin, ymin, xmax, ymax)

