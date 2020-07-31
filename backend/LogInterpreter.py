import re
from collections import namedtuple

DIST_RANGE_REGEX = "\d+[.\d+]?-\d+[.\d+]?"
SEQUENCE_REGEX = "\d+[.\d+]?-\d+[.\d+]?\D*"
DrillLog = namedtuple('DrillLog', 'sequences rawText')
MaterialSequence = namedtuple('MaterialSequence', 'start stop discription rawText')


#checks to see if the information is an actual well log
def isLog (info):
    """ Check if an input is a formatted well log """
    if not isinstance(info, str):
        return False
    occurances = re.findall(DIST_RANGE_REGEX, info)
    return len(occurances) > 0


def getLogData (info):
    if not isLog(info):
        return None
    
    sequences = re.findall(SEQUENCE_REGEX, info)

    materialSequences = []

    for sequence in sequences:
        depthRange = re.search(DIST_RANGE_REGEX, sequence)
        if depthRange is not None:
            start, end = depthRange.group(0).split('-')
        else:
            start, end = (-1,-1)
        discription = re.sub("[;\d,-]", "", sequence).strip()

        materialSequence = MaterialSequence(start = start, end = end, discription = discription, rawText = sequence)

        materialSequences.append(materialSequence)

    drillLog = DrillLog(sequences=materialSequences, rawText=info)   
    return drillLog     

    
    