import enum
"""
Enumerates available types of data to read.

For members with more than one name per value, the first member (the original) needs to be untouched since it may be 
used as back-reference (specifically for cache hierarchies). 
"""
class ReaderType(enum.IntEnum):
    RAW = SAMPLED = enum.auto()                     # Raw sampled data
    SHAPEPRESERVING = enum.auto()                   # Minimum data points while preserving shape
    INT = INTERPOLATE = INTERPOLATED = enum.auto()  # Interpolated data
    MIN = MINIMUM = enum.auto()                     # Min value
    MAX = MAXIMUM = enum.auto()                     # Max value
    AVG = AVERAGE = AVERAGED = enum.auto()          # Averaged data
    VAR = VARIANCE = enum.auto()                    # Variance of data
    STD = STDDEV = enum.auto()                      # Standard deviation of data
    RNG = RANGE = enum.auto()                       # Range of data
    COUNT = enum.auto()                             # Number of data points
    GOOD = enum.auto()                              # Number of good data points
    BAD = NOTGOOD = enum.auto()                     # Number of not good data points
    TOTAL = enum.auto()                             # Number of total data
    SUM = enum.auto()                               # Sum of data
    SNAPSHOT = FINAL = LAST = enum.auto()           # Last sampled value