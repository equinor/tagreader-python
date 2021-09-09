from tagreader import IMSClient
from tagreader.utils import ReaderType

tag = '1219-20LIC0013_YR'
start = '07-Sep-21 18:00:00'
end = '08-Sep-21 00:00:00'
read_type = ReaderType.RAW

ims = IMSClient("PIMAM", "piwebapi")

# Fails in original code because json result
# in web_handlers.read_tag is empty leading to empty df
# Error was when trying to convert Timestamp and it is a missing column
dta = ims.read(tag, start, end, 10, read_type=read_type)
