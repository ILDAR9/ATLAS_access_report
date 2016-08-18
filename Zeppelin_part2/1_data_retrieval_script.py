%pyspark

# preload for the next snippet
from urllib2 import urlopen
import urllib2
import re
import pandas as pd
import datetime

def findall(reg_exp, url_storage):
    data_storage_url = urlopen(url_storage)
    content = data_storage_url.read().decode('utf-8')
    pattern = re.compile(reg_exp)
    list_ =  pattern.findall(content)
    return list_
    
def load_data_to_sql(frame):
    sqlCtx = SQLContext(sc)
    data_volume = sqlCtx.createDataFrame(frame)
    sqlContext.registerDataFrameAsTable(data_volume, "report")

def prepare_plot(csv_store_url):
    print "csv storage url " + csv_store_url
    
    csv_file = findall(">(full_monthly.csv)", csv_store_url)
    if not csv_file:
        return False
    try:
        # ToDo find monthly all file
        csv_file = "/".join([csv_store_url, csv_file.pop()])
        print "Loading: %s" % csv_file
        df = pd.read_csv(csv_file, index_col=None, header=1)
        if df.size == 0:
            raise ValueError("Empty file")
        load_data_to_sql(df)
        return True
        
    except ValueError :
        return False

host = "http://atlstats.web.cern.ch/atlstats/zeroaccess/"
filelist = findall(">(\w+\d+-\d+-\d+[-\w]+)", host)
filelist = sorted(filelist, reverse=True)
options = zip(map(lambda s : host + s, filelist) , filelist)
del filelist, host