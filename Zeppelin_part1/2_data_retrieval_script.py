%pyspark
# preload for the next snippet
from urllib2 import urlopen
import re
import pandas as pd

_flag_only_static = False

def findall(reg_exp, url_storage):
    data_storage_url = urlopen(url_storage)
    content = data_storage_url.read().decode('utf-8')
    pattern = re.compile(reg_exp)
    list_ =  pattern.findall(content)
    return list_
    
def load_plot_to_sql(frame):
    sqlCtx = SQLContext(sc)
    data_volume = sqlCtx.createDataFrame(frame)
    sqlContext.registerDataFrameAsTable(data_volume, "volumes")

def prepare_plot(csv_store_url):
    print "plotting url " + csv_store_url
    list_ = []
    month_files = findall(">([\d\w]*.csv)", csv_store_url)
    
    for file_ in month_files:
        df = pd.read_csv("/".join([csv_store_url, file_]), index_col=None, header=None, names = ["period", "volume"])
        if df.size == 0:
            raise ValueError("Files exist but Empty!")
        df["month"] = file_[:-4]
        list_.append(df)
    frame = pd.concat(list_)
    frame['period'] = frame['period'].astype(str)
    static_plot(frame)
    if not _flag_only_static:
        load_plot_to_sql(frame)


host = "http://atlstats.web.cern.ch/atlstats/scrutiny/"
filelist = findall(">(\w+\d+-\d+-\d+\w+)", host)
options = zip(map(lambda s : host + s, filelist) ,filelist)
del filelist, host