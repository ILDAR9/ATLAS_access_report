%pyspark

DEBUG = False

date_chosen_url = z.select("Date",options)
print "Load date -> %s" % (date_chosen_url)

# data, csv, cvs (user misspelled occurs)
folder_name = findall(">(data|c[sv]{1}[sv]{1})/", date_chosen_url)
if folder_name:
    folder_name = "/".join([date_chosen_url, folder_name.pop()])
    if not prepare_plot(folder_name):
        print '%%html <h1 style="color:red">%s</h1>' % ('Files exist but Empty!')
    else:
        offset = date_chosen_url.rfind('/')+1
        date_loaded = date_chosen_url[offset:offset+10]
        if not DEBUG:
            print '%%html <div style="text-align: center;"><h4>%s </br>by <b>%s</b></h4> <a href="%s" target="_blank">Source folder</a></div>' % ("Top-N unused data by projects/datatypes", datetime.datetime.strptime(date_loaded, "%Y-%m-%d").strftime("%d %B %Y"), date_chosen_url)
    
else:
    alert_text = 'No data or csv folder :('
    #print "%%html <h2 style='color:red ;margin-left: 100px'>%s</h2>" % (alert_text)
    if DEBUG:
        print alert_text
    else:
        print'%%html <img src="%s" alt="%s" height="210" width="210" left-margin:100px>' % ('http://4.bp.blogspot.com/_7RXOll2bOJs/R1SQ-jBzv4I/AAAAAAAAAE8/VeTTV4X5U-w/s320/NoDataFound.gif', alert_text)