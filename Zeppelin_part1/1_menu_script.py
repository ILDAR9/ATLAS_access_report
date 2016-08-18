%pyspark 

date_chosen_url = z.select("date",options)
print "Load date -> %s" % (date_chosen_url)

# data, csv, cvs (user misspelled occurs)
folder_name = findall(">(data|c[sv]{1}[sv]{1})/", date_chosen_url)
if folder_name:
    folder_name = "/".join([date_chosen_url, folder_name[0]])
    try:
        prepare_plot(folder_name)
    except ValueError as vex:
        print '%%html <h1 style="color:red">%s</h1>' % (str(vex))
else:
    alert_text = 'No data or csv folder :('
    #print "%%html <h2 style='color:red ;margin-left: 100px'>%s</h1>" % (alert_text)
    print'%%html <img src="%s" alt="%s" height="400" width="400" left-margin:100px>' % ('http://4.bp.blogspot.com/_7RXOll2bOJs/R1SQ-jBzv4I/AAAAAAAAAE8/VeTTV4X5U-w/s320/NoDataFound.gif', alert_text)
    