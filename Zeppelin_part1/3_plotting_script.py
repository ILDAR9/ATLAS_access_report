%pyspark

def static_plot(frame):
    import pylab as pl
    import numpy as np
    import StringIO
    from matplotlib.font_manager import FontProperties

    
    PLOT_TITLE = "Volume report"
    PREF_POS = 'pos_'
    PREF_COL = 'col_'
    PREF_DATA = 'data_'
    KEY_xLABELS = 'labels'
    LEN_X = 17
    CSS_WIDTH = 800
    CSS_MARGIN_LEFT = 200
    BAR_WIDTH = 0.15 #bar width
        
    months = frame.month.unique()
    d = {}
    pl.clf()
    ax = pl.subplot(111)
    
    def set_style():
        from matplotlib import rcParams

        fig_width = 5  # width in inches
        fig_height = 3  # height in inches
        fig_size =  [fig_width,fig_height]
        params = {'backend': 'Agg',
          'axes.labelsize': 12,
          'axes.titlesize': 12,
          'font.size': 12,
          'xtick.labelsize': 12,
          'ytick.labelsize': 12,
          'figure.figsize': fig_size,
          'savefig.dpi' : 1200,
          'font.family': 'normal',
          'axes.linewidth' : 0.5,
          'xtick.major.size' : 2,
          'ytick.major.size' : 2,
          'font.size' : 8
          }
        rcParams.update(params)
    
    # grouping by months
    def load_metadata():
        def _get_data(month):
            s = frame.loc[frame['month'] == month].volume
            s = s if len(s) == LEN_X else np.append(0, s)
            return s
            
        # position
        d.update(dict(map(lambda (x,y) : (PREF_POS+ y, x-2),  enumerate(months))))
        # color
        color_list = ['red', 'blue', 'green', 'purple', 'yellow']
        d.update(dict(zip(map(lambda x: PREF_COL + x, months), color_list)))
        # load data  Y
        d.update(dict(zip(map(lambda x: PREF_DATA + x, months) , map(lambda x : _get_data(x), months))))
        # load data X
        d.update({KEY_xLABELS: frame.period.unique()})
    
    def set_up_plot():
        def _set_labels():
            # update labels 
            labels = d[KEY_xLABELS]
            labels[0] = '0 (older X)'
            labels[1] = '0 (younger X)'
            labels[16] = '> 14'
            # set new labels to plotting
            pl.xticks(range(LEN_X), labels, rotation='vertical')
            pl.margins(0.025)
            pl.subplots_adjust(bottom=0.025)
        
        # show just bottom and left border
        
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)
        ax.xaxis.set_ticks_position('bottom')
        ax.yaxis.set_ticks_position('left')
        ax.ylabel = 'Volume'
        axes = pl.axes()
        axes.autoscale_view()
        axes.set_title(PLOT_TITLE)
        
        fontP = FontProperties()
        fontP.set_size('small')
        #pl.legend(months, prop = fontP, loc='upper center', ncol=3, title='X months')
        pl.legend(months, loc='upper center', ncol=3, title='X months')
        pl.xlabel("Count access")
        pl.ylabel("Volume")
        _set_labels()
        
        
    def plotting():
        X = np.arange(LEN_X)
        
        for month_slice in  months:
            ax.bar(X + BAR_WIDTH*d[PREF_POS+month_slice], d[PREF_DATA + month_slice], width=BAR_WIDTH, color= d[PREF_COL + month_slice], align='center')
        
        ax.autoscale(tight=True)    

    def show():
        img = StringIO.StringIO()
        pl.savefig(img , bbox_inches="tight", format='svg')  
        img.seek(0)
        print "%%html <div style='width:%dpx; margin-left: %dpx'>" % (CSS_WIDTH, CSS_MARGIN_LEFT) + img.buf + "</div>"
        
    set_style()
    load_metadata()
    plotting()
    set_up_plot()
    show()