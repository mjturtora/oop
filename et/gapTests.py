if __name__ == '__main__':
    '''
    Test Gap filling methods. Start by reading gap file and using it
    to build gap mask
    '''
    __author__ = 'mturtora'
    import pandas as pd
    pd.set_option('display.width', 170)

    # read data and drop cells from gaps in other variables that show up as NA
    path = 'datastore\\DeadRiver_01\\'
    df_gaps = pd.read_csv(path + 'gap_met.csv', index_col='Index')
    df_gaps = df_gaps['CS215_temp'].dropna(axis=0)

    # iterate over rows in gap list to expand to an even interval series
    looped = False  # for lazy initialization
    #for index, value in df_gaps['CS215_temp'].iteritems():
    for index, value in df_gaps.iteritems():
        #print 'iteration = ', index, value
        gap_date_index = pd.date_range(index, periods=value, freq='30min')
        s_gap = pd.Series(value, index=gap_date_index)
        #print 's_gap = \n', s_gap, '\n'
        if not looped:
            looped = True
            s_mask = s_gap
        else:
            s_mask = s_mask.append(s_gap)
    # make even interval and fill missing with 0 with a little chain.
    s_mask = s_mask.reindex(index=pd.date_range(min(s_mask.index), max(s_mask.index), freq='30min')).fillna(0)
    print '\nAFTER LOOP\n'
    print 's_mask = \n', s_mask.describe()
    s_mask.to_csv(path + 'test.csv')
