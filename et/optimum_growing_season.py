__author__ = 'mturtora'

import datetime as dt
import pandas as pd
import statsmodels.formula.api as smf
import et_util as et

def read_file(path):
    kdc = True
    dataframe = pd.read_csv(path, parse_dates=[[0, 1, 2]],
                          keep_date_col=kdc, warn_bad_lines=True, error_bad_lines=False,
                          na_values=[-99999, 99999, 9999, -9999, 6999], date_parser=et.doy_parser, index_col='yr_dy_t')
    return dataframe

if __name__ == '__main__':
    pd.set_option('display.width', 170)
    path = 'I:\\ALL\\TampaET\\Starkey\\process\\201505\\'
    df = read_file(path + 'pall.csv')
    df = df[['e6', 'xpt']]
    fit_list = []
    for year in ['2013', '2014']:
        df_y = df[year]
        #print('min index = %s,  max index = %s' % (min(df_y.index), max(df_y.index)))
        begin_doy = 0
        for begin_date in df_y.index.dayofyear:
            if begin_date == begin_doy:
                pass
            else:
                begin_doy = begin_date
                df_begin = df_y[(df_y.index.dayofyear >= begin_date)]
                #print('min index = %s,  max index = %s' % (min(df_begin.index), max(df_begin.index)))
                #print year begin_date

                end_doy = 366
                for end_date in reversed(df_begin.index.dayofyear):

                    if end_date == end_doy or (end_date - begin_date < 30.):
                        #print begin_date, end_date
                        pass
                    else:
                        end_doy = end_date
                        df_use = df_begin[(df_begin.index.dayofyear <= end_date)]
                        lm = smf.ols(formula='e6 ~ xpt', data=df_use).fit()
                        fit_list.append([year, begin_date, end_date, lm.params.xpt])
                        print('min index = %s,  max index = %s' % (min(df_use.index), max(df_use.index)))

                    #print('Parameters = ', begin_date, end_date, lm.params.xpt)
    #et.echo_description(df_y)
    headers = ['Year', 'Begin', 'End', 'Slope']
    print fit_list
    print 'headers = ', headers
    df_slopes = pd.DataFrame(fit_list, columns=headers)
    df_slopes.to_csv(path + 'slopes.csv')

