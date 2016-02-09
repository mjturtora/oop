"""
s_post_process.py
Script to process FORTRAN output into ET estimates.  Some code mirrors qalldata file,
   some mirrors qmonthdate named cumulative files (with pivot tables).
"""

if __name__ == '__main__':
    __author__ = 'mturtora'
    import matplotlib.pyplot as plt
    import datetime as dt
    import pandas as pd
    import et_util as et
    # OPTIONS
    pd.set_option('display.width', 170)
    ############################################
    # Program run time options:
    ############################################
    # date for data release:
    release_start = dt.datetime(2013, 9, 12, 14)
    # next 2 lines for 70 pt moving average alpha
    analysis_start_range = pd.date_range(end=release_start, periods=70, freq='30min')
    analysis_start = min(analysis_start_range)
    ###############################
    # path for FORTRAN output:
    path = 'I:\\ALL\\TampaET\\Starkey\\process\\201507\\'
    ###########################
    # Read p file
    fname = 'palldata.txt'
    pathname = path + fname
    kdc = True
    df_p = pd.read_csv(pathname, delim_whitespace=True, na_values=[-99999, 99999, 9999, 6999], parse_dates=[[0, 1, 2]],
                       keep_date_col=kdc, warn_bad_lines=True, error_bad_lines=False,
                       date_parser=et.doy_parser, index_col='yr_dy_t')
    print 'READ CSV (palldata.txt)'
    #df_p.drop(['Unnamed: 0', 'ep'], axis=1, inplace=True)
    df_p.drop(['ep'], axis=1, inplace=True)
    # p file just to get MAA for "good" data
    df_p.drop(['xmois', 'ws', 'solar', 'airtemp', 'def', 'avail', 'e5', 'h5',
               'cv', 'ET'], axis=1, inplace=True)
    # list of column names on p & q df's:
    #names=['yr', 'dy', 't', 'xmois', 'ws', 'solar', 'airtemp', 'def',
    #       'avail', 'xpt', 'e5', 'h5', 'e6', 'cv', 'ep', 'ET']
    ##########################
    # Get centered 141-pt moving median alpha.
    df_p['alpha_raw'] = df_p['e6'] / df_p['xpt']
    df_p['alpha_rollmed'] = pd.rolling_median(df_p['alpha_raw'], 141, center=True)
    # todo: decide if le_sim or resid needed (from qalldata file)
    #df_p['le_sim'] = df_p['alpha_rollmed'] / df_p['xpt']
    #df_p['resid'] = df_p['le_sim'] - df_p['e6']
    ###########################
    fname = 'qalldata.txt'
    pathname = path + fname
    # did something change? leading blank line not being read as column now? change parse_dates from 1,2,3 to 0,1,2
    # and comment out drop Unnamed
    df_q = pd.read_csv(pathname, delim_whitespace=True, na_values=[-99999, 99999, 9999, -9999, 6999], parse_dates=[[0, 1, 2]],
                       keep_date_col=kdc, warn_bad_lines=True, error_bad_lines=False,
                       date_parser=et.doy_parser, index_col='yr_dy_t')
    #df_q.drop(['Unnamed: 0'], axis=1, inplace=True)

    # set xpt to 0 at night
    df_q.loc[(df_q.solar < 10), 'xpt'] = 0

    # daily average of moving median alpha
    # w/o fillna, missing 6/6/13, 8/4/13-8/6/13, some of 8/16-17, 20-24, 26-30/13, 9/4-11/13 but near end?,
    # now fill gaps in rolling median while getting daily means ("forward" fill means not medians)
    ts_maa = df_p['alpha_rollmed'].resample('D', how='mean', fill_method='ffill')
    # MAA match merged into q the pandas way using index reset.set combos
    # my stackoverflow question:
    # http://stackoverflow.com/questions/24788147/pandas-lookup-daily-series-value-for-half-hour-dataframe-index
    df_q['Date'] = pd.to_datetime(df_q.index.date)
    df_q = df_q.reset_index().set_index('Date')
    df_q['MAA'] = ts_maa
    df_q = df_q.reset_index().set_index('yr_dy_t')
    # if e6 isnull, lookup MAA for day and multiply times xpt, if notnull, use e6
    # todo: excel spreadsheet vlookup calc will get a number even when MAA missing: TRUE!!!
    et.df_csv(df_q, path + 'df_q.csv', 'QQQQQ', df_name='df_q', echo=True)
    df_q.ix[pd.isnull(df_q['e6']), 'LE_gap_filled'] = df_q['xpt'] * df_q['MAA']
    df_q.ix[pd.notnull(df_q['e6']), 'LE_gap_filled'] = df_q['e6']

    ####################################################################
    # Get processed precip, water levels, and SR01Up_Avg from CALCS.
    fname = 'Stark_CALCS.csv'
    pathname = path + fname
    df_c = pd.read_csv(pathname,
                       keep_date_col=kdc, warn_bad_lines=True, error_bad_lines=False,
                       index_col='TIMESTAMP')
    df_c = df_c.drop(['0', 'ref_temp', 'precip', 'WL', 'Cp', 'del_T'], axis=1)
    # na_values=[-99999, 99999, 9999, 6999] ?
    df_q = df_q.combine_first(df_c)
    # "average Net Rad"
    df_q['mn_Rn'] = (df_q['REBS'] + df_q['Rn']) / 2

    # get positive LE estimates, use SR01Up_Avg if solar missing
    df_q['LE_gap_filled_pos'] = (df_q['LE_gap_filled'] >= 0) * df_q['LE_gap_filled']
    df_q['Filled'] = (pd.isnull(df_q['e6']))
    df_q['LE_day_and_filled'] = (pd.isnull(df_q['e6'])) & (df_q.solar > 10)
    df_q['LE_day_and_filled_SR'] = ((pd.isnull(df_q['e6'])) & ((df_q.solar > 10) |
                                   (pd.isnull(df_q['e6'])) & ((pd.isnull(df_q['solar']) & (df_q.SR01Up_Avg > 1.0)))))
    # =IF(M99919>10,1,0) (excelese)
    # Estimated ET based on positive gap filled LE, PET based on alpha = 1.26 where ET_est > 0
    df_q['ET_est'] = df_q['cv'] * df_q['LE_gap_filled_pos']
    df_q['PET'] = (df_q['ET_est'] > 0) * (1.26 * (df_q['cv'] * df_q['xpt']))

    # Above code duplicates "qalldata_1402.xlsx" type files and PET and missing logic from final process xls file
    ####################################################################
    # BUT, from FORTRAN: ep only !=0 when (day.gt.95.and.day.lt.316)!!
    df_q['ep_day'] = ((df_q['ep'] > 0) & (df_q.solar > 1)) * df_q['ep']

    # Get half-hour (_hh) data release spreadsheet order:
    df_hh = pd.DataFrame(df_q, index=df_q.index, columns=['yr', 'dy', 't', 'date', 'precip_corrected',
                        'soil_temp_smoothed',
                        'soil_heat_flux8_1', 'soil_heat_flux8_2', 'soil_moisture4', 'soil_moisture20',
                        'wind_speed', 'solar', 'SR01Up_Avg', 'REBS', 'Rn', 'mn_Rn', 'avail', 'G', 'airtemp',
                        'RH', 'DTW', 'LE_day_and_filled', 'Filled', 'LE_day_and_filled_SR', 'ET_est', 'PET',
                        'xpt', 'cv', 'MAA'])

    # todo:Need to add date specific corrections!
    df_hh['DTW'] = df_hh['DTW'] + 0.11

    # set to True to plot to compare solar with SR01Up_Avg
    make_rad_plot = True
    if make_rad_plot:
        df_hh.plot(x='solar', y='SR01Up_Avg', lw=0, marker='o', title='SR01Up_Avg vs. solar rad')
        plt.show()
    # set to True to plot to compare xpt with avail
    make_xpt_plot = True
    if make_xpt_plot:
        df_hh.plot(x='xpt', y='avail', lw=0, marker='o', title='avail vs. xpt')
        plt.show()

    ####################################################################
    # Get summaries     ts_maa = df_p['alpha_rollmed'].resample('D', how='mean')
    # need mean of ET for daily ET, not sum
    #df_day_mn = df_hh['DTW'].resample('D', how='mean')
    #df_day_sum = df_hh.loc[:, ['precip_corrected', 'ET_est']].resample('D', how='sum')
    #df_day = df_day_sum.join(df_day_mn)

    # Daily means and sums; join by day; reorder columns for final table order
    # ET on per day basis so average to get per day, sum precip to get daily total

    #pad half-hour values for ET and PET with zeros before getting daily means
    # since half-hour units are on a per day basis
    df_hh = df_hh.resample('30min', axis=0)
    df_hh.loc[:, ['ET_est', 'PET']] = df_hh.loc[:, ['ET_est', 'PET']].fillna(value=0)

    df_day_mn = df_hh.loc[:, ['DTW', 'ET_est', 'PET']].resample('D', how='mean')
    df_day_sum = df_hh['precip_corrected'].resample('D', how='sum')
    df_day = df_day_mn.join(df_day_sum)
    df_day['A_P_idx'] = df_day['ET_est'] / df_day['PET']
    df_day = pd.DataFrame(df_day, index=df_day.index, columns=['precip_corrected', 'DTW', 'ET_est', 'PET', 'A_P_idx'])
    # Potential ET on a daily basis
    # Now sum daily ET for monthly total
    df_month_mn = df_day['DTW'].resample('M', how='mean')
    df_month_sum = df_day.loc[:, ['precip_corrected', 'ET_est', 'PET']].resample('M', how='sum')
    df_month = df_month_sum.join(df_month_mn)
    df_month['A_P_idx'] = df_month['ET_est'] / df_month['PET']
    df_month = pd.DataFrame(df_month, index=df_month.index, columns=['precip_corrected', 'DTW',
                                                                     'ET_est', 'PET', 'A_P_idx'])
    #  'average net radiation, W/m2' ???
    # = LE_day_and_filled ???

    # u'ET', u'ET_est',   u'LE_gap_filled', u'LE_gap_filled_pos', u'MAA',    \
    #   u'WL_temp',     u'battery', u'cv', u'def', u'e6', u'ep', u'ep_day',\
    #  u'precip_corrected', u'soil_heat_flux8_1', u'soil_heat_flux8_2', u'soil_heat_flux_average',\
    #u'soil_moisture20', u'soil_moisture4', u'soil_temp_smoothed', u'soil_tmp4', u'solar', u'storage',\
    #u'wind_direction', u'wind_direction_sdv', u'wind_speed', u'ws', u'xmois', u'xpt'
    
    # from excel heading in data release file for 30-minute data:
    # -----------------------------------------------------------
    # year', 'day', 'hour', 'date', 'corrrected rainfall, mm', 'soil temp, deg C', 'soil heat flux1, W/m2',
    # 'soil heat flux2, W/m2', 'soil moisture (8cm), ratio', 'soil moisture (20cm), ratio', 'wind speed, m/s',
    # 'solar radiation, W/m2', 'net radiation1, W/m2', 'net radiation2, W/m2', 'average net radiation, W/m2',
    # 'available energy, W/m2', 'G, W/m2', 'air temperature, deg C', 'relative humidity, %',
    # 'depth to water below land surface, meters', 'ET flag: 0=measured or night, 1=predicted', 'ET, mm/d'

    ###################################
    # OUTPUTS

    # STILL NEED PET, 12 month ET, Actual to Potential Index

    et.df_csv(df_day, path + 'df_day.csv', 'DAYDAY', df_name='df_day', echo=True)
    #et.df_csv(df_p, path + 'df_p.csv', 'PPPPP', df_name='df_p', echo=True)
    #et.df_csv(df_q, path + 'df_q.csv', 'QQQQQ', df_name='df_q', echo=True)
    et.df_csv(df_hh, path + 'df_hh.csv', 'Half-Hour', df_name='df_hh', echo=True)
    et.df_csv(df_month, path + 'df_month.csv', 'Monthly', df_name='df_month', echo=True)
    #et.df_csv(df_mnth_mn, path + 'df_mnth_mn.csv', 'Monthly Means', df_name='df_mnth_mn', echo=True)
