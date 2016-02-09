"""
Preprocess ET.py
Script to prepare data for fortran program.
"""

# Child of 'Preprocess DR.py' which was child of
# 'Date_Ranges_Concat_mod.py'

###############################
# Main program start
# I:\ALL\TampaET\FieldFiles\Starkey\RAW2
#path = '..\\DeadRiver\\Raw\\'

###############################
# Next Data Release Start Date/Time:
# previous release ends: 9/12/13 1330
# but, need 70 before start time for moving average alpha
# 48/day, Use pd.date_range to get backward time

if __name__ == '__main__':
    
    __author__ = 'mturtora'
    #from datetime import datetime, timedelta
    import numpy as np
    import sys
    import datetime as dt
    from time import gmtime, strftime
    import pandas as pd
    import et_util as et
    # OPTIONS
    pd.set_option('display.width', 170)
    #pd.set_option('use_inf_as_null', True)  # ??? treat inf and -inf as missing (need for CR1000 data.
    ############################################
    # Program run time options:
    ############################################
    # set in and out paths:

    # path for data logger files
    path_in = 'RAW_TEST\\'
    #path_in = '..\\DeadRiver\\Raw\\'
    #path_in = '..\\Starkey\\RAW_TEST\\'
    #path_in = '..\\Starkey\\RAW2\\'
    #path_in = '..\\Starkey\\RAW\\'
    #path_in = '..\\Starkey\\RAW_ALL\\'  # source of all that is raw data

    # each datastore folder has working files and an archive subfolder.
    # '_out' suffix is now a misnomer. Maybe rename to 'work?'
    #path_out = 'datastore\\Starkey_2015_02\\'
    path_out = 'datastore\\Starkey_01\\'
    #path_out = 'datastore\\DR01\\'
    #path_out = 'datastore\\test_datastore\\'
    #path_out = 'datastore\\raw2_datastore\\'
    #path_out = 'datastore\\raw_datastore\\' # output of the whole enchilada
    #path_out = 'datastore\\raw_datastore_all\\'  # output of the whole enchilada

    # problem data logger files "cleaned" and written to temp folder
    #path_temp = 'datastore\\stark_temp_files\\'
    #path_temp = 'datastore\\dead_temp_files\\'
    path_temp = 'datastore\\stark_temp_files\\'

    # Station to analyze: 's' for Starkey, 'd' for Dead River
    station = 's'
    #station = 'd'

    # date times for data release:
    #   looks like good data for DR starts (2009, 11, 20, 13, 00)
    #   ends: (2014, 11, 21, 11, 00)
    #   some odd dates in there to check:
    #     1819-03-31 20:00:00
    #     3/26/2019  12:00:00 PM
    # todo: output wild dates with file/record info to check, then handle

    # for quick and dirty Starkey, 2015_02, just be reasonable here.
    # Need enough pre-release Starkey start for moving average.
    release_start = dt.datetime(2009, 6, 1, 0, 0)
    release_end = dt.datetime(2016, 6, 1, 0, 0)
    # test periods:
    #release_start = dt.datetime(2010, 11, 1, 0, 0)
    #release_end = dt.datetime(2011, 6, 1, 0, 0)

    ####################################
    # rebuild inventory, raw archive and raw filtered.
    # Rebuild unfiltered raw files to folder \archive\raw
    # or rebuild filtered raw files to folder \archive\filtered
    # valid entries are 'raw' and 'filtered', any other will pass to 'working'
    #rebuild = 'filtered'
    rebuild = 'raw'
    # todo: reading ec_all is very slow. Add option to skip or only do when needed.
    #   -> kludge: deleted all but top 2 rows from working copy:
    # path_out + 'df_ec_all.csv', (full version in "df_ec_all - Copy.csv")

    #####################################

    # print descriptive stats to terminal
    echo_get = False  # echo from get_df (single file) (in inventory)
    combined_echo = False  # echo after concatenate (in inventory)
    output_echo = False  # echo on file output (csv_out must = True)

    # Output to .csv files:
    csv_out = True
    # Gap analysis options:
    gap_anal = False
    make_gap_plots = False  # Has no effect if gap_anal = False
    # Create gap mask (requires that gap_anal has be run previously):
    #expand_gaps = True  # used by impute strata

    # interpolate missing records in met file?
    impute_values = False
    # met options:
    make_calcs = False
    make_calc_plots = False

    if station == 's':
        station_prefix = 'Stark_'
    else:
        station_prefix = 'DR_'
    #if station == 's':    # don't forget to put back! (or not: just process extra!)
    if station == 'g':
        analysis_start_range = pd.date_range(end=release_start, periods=70, freq='30min')
        analysis_start = min(analysis_start_range)
    else:
        analysis_start = release_start

    # call startup to check dates and build or retrieve files
    timer = open(path_out + 'RunTime.dat', 'r+')
    timer.write('Program Start: ' + strftime("%Y-%m-%d %H:%M:%S") + '\n')
    df_met, df_rad, df_ec, df_ec_all = \
        et.startup(path_in, path_out, path_temp, station, station_prefix,
                   analysis_start, release_start, release_end,
                   rebuild, combined_echo, echo_get)

    #############################################################################

    # todo: filter data before gap analysis.
    # but hand edited data needs to be filtered and then gap checked

    if gap_anal:
        # todo: Don't call gap from empty df's!
        print 'calling gap for met'
        dtype = 'met'
        df_met_gaps = et.gap(path_out, df_met, dtype, make_gap_plots)
        print '#################################################'
        print 'calling gap for rad'
        dtype = 'rad'
        df_rad_gaps = et.gap(path_out, df_rad, dtype, make_gap_plots)
        print '#################################################'
        print 'calling gap for ec'
        dtype = 'ec'
        df_ec_gaps = et.gap(path_out, df_ec, dtype, make_gap_plots)
        print '#################################################'

    if impute_values:
        df_met = et.impute_master(path_out, df_met)

    # run met calcs
    if make_calcs:
        if station == 's':
            df_calc = et.s_met_calcs(path_out, df_met, df_rad, df_ec, station, make_calc_plots)
        else:
            df_calc = et.d_met_calcs(df_met, df_rad, df_ec, station, make_calc_plots)

    # Output data to csv
    if csv_out:
        et.df_csv(df_ec, path_out + station_prefix + 'EC.csv', 'EC', 'df_ec', output_echo)
        et.df_csv(df_ec_all, path_out + station_prefix + 'EC_all.csv', 'EC ALL', 'df_ec_all', output_echo,
                  index=False, header=False)
        et.df_csv(df_met, path_out + station_prefix + 'Met.csv', 'MET', 'df_met', output_echo)
        et.df_csv(df_rad, path_out + station_prefix + 'RAD.csv', 'RAD', 'df_rad', output_echo)
        if make_calcs:
            et.df_csv(df_calc, path_out + station_prefix + 'CALCS.csv', 'CALCS', 'df_calc', output_echo)
    timer.write(' Program  End: ' + strftime("%Y-%m-%d %H:%M:%S") + '\n')
    timer.close()
    print 'Program End'
    print '###############'
