"""
et_util.py
Functions for processing Tampa ET data.
"""

# ##########################################################################
# step by step instructions for SPHINX:
# http://codeandchaos.wordpress.com/2012/07/30/sphinx-autodoc-tutorial-for-dummies/

# ##########################################################################
# some syntax examples:
# pd.rolling_mean(df, 48).plot(subplots=True)
# df['LE_corrected'].plot(style='o', markeredgecolor='red', markerfacecolor='none', markersize=3)
# df.ix[(df.Uz == 99999) | (df.Uz == -99999), 'Uz'] = np.nan
# .to_csv('I:\ALL\TampaET\FieldFiles\python\df_mr.csv')
# print min(df.index), max(df.index)
# df2 = df.reindex(index = pd.date_range(min(df.index),max(df.index), freq='30min'))

# ##########################################################################
# Data notes:
# if counts of Uz ne to LE there is a problem!!
# counts of first 9 fields > remaining indicate battery problems!
# File E1071003.DAT gets an extra field starting in row 95 (from 41 to 42)
# ##########################################################################

import pandas as pd
import numpy as np
import datetime as dt
#import matplotlib.pyplot as plt
import sys
import os
import time

# Begin functions


def startup(path_in, path_out, path_temp,
            station, station_prefix, analysis_start, release_start, release_end,
            rebuild, combined_echo, echo_get):
    """
    Checks file status, rebuilds inventory and archive if requested, working files if needed.

    :param path_in: Path to raw data files
    :param path_out: Path to store processed data
    :param path_temp:
    :param station: Currently, 's' or 'd'
    :param station_prefix: File name prefix for station files
    :param analysis_start: Sometimes earlier that release start to support moving averages.
    :param release_start: Datetime for start of data report
    :param release_end: Datetime for end of data report
    :param rebuild: Text request for archive rebuild
    :param combined_echo: Logical request for dataframe description of final data to console
    :param echo_get:  Logical request for dataframe description of each input file to console
    :return: df_ec, df_ec_all, df_met, df_rad
    """

    # open configuration file in datastore folder for each datastore:
    config = open(path_out + 'Configuration.txt', 'r+')
    if rebuild.lower() == 'raw' or rebuild.lower() == 'filtered':
        if rebuild.lower() == 'raw':  # rebuild AND filter. (filter no matter what)
            info_message = 'Inventory and Raw archive rebuild requested. This takes a long time. Are you sure?'
            prompt_message = 'Rebuild archive? "Y/N": '
            sys_message = 'Archive rebuild aborted'
            confirm_rebuild(info_message, prompt_message, sys_message)  # if it returns, rebuild confirmed.
            # Read all raw data files to create "archive" dataframes where "archive" means full data set.
            # Perhaps the inventory step could be separated out
            df_met, df_rad, df_ec, df_ec_all = \
                inventory(path_in, path_out, path_temp, station, station_prefix, analysis_start,
                          combined_echo, echo_get)
            print 'Inventory Complete'
            print 'Writing all data to raw archive folder'
            # todo: should write a file with actual timestamps of achived files
            # if archive update fails, file times and working files not updated either.
            write_raw(path_out + 'archive\\raw\\', df_met, df_rad, df_ec, df_ec_all)
            print 'Rebuild of raw archive complete'

        if rebuild.lower() == 'filtered':  # re-filter raw archive
            # must input files from raw archive before filter since raw not in memory
            print 'Reading all data from raw archive folder'
            df_met, df_rad, df_ec, df_ec_all = read_files(path_out + 'archive\\raw\\')

        '''
        Commented out manual filtering
        # Filter here either way
        print '**************************************'
        print 'Filtering MET data for %s' % station
        met_filter(df_met, station)
        print 'Converting precip units for %s' % station
        df_met = convert_precip(df_met)
        print 'Applying Manual edits from edit files on path:', path_out
        df_met = apply_edits(path_out, df_met)
        print '**************************************'

        '''

        print 'Writing all data to filtered archive folder'
        # todo: should write a file with actual timestamps of archived files
        # if archive update fails, file times and working files not updated either.
        write_raw(path_out + 'archive\\filtered\\', df_met, df_rad, df_ec, df_ec_all)
        print 'Rebuild of filtered archive complete'

        # Archive rebuilt and data in memory so select from memory and save dates to config file
        print 'Selecting records over requested date range from data in memory to rebuild working files.'
        # Call function to select user defined date range
        # (somewhat of a kludge but could be cleaned up later by calling for each type separately)
        df_met, df_rad, df_ec, df_ec_all = select_dates(analysis_start, release_end,
                                                        df_met, df_rad, df_ec, df_ec_all)
        print 'Writing data over requested date range to disk files on working data path.'
        # Call function to write date limited (working) files to disk
        write_raw(path_out, df_met, df_rad, df_ec, df_ec_all)
        print 'Updating configuration file with new working file datetimes.'
        write_file_times(config, analysis_start, release_end)

    # If no archive rebuild requested, check if working files rebuild is needed.
    else:
        print 'No archive rebuild requested.'
        print "Checking requested time span against Working File times."
        print "Requested release start %s, requested release end %s." % (release_start, release_end)
        file_times = get_file_times(config)
        # todo: following should really check last archived time, not last requested.
        # file_times are the requested times, not the actual.
        if analysis_start == file_times[0] and \
                        release_end == file_times[1]:
            print ' '
            print 'No change in time span, no rebuild or reselect needed.'
            print 'Reading files from working path.'
            print ' '
            df_met, df_rad, df_ec, df_ec_all = read_files(path_out)
        else:
            print ' '
            print 'Time span changed, checking if rebuild of working files needed.'
            if analysis_start < file_times[0] or \
                            release_end > file_times[1]:
                # todo: this only works if requested times are available, should check archive
                info_message = 'Requested time span outside of Working File timespan.'
                prompt_message = 'Rebuild working file from archive? "Y/N": '
                sys_message = 'program terminated, Check requested time span'
                confirm_rebuild(info_message, prompt_message, sys_message)
                # rebuild confirmed if returned
                print 'Reading archive files to rebuild working files.'
                df_met, df_rad, df_ec, df_ec_all = read_files(path_out + 'archive\\')
            else:
                print 'Time span within working range, reselecting from working files.'
                print 'Reading existing working files.'
                df_met, df_rad, df_ec, df_ec_all = read_files(path_out)
            # new date range either way
            print 'Selecting data within new date range.'
            # Call function to select user defined date range (somewhat of a kludge but could be cleaned up later)
            # with call by "object" do I really need the equals sign?
            # also, if called individually, could turn off df_ec_all.
            df_met, df_rad, df_ec, df_ec_all = select_dates(analysis_start, release_end,
                                                            df_met, df_rad, df_ec, df_ec_all)
            print 'Writing data within new date range to new working files on output path.'
            # Call function to write date limited (working) files to disk
            write_raw(path_out, df_met, df_rad, df_ec, df_ec_all)
            print 'Updating configuration file with new working datetime range.'
            write_file_times(config, analysis_start, release_end)
    config.close()
    return df_met, df_rad, df_ec, df_ec_all


def confirm_rebuild(info_message, prompt_message, sys_message):
    response = ''  # just to make pyCharm happy.
    while True:
        print info_message
        response = raw_input(prompt_message).upper()
        if response in ['Y', 'N']:
            break
        else:
            print "Not a valid entry"
    if response != 'Y':
        sys.exit(sys_message)  # exit early to test above code
    return


def get_file_times(config):
    """Reads Configuration.txt and gets archive start and end times.

    :return: file_times list

    Dependents: main
    External Dependencies: datetime
    """
    lines = config.readlines()
    file_times = []
    for l in lines:
        file_time = dt.datetime.strptime(l[:-2], "%Y-%m-%d %H:%M:%S")
        file_times.append(file_time)
    print 'file_times = ', file_times
    return file_times


def write_file_times(config, release_start, release_end):
    """Writes archive start and end times to Configuration.txt.

    :param release_start: start datetime of last archive build
    :param release_end: end datetime of last archive build
    :return: file_times list

    Dependents: main
    External Dependencies: datetime
    """
    r_start = release_start.strftime("%Y-%m-%d %H:%M:%S")
    r_end = release_end.strftime("%Y-%m-%d %H:%M:%S")
    config.seek(0)
    config.write(r_start + '\n')
    config.write(r_end + '\n')
    return


# try inventory as function that builds datastore
def clean_file(path_in, path_temp, fname):
    default_path = path_in + fname
    f = open(default_path, 'r')
    tmp_name = path_temp + fname
    dirty = False
    while True:
        line = f.readline()
        if line[0] != ',':
            break
        print 'Found bad line using clean_file function: '
        print line
        dirty = True
    if dirty:
        g = open(tmp_name, 'w')
        while True:
            g.write(line)
            line = f.readline()
            if len(line) == 0:
                g.close()
                break
    else:
        tmp_name = default_path
    f.close()
    return tmp_name


def inventory(path_in, path_out, path_temp, station, station_prefix, analysis_start,
              combined_echo, echo_get):
    # path = 'I:\\ALL\\TampaET\\FieldFiles\\Starkey\\RAW2\\'
    # path = '..\\DeadRiver\\Raw\\Working\\'
    # path = '..\\DeadRiver\\Raw\\'

    et_files = os.listdir(path_in)
    CRthousand_cnt = 0
    CRtenX_cnt = 0
    for fname in et_files:
        pathname = path_in + fname  # set default path (can be changed for dirty files)
        print '############################################################'
        print pathname
        # E2071003.DAT almost identical timestamps as E2071003.DAT but values different??
        # -> skip it! (7/10/2003)
        # M021709.DAT (2/17/2009) has extra field from row 1495 on... changed extension to
        # 'original' to skip it!
        # M031109.DAT (3/11/2009) SAME
        # M090407.dat (9/4/2007) SAME
        # starkey4R_093009_2009_09_30_09_17_09.dat file has header only, skip
        # starkey4R_093009_2009_09_30_09_17_42.dat SAME
        # DR_EC_CR10X_20130801.dat was all messed up
        # SM_20140305.dat has mixed types? should check - caused by ,-,
        # fixed by adding ", na_values=['-', -99999, 99999, 6999, -6999]
        # to (also added '-' to read_csv in read_s_met
        # SR20130410.dat - mixed types, more columns. 3,4,5,6,7,8,9,11,12,13,14,15
        #   fixed by adding ", na_values=["NAN", "INF", "-INF", 6999]" to read_CRthousand

        #  but why was this one in there? or fname.upper() == 'E2071003.DAT'
        if fname[-3:].upper() == 'BAK' or fname[-4:].upper() == 'XLSX' or \
                        fname[-8:].upper() == 'ORIGINAL' \
                or fname[0:4].upper() == 'MACRO':
            continue
        if fname[-3:].upper() == 'DAT':  # Just take .dat files
            f = open(pathname, 'r')
        else:
            continue
        line = f.readline()
        f.seek(0)
        if line[0] == '"':
            f.close()
            # At Starkey, rad files are CR1000 files
            # CR1000 (4-comp radiometer file w/header lines and TimeStamp)
            #
            CRthousand_span = read_CRthousand_file(pathname, fname)
            #
            date_str = CRthousand_span['StopTime'][0][0:11]
            time_str = CRthousand_span['StopTime'][0][11:-1]
            stop_time = dt.datetime(int(date_str[0:4]), int(date_str[5:7]), int(date_str[8:10]),
                                    int(time_str[0:2]), int(time_str[3:5]))
            if stop_time < analysis_start:
                #print 'fname, stop_time, analysis_start = ', fname, stop_time, analysis_start
                continue
            elif CRthousand_cnt == 0:
                CRthousand_dict = CRthousand_span
                CRthousand_cnt = 1
            else:
                CRthousand_dict = pd.concat([CRthousand_dict, CRthousand_span])
                #print CRthousand_dict
        else:
            # need to deal with bad first lines like:
            # ,.00167,.09777,0,.00991,0,.02939,.00104,12.82 in
            # SE_2013_80-132.dat
            # start with comma?  if comma skip line
            # CR10X file w/Day of Year date values and no header lines
            if line[0] == ',':
                pathname = clean_file(path_in, path_temp, fname)
                # or fname.find('Dead') > -1 \  <- don't know why this was below
            f.close()
            # Dead River rad files are not CR1000
            if fname.upper().find('DATA') > -1 \
                    or fname.find('Rad') > -1 or fname.find('r2') > -1 \
                    or fname.upper().find('DEAD') > -1:
                df_type = 'rad'
            elif 'M' in fname.upper():
                #if fname.find('M') > -1 or fname.find('Met') > -1:
                df_type = 'met'
            else:
                df_type = 'ec'

            print 'df_type= ', df_type
            #
            #CRtenX_span = read_CRtenX_file(pathname, fname, df_type, skip)
            CRtenX_span = read_CRtenX_file(pathname, fname, df_type)
            #
            try:
                cr_date = dt.datetime(CRtenX_span['end_year'], 1, 1, CRtenX_span['end_time'] / 100,
                                      CRtenX_span['end_time'] - (100 * (CRtenX_span['end_time'] / 100))) \
                          + dt.timedelta(days=int(CRtenX_span['end_doy'][0]) - 1)
                # year, month, hhmm:
                if cr_date < analysis_start:
                    continue
                elif CRtenX_cnt == 0:
                    CRtenX_dict = CRtenX_span
                    CRtenX_cnt = 1
                else:
                    CRtenX_dict = pd.concat([CRtenX_dict, CRtenX_span])
            except ValueError:
                print 'ValueError on file: ', fname
                print 'Look for NaN in'
                print "CRtenX_span['end_year'] = ", CRtenX_span['end_year']
                print "CRtenX_span['end_time'] = ", CRtenX_span['end_time']
                print 'OR'
                print "CRtenX_span['end_doy'] = ", CRtenX_span['end_doy']
                pass

        f.close()
    CRtenX_dict = CRtenX_dict.sort(['df_type', 'str_year', 'str_doy', 'str_time'])

    print
    print
    print '#######################'
    print 'All CR10X Files In Folder: %s ' \
          '\n  with an End Date > %s the Release Start Date' % (path_in, analysis_start)
    print
    print CRtenX_dict
    CRtenX_dict.to_csv(path_out + station_prefix + 'CR10X Date Ranges.csv')
    print
    print
    try:
        CRthousand_dict = CRthousand_dict.sort(['StartTime'])
        print '#######################'
        print 'All CR1000 Files In Folder: %s' \
              '\n  with an End Date > %s the Release Start Date' % (path_in, analysis_start)
        print
        print CRthousand_dict
        CRthousand_dict.to_csv(path_out + station_prefix + 'CR1000 Date Ranges.csv')
        print
    except NameError:
        pass
    # ############################################################################
    # dictionaries of date ranges built, now concatenate data for all files back to
    # earliest files needed

    #BUT!!! need original format for ec data!!!!
    # so should read ec files using different method than read_s_ec()
    # or add options to it to keep data in original format

    # concatenate datafiles to build one file for each data type to cover needed time span.
    if station == 's':
        f_com = ["CRtenX_dict[(CRtenX_dict.loc[:, 'df_type'] == 'met')]['file_name']",
                 "CRtenX_dict[(CRtenX_dict.loc[:, 'df_type'] == 'ec')]['file_name']",
                 "CRthousand_dict[(CRthousand_dict.loc[:,'df_type']=='rad')]['file_name']"]
    else:  # station = Dead River (doesn't have CR1000 for rad, uses storage module)
        f_com = ["CRtenX_dict[(CRtenX_dict.loc[:, 'df_type'] == 'met')]['file_name']",
                 "CRtenX_dict[(CRtenX_dict.loc[:, 'df_type'] == 'ec')]['file_name']",
                 "CRtenX_dict[(CRtenX_dict.loc[:, 'df_type'] == 'rad')]['file_name']"]
    # cycle through data types
    for i, df_type in enumerate(['met', 'ec', 'rad']):
        exec 'fl = f_com[i]'
        s = eval(fl)
        flist = [s[j] for j in range(len(s))]
        print '##########################'
        print ' '
        print 'flist = ', flist
        df_temp = concat_df(path_in, path_temp, flist, df_type, station, echo_get)
        print ' '

        if df_type == 'ec':
            df_ec = df_temp
            df_name = 'df_ec'
            # read "full" ec file
            print '##########################'
            print ' '
            print 'flist = ', flist
            df_ec_all = concat_df(path_in, path_temp, flist, df_type, station, echo_get, 'dat')
            print ' '
        elif df_type == 'met':
            df_met = df_temp
            df_name = 'df_met'
        else:
            df_rad = df_temp
            df_name = 'df_rad'
        if combined_echo:
            echo_description(df_temp, 'Combined', df_type, df_name)
            if df_type == 'ec':
                echo_description(df_ec_all, 'Combined', 'ec_all', 'df_ec_all')
    print 'Done with concat'
    print ' '
    print '#########################'
    return df_met, df_rad, df_ec, df_ec_all


def met_filter(df_met, station):
    # filter Dead River met records. (Set values outside limits to missing)
    # todo: log these occurrences for reporting
    if station == 'd':
        df_met.ix[(df_met['0'] != 120), '0'] = np.nan
        df_met.ix[(df_met.battery < 9) | (df_met.battery > 30), 'battery'] = np.nan
        df_met.ix[(df_met.CS215_RH < 10) | (df_met.CS215_RH > 110), 'CS215_RH'] = np.nan
        df_met.ix[(df_met.CS215_temp < -5) | (df_met.CS215_temp > 40), 'CS215_temp'] = np.nan

        df_met.ix[df_met.KH2O_volt < -300, 'KH2O_volt'] = np.nan
        df_met.ix[(df_met.net_rad_1 < -250) | (df_met.net_rad_1 > 1000), 'net_rad_1'] = np.nan
        df_met.ix[df_met.precip < 0, 'precip'] = np.nan
        df_met.ix[(df_met.ref_temp < -20) | (df_met.ref_temp > 60), 'ref_temp'] = np.nan

        df_met.ix[(df_met.soil_heat_flux < -40) | (df_met.soil_heat_flux > 40), 'soil_heat_flux'] = np.nan
        df_met.ix[(df_met.soil_moisture < -1) | (df_met.soil_moisture > 10), 'soil_moisture'] = np.nan
        df_met.ix[(df_met.soil_tmp < -10) | (df_met.soil_tmp > 50), 'soil_tmp'] = np.nan
        df_met.ix[(df_met.solar < -10) | (df_met.solar > 1200), 'solar'] = np.nan
        # Truncate solar data past sensor failure:
        df_met.loc[(df_met.index >= u'20110923'), 'solar'] = np.nan
        df_met.ix[(df_met.solar < -10) | (df_met.solar > 1200), 'solar'] = np.nan

        df_met.ix[(df_met.wind_direc_mean < -10) | (df_met.wind_direc_mean > 400), 'wind_direc_mean'] = np.nan
        df_met.ix[(df_met.wind_direction_sdv < -10) | (df_met.wind_direction_sdv > 125), 'wind_direction_sdv'] = np.nan
        df_met.ix[(df_met.wind_speed < -100) | (df_met.wind_speed > 15), 'wind_speed'] = np.nan

        df_met.ix[(df_met.WL < 30) | (df_met.WL > 45), 'WL'] = np.nan
        df_met.ix[(df_met.WL_temp < -10) | (df_met.WL_temp > 50), 'WL_temp'] = np.nan


def convert_precip(df):
    # convert mm to inches for original rain gauge
    # 3/31/2011 04:30 has precip = 144 : check NWS?
    # 120,2011,90,430,18.88,144 row 33 in file I:\..\RAW\M041111.DAT
    # same on Q: file from 4/11/2011 but excel master has 0.4 for raw.
    # should check any rain > 1.5 inches?
    # seems to match NWS Tampa.
    # Also, field note from 3/30/11 that gage was clogged so rain from
    # 3/28 may have trickled in until cleared at 1230 on 3/30.
    df.ix[(df.index <= '20120217 14:30'), 'precip'] = df['precip'] / 25.4
    # apply multiplier correction to second rain gauge
    df.ix[('20120217 15:00' <= df.index) & (df.index <= '20120416 09:30'), 'precip'] = df['precip'] / 10.
    return df


def apply_edits(path_out, df):
    """
    Use external file of manually identified bad data to remove bad data.

    :param path_out:
    :param df:
    :return:
    """
    # delete CS 215 data
    fname = 'CS_215_Deletions.csv'
    delete_list = [('Deleted Temp', 'CS215_temp'), ('Deleted RH', 'CS215_RH')]
    df = join_and_delete(df, path_out, fname, delete_list)

    # delete Observation Well Data
    fname = 'KPSI_Deletions.csv'
    delete_list = [('Deleted WL', 'WL'), ('Deleted WL_temp', 'WL_temp')]
    df = join_and_delete(df, path_out, fname, delete_list)
    return df


def join_and_delete(df, path_out, fname, delete_list):
    df_deletions = pd.read_csv(path_out + 'archive\\' + fname,
                               parse_dates=True, index_col='TIMESTAMP')
    # Comments column not in all csv's
    if 'Comments' in df_deletions.columns:
        df_deletions.drop(['Comments'], axis=1, inplace=True)
    # df_deletions.plot(style='o')
    # plt.show()

    df = df.join(df_deletions)
    for delete_indicator, edit_column in delete_list:
        df.ix[pd.notnull(df[delete_indicator]), edit_column] = np.nan
    df.drop([delete_list[0][0], delete_list[1][0]], axis=1, inplace=True)
    return df


def read_files(path_out):
    print 'reading ec'
    df_ec = pd.read_csv(path_out + 'df_ec.csv', parse_dates=True, index_col='TIMESTAMP')
    print 'reading ec_all'
    df_ec_all = read_s_ec(path_out + 'df_ec_all.csv', out_form='dat')
    print 'reading met'
    df_met = pd.read_csv(path_out + 'df_met.csv', parse_dates=True, index_col='TIMESTAMP')
    print 'reading rad'
    df_rad = pd.read_csv(path_out + 'df_rad.csv', parse_dates=True, index_col='TIMESTAMP')
    print 'All input files read'
    print '###########################'
    return df_met, df_rad, df_ec, df_ec_all


def write_raw(path_out, df_met, df_rad, df_ec, df_ec_all):
    df_ec.to_csv(path_out + 'df_ec.csv')
    df_ec_all.to_csv(path_out + 'df_ec_all.csv', index=False, header=False)
    df_met.to_csv(path_out + 'df_met.csv')
    df_rad.to_csv(path_out + 'df_rad.csv')
    return df_met, df_rad, df_ec, df_ec_all


# ##########################################
# for each type, merge files in date range
def concat_df(path_in, path_temp, flist, df_type, station, echo_get=False, out_form='csv'):
    """ Concatenate given list of files using 'update.'

    :rtype : object
    :param path_in: Pathname to files
    :param flist: List of files to combine
    :param df_type: Data type
    :param out_form: passed to get_df.  'csv' or not
    :return: df_cat - DataFrame of combined data

    """
    once = False
    df_cat = []
    for fn in flist:
        if not once:
            print 'Reading %s Files for station %s' % (df_type, station)
        print 'Reading file,  out_form: ', path_in + fn, out_form
        # clean CR10X files (don't need to check CR1000 files):
        if df_type != 'RAD':
            path = clean_file(path_in, path_temp, fn)
        else:
            path = path_in + fn

        df = get_df(path, df_type, out_form, station, echo_get)
        # oddly, some files have duplicate timestamps.  This takes first one.
        # might need to check how this works with battery problems
        # 20150713: Datalogger time reset may cause double punches (duplicates)
        # check Starkey 12/11/14 15:30 (2014,345,1530)
        # For that record, first one looks better.
        df = df.reset_index().drop_duplicates(cols='TIMESTAMP').set_index('TIMESTAMP')
        if not once:
            print 'Read First %s file' % df_type
            df_cat = df
            once = True
        else:
            df_cat = df_cat.combine_first(df)
    return df_cat


# generalize df read process with feedback
def get_df(fname, df_type, out_form, station, echo_get):
    """
    "Call correct Campbell read function for station and data type and print feedback"

    :param fname: file path
    :param df_type: Datafile type (ec, met, rad)
    :param station: Starkey or Dead River
    :param echo_get: enable or disable print feedback
    :return: DataFrame of datafile

    Dependants: concat_df
    Internal Dependencies:none
    External Dependencies:pandas
    """
    f = open(fname, 'r')
    if station == 's':
        if df_type == 'ec' and out_form == 'csv':
            df = read_s_ec(f)
        elif df_type == 'ec' and out_form == 'dat':
            df = read_s_ec(f, 'dat')
        elif df_type == 'met':
            df = read_s_met(f)
        else:
            df = read_s_rad(f)
    else:
        if df_type == 'ec' and out_form == 'csv':
            df = read_d_ec(f)
        elif df_type == 'ec' and out_form == 'dat':
            df = read_d_ec(f, 'dat')
        elif df_type == 'met':
            df = read_d_met(f)
        else:
            df = read_d_rad(f)
    f.close()
    if echo_get:
        echo_description(df, fname, df_type, out_form)
    return df


# EC read functions read all columns and drop extraneous first, then name
# what remains.
def read_s_ec(f, out_form='csv'):
    """Read Starkey ec CR10X file two ways depending on purpose.
    Keeps all variables for 'non-csv' option.  Use for building input file for fortran.

    :param f: file path
    :param out_form: destination output type (csv drops extraneous variables)
    :return: DataFrame of CR10X datafile with either selected variables or all
    
    Dependants: get_df - helper function
    Internal Dependencies: doy_parser
    External Dependencies:pandas
    """
    if out_form == 'csv':
        kdc = True  # Keep Date Column
        df = pd.read_csv(f, header=None, na_values=[-99999, 99999], parse_dates=[[1, 2, 3]], date_parser=doy_parser,
                         keep_date_col=kdc, warn_bad_lines=True, error_bad_lines=False,
                         index_col='1_2_3')
        print 'Num cols in ec.dat: len(df_ec.columns) = ', len(df.columns)
        if len(df.columns) == 42:
            df.drop([7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                     21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
                     33, 34, 35,
                     38,
                    ], axis=1, inplace=True)
            df.columns = ['Site', '1', '2', '3', 'Uz', 'Ux', 'Uy', 'LE', 'H_Ts', 'LE_corrected',
                          'Battery', 'smpl_cnt', 'hygro_volt',
                          'airtemp', 'RH', 'EC_Ref_Temp']
        else:
            df.drop([7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                     21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
                     33, 34, 35,
                     38,
                    ], axis=1, inplace=True)
            df.columns = ['Site', '1', '2', '3', 'Uz', 'Ux', 'Uy', 'LE', 'H_Ts', 'LE_corrected',
                          'Battery', 'smpl_cnt', 'hygro_volt',
                          'airtemp', 'RH']

        df.index.names = ['TIMESTAMP']
        # rough guess of LE
        df['LE_corrected'] = df['LE'] / -0.192
    else:
        # read all data for fortran output (42 total columns)
        kdc = False
        df = pd.read_csv(f, header=None, parse_dates=[[1, 2, 3]], date_parser=doy_parser,
                         keep_date_col=kdc, warn_bad_lines=True, error_bad_lines=False,
                         index_col='1_2_3')
        # df.iloc[:, 0] = df.iloc[:, 0].astype('int32')
        df.index.names = ['TIMESTAMP']
        df.insert(1, 1, df.index.year)
        df.insert(2, 2, df.index.dayofyear)
        df.insert(3, 3, df.index.hour)
        df['min'] = df.index.minute
        for i in range(len(df)):
            #print 'str(row[3]).replace(.0, ) =', str(row[3]).replace('.0', '')
            if df.ix[i, 'min'] == 0:
                df.ix[i, 3] = int(str(df.ix[i, 3]).replace('.0', '') + '00')
            else:
                df.ix[i, 3] = int(str(df.ix[i, 3]).replace('.0', '') + '30')
                #print 'str(row[3]).replace(.0, ) =', str(df.ix[i, 3]).replace('.0', '')
        df = df.drop(['min'], axis=1)
    return df


def read_d_ec(f, out_form='csv'):
    """Read Dead River ec CR10X file two ways depending on purpose.
       Keeps selected variables for analysis.

    :param f: file path
    :return: DataFrame of CR10X datafile with selected variables

    Dependants: get_df - helper function
    Internal Dependencies: doy_parser
    External Dependencies:pandas
    """
    # print 'READ_D_EC: F: ', f
    if out_form == 'csv':
        kdc = True  # Keep Date Column
        df = pd.read_csv(f, header=None, na_values=[-99999, 99999, -6999], parse_dates=[[1, 2, 3]],
                         date_parser=doy_parser,
                         keep_date_col=kdc, warn_bad_lines=True, error_bad_lines=True,
                         index_col='1_2_3')
        df.drop([7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
                 33, 34, 35,
                 38, 39, 40], axis=1, inplace=True)
        df.columns = ['Site', '1', '2', '3', 'Uz', 'Ux', 'Uy',
                      'LE', 'H_Ts', 'LE_corrected',
                      'Battery', 'smpl_cnt', 'hygro_volt']
        df.index.names = ['TIMESTAMP']
        # rough guess of LE
        df['LE_corrected'] = df['LE'] / -0.192
    else:
        #print 'READ_D_EC, OUT_FORM != CSV'
        # read all data for fortran output (42 total columns ???)
        kdc = False
        df = pd.read_csv(f, header=None, parse_dates=[[1, 2, 3]], date_parser=doy_parser,
                         keep_date_col=kdc, warn_bad_lines=True, error_bad_lines=False,
                         index_col='1_2_3')
        #df.iloc[:, 0] = df.iloc[:, 0].astype('int32')
        df.index.names = ['TIMESTAMP']
        df.insert(1, 1, df.index.year)
        df.insert(2, 2, df.index.dayofyear)
        df.insert(3, 3, df.index.hour)
        df['min'] = df.index.minute
        for i in range(len(df)):
            #print 'str(row[3]).replace(.0, ) =', str(row[3]).replace('.0', '')
            if df.ix[i, 'min'] == 0:
                df.ix[i, 3] = int(str(df.ix[i, 3]).replace('.0', '') + '00')
            else:
                df.ix[i, 3] = int(str(df.ix[i, 3]).replace('.0', '') + '30')
                #print 'str(row[3]).replace(.0, ) =', str(df.ix[i, 3]).replace('.0', '')
        df = df.drop(['min'], axis=1)
        #print 'READ_D_EC: Bottom'

    return df


# MET read functions assign names to all input columns and then drop extraneous ones
def read_s_met(f):
    """Read Starkey met CR10X file two ways depending on purpose.
    Keeps selected variables for analysis.

    :param f: file path
    :return: DataFrame of CR10X datafile with selected variables

    Dependants: get_df - helper function
    Internal Dependencies: doy_parser
    External Dependencies:pandas
    """

    df = pd.read_csv(f, header=None, na_values=['-', -99999, 99999, 6999, -6999], parse_dates=[[1, 2, 3]],
                     date_parser=doy_parser,
                     index_col='1_2_3', names=['0', '1', '2', '3',
                                               'ref_temp', 'precip', 'soil_tmp4',
                                               'soil_heat_flux8_1', 'soil_heat_flux8_2',
                                               'soil_moisture4', 'soil_moisture20',
                                               'wind_speed', 'wind_direction',
                                               'wind_direction_sdv', 'solar',
                                               'net_rad_1', 'net_rad_2', 'battery', 'WL',
                                               'WL_temp', 'w', 'x', 'y', 'z', 'aa',
                                               'ab', 'ac', 'ad', 'ae', 'af', 'ag'])
    df = df.drop(['net_rad_2',
                  'w', 'x', 'y', 'z',
                  'aa', 'ab', 'ac', 'ad', 'ae', 'af', 'ag'
                 ], axis=1)
    df.index.names = ['TIMESTAMP']
    return df


def read_d_met(f):
    """Read Dead River met CR10X file two ways depending on purpose.
    Keeps selected variables for analysis.

    :param f: file path
    :return: DataFrame of CR10X datafile with selected variables
    
    Dependants: get_df - helper function
    Internal Dependencies: doy_parser
    External Dependencies:pandas
    """
    df = pd.read_csv(f, header=None, na_values=[-99999, 99999, 6999, -6999], parse_dates=[[1, 2, 3]],
                     warn_bad_lines=True, error_bad_lines=False,
                     date_parser=doy_parser,
                     index_col='1_2_3', names=['0', '1', '2', '3',
                                               'ref_temp', 'precip', 'soil_tmp',
                                               'soil_moisture', 'soil_heat_flux',
                                               'solar', 'net_rad_1',
                                               'WL', 'WL_temp', 'wind_speed',
                                               'wind_direc_mean', 'wind_direction_sdv',
                                               'KH2O_volt', 'battery',
                                               'CS215_temp', 'CS215_RH',
                                               'w', 'x', 'y', 'z', 'aa',
                                               'ab', 'ac', 'ad', 'ae'])
    df = df.drop(['w', 'x', 'y', 'z',
                  'aa', 'ab', 'ac', 'ad', 'ae'
                 ], axis=1)
    df.index.names = ['TIMESTAMP']
    return df


def read_s_rad(f):
    """
    Read Starkey CR1000 datafile of 4-comp radiometer.

    :param f: file path
    :return: DataFrame of 4-comp radiometer data.
    
    Dependants: get_df - helper function
    Internal Dependencies: doy_parser
    External Dependencies:pandas
    """
    # but since pandas can treat INF's different than NaN's, may want to chage na_values.
    df = pd.read_csv(f, skiprows=[0, 2, 3], parse_dates=True, na_values=["NAN", "INF", "-INF", 6999],
                     index_col='TIMESTAMP')
    df = df.drop(['RECORD'], axis=1)
    return df


# MET read functions assign names to all input columns and then drop extraneous ones
def read_d_rad(f):
    """Read Dead River met CR10X file.
    Keeps selected variables for analysis.

    :param f: file path
    :return: DataFrame of CR10X datafile with selected variables

    Dependants: get_df - helper function
    Internal Dependencies: doy_parser
    External Dependencies:pandas
    """
    df = pd.read_csv(f, header=None, na_values=[-99999, 99999, 6999, -6999], parse_dates=[[1, 2, 3]],
                     date_parser=doy_parser,
                     index_col='1_2_3', names=['0', '1', '2', '3',
                                               'SW_out', 'SW_in',
                                               '6', '7',
                                               'Sensor_temp',
                                               '9', '10',
                                               'LW_in', 'LW_out', 'Battery', 'Sky_Temp', 'Ground_Temp'])
    df = df.drop(['0', '6', '7', '9', '10'], axis=1)
    df.index.names = ['TIMESTAMP']
    return df


def doy_parser(year, doy, time):
    """Parse day of year from CR10X into datetime for read_csv.

    :param year: 4-digit year
    :param doy:  day of year
    :param time: time as HHMM w/o leading zeros
    :return: datetime variable

    Dependants: read_s_ec read_d_ec, read_s_met, read_d_met
    Internal Dependencies: None
    External Dependencies: datetime, time
    """

    try:
        # new stuff for post processing times like '100.'
        # print 'START ', year, doy, time

        # really need a "strip" function!
        for k, c in enumerate(str(year)):
            if c == '.':
                year = year[:k]
        for k, c in enumerate(str(doy)):
            if c == '.':
                doy = doy[:k]
        for k, c in enumerate(str(time)):
            if c == '.':
                #print 'k,c, time = ', k,c, time
                time = time[:k]
                #print 'TIME = ', time
        #print 'TIME OUTSIDE = ', time

        #year = [year[:k] if c == '.' else year for k, c in enumerate(str(year)) ]
        #print 'year=', year

        '''
        if time[-2] == '.':
            time = time[:-2]
        if year[-1] == '.':
            year = year[:-1]
        if doy[-1] == '.':
            doy = doy[:-1]
        if time[-1] == '.':
            time = time[:-1]
            #print 'END ', year, doy, time
        '''
    except TypeError:
        print 'DOY_PARSER: TypeError, doy, type:', doy, type(doy)

    t4 = time
    if len(time) < 4:
        for i in range(0, 4 - len(time)):
            t4 = '0' + t4
    hr = t4[0:2]
    minute = t4[2:5]

    # print year, doy, hr, minute

    year, doy, hr, minute = [int(x) for x in [year, doy, hr, minute]]
    if hr == 24:
        hr = 00
        doy += 1
        # non-standard minute warning print:
        #if not (minute == 30 or minute == 0):
        #print 'Minute out of range at', year, doy, hr, minute
    date = dt.datetime(year, 1, 1, hr, minute) + dt.timedelta(days=doy - 1)
    return date


# read CR10X file to get start and end times
# def read_CRtenX_file(path, fname, df_type, skip):
def read_CRtenX_file(path, fname, df_type):
    """Return dict entry of CR10X file time span.

    :param path: file path
    :param fname: file name (stored in output data)
    :param df_type: datafile type (met or ec)
    :return: DataFrame entry of begin and end date times for file.
    
    Dependants: main
    Internal Dependencies:none
    External Dependencies:pandas
    """
    dframe = pd.read_csv(path, header=None, na_values=['-', -99999, 99999, 6999, -6999],
                         warn_bad_lines=True, error_bad_lines=False,
    )

    CRtenX = pd.DataFrame({'file_name': fname,
                           'df_type': df_type,
                           'str_year': dframe[1][0],
                           'str_doy': dframe[2][0],
                           'str_time': dframe[3][0],
                           'end_year': dframe[1].irow(-1),
                           'end_doy': dframe[2].irow(-1),
                           'end_time': dframe[3].irow(-1)}, index=['1'])

    CRtenX = CRtenX[['file_name',
                     'df_type',
                     'str_time',
                     'str_doy',
                     'str_year',
                     'end_time',
                     'end_doy',
                     'end_year']]
    print 'read_CRtenX_file fname = ', fname
    return CRtenX


# read CR1000 file to get start and end timestamps
def read_CRthousand_file(f, fname):
    """Read CR1000 file time spans.

    Reads file 'f' from folder 'path' as a pandas dataframe.
    Returns first and last timestamp with filename as key.

    :param f: file path
    :param fname: file name (stored in output data)
    :return: DataFrame entry of begin and end date times for file.
    
    Dependants: main
    Internal Dependencies:none
    External Dependencies:pandas
    """

    dframe = pd.read_csv(f, skiprows=[0, 2, 3], na_values=["NAN", "INF", "-INF", 6999])
    CRthousand = pd.DataFrame({'file_name': fname,
                               'df_type': 'rad',
                               'StartTime': dframe['TIMESTAMP'][0],
                               'StopTime': dframe['TIMESTAMP'].irow(-1)}, index=['1'])
    return CRthousand


def select_dates(analysis_start, release_end, df_met, df_rad, df_ec, df_ec_all):
    # limit data to analysis date range
    # todo: refactor date selection to allow bypass of un-needed types (like df_ec_all for speed)
    print 'Selecting from ec from %s and to %s' % (analysis_start, release_end)
    df_ec = df_ec[(df_ec.index >= analysis_start) & (df_ec.index <= release_end)]
    print 'Selecting from ec_all'
    df_ec_all = df_ec_all[(df_ec_all.index >= analysis_start) & (df_ec_all.index <= release_end)]
    print 'Selecting from met'
    df_met = df_met[(df_met.index >= analysis_start) & (df_met.index <= release_end)]
    print 'Selecting from rad'
    df_rad = df_rad[(df_rad.index >= analysis_start) & (df_rad.index <= release_end)]
    print '#########################'
    return df_met, df_rad, df_ec, df_ec_all


def pad_missing_with_nulls(df_pad, dtype):
    """
    Insert blank rows for missing 30min timestamps. Used by gap and impute.

    :param df_pad: dataframe to pad
    :param dtype:  type of data in dataframe
    :return: padded dataframe df_pad
    """
    print 'Before reindex for %s in pad_missing_with_nulls:' % dtype
    min_index = min(df_pad.index)
    max_index = max(df_pad.index)
    #print 'min_index, max_index = ', min_index, max_index
    print 'Start TIMESTAMP: ', min_index, 'Stop TIMESTAMP: ', max_index
    pre_re_length = len(df_pad)
    print 'Length =', pre_re_length
    df_pad = df_pad.reindex(index=pd.date_range(min_index, max_index, freq='30min'))
    df_pad.index.name = 'TIMESTAMP'
    print 'After reindex'
    print 'Start TIMESTAMP: ', min(df_pad.index), 'Stop TIMESTAMP: ', max(df_pad.index)
    print 'Length =', len(df_pad)
    print 'Number of rows added =', len(df_pad) - pre_re_length
    return df_pad


def gap(path, df0, dtype, make_gap_plots):
    """ Catalog and plot time gaps in a time series DataFrame.

    :param df0: Time series DataFrame to be checked for gaps.
    :param dtype: Data file type (met, ec, rad)
    :param make_gap_plots: logical
    :return: df_gap - DataFrame of gap lengths and start times.
    
    Other stuff after field list
    """

    print 'In gap'
    df0 = pad_missing_with_nulls(df0, dtype)
    # build gap dict
    gap_dict = {}
    for col in df0.columns:
        print 'processing gap col = ', col
        gap_length = 0
        length_list = []
        start_list = []
        for i, c in zip(df0.index, df0[col]):
            if pd.isnull(c) and gap_length == 0:
                gap_start = i
                gap_length = 1
            elif pd.isnull(c) and gap_length != 0:
                gap_length += 1
            elif gap_length != 0:
                length_list.append(gap_length)
                start_list.append(gap_start)
                gap_length = 0
            s = pd.Series(length_list, index=start_list, name="Index")
        gap_dict[col] = s
        #print s
    print '########################'
    print "Initialize df_gap"
    df_gap = pd.DataFrame(gap_dict)
    df_gap.index.name = 'TIMESTAMP'

    print '##########################'
    print 'df_gap list:'
    print df_gap
    print 'df_gap Length', len(df_gap)
    if len(df_gap) != 0:
        print 'Gaps Found'
        # Output gaps to CSV file
        # todo: add index name to file? (maybe: name index in .DataFrame above)
        df_gap.to_csv(path + 'gap_' + dtype + '.csv')
        if make_gap_plots and len(df_gap) < 22:
            print 'Building Gap Plot'
            df_gap.plot(kind='bar')
            plt.show()
        elif make_gap_plots and dtype == 'rad':
            sp = 'NetTot_Avg'
            print "Too many gaps for complete plot"
            print 'Building Gap Sub Plot of %s' % sp
            df_gap[sp].plot(kind='bar')
            plt.show()
        elif make_gap_plots and dtype == 'ec':
            sp = 'Ux'
            print "Too many gaps for complete plot"
            print 'Building Gap Sub Plot of %s' % sp
            df_gap[sp].plot(kind='bar')
            plt.show()
        else:
            print "Too many gaps and no subplot designed."
    else:
        print 'No Gaps Found :)'
    print
    return df_gap


def gap_expand(path_out, column):
    '''
    Use gap file to build even interval time series of gap lengths
    '''

    # read data and drop cells from gaps in other variables that show up as NA
    # should test for existence of gap_met file and handle error
    df_gaps = pd.read_csv(path_out + 'gap_met.csv')  # index_col='TIMESTAMP')
    df_gaps = df_gaps[column].dropna(axis=0)

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
    s_mask.name = 'mask'
    #print 's_mask = \n', s_mask.describe()
    s_mask.to_csv(path_out + 'gap expansion.csv')
    return s_mask


def confirm_plot(info_message, prompt_message, sys_message):  # not called yet
    response = ' '  # just to make pyCharm happy.
    while True:
        print info_message
        response = raw_input(prompt_message).upper()
        if response in ['Y', 'N']:
            break
        else:
            print "Not a valid entry"
    if response != 'Y':
        sys.exit(sys_message)  # exit early to test above code
    return


def impute_master(path_out, df):
    """
    Control missing value imputation.  Calls impute_strata.

    :param path_out:
    :param df:
    :return:
    """

    #df.drop(['0'], inplace=True, axis=1)
    '''
    response = ' '  # just to make pyCharm happy.
    while True:
        prompt_message = 'Make Histograms (Y/N)? '
        response = raw_input(prompt_message).upper()
        if response in ['Y', 'N']:
            break
        else:
            print "Not a valid entry"
    if response == 'Y':
        df.hist()
        #df.plot(kind='hist')
        plt.show()

    '''
    df_day_mn = df.resample('D', how='mean')
    df_month_mn = df.resample('M', how='mean')
    # output means:
    df_day_mn.to_csv(path_out + 'df_day_mn.csv', index=True, header=True)
    df_month_mn.to_csv(path_out + 'df_month_mn.csv', index=True, header=True)
    # create year month variable (YYYYMM) in monthly and half-hour data sets to join on.
    # might have problems if entire month missing
    df_month_mn['year_month'] = pd.to_datetime(df_month_mn.index.year * 10000 + df_month_mn.index.month * 100 + 1,
                                               format='%Y%m%d')
    # reindex to year_month (might not need this) for lookup
    df_month_mn = df_month_mn.set_index(df_month_mn.year_month)

    df = pad_missing_with_nulls(df, 'met')
    # create join variable on data for lookup
    df['year_month'] = pd.to_datetime(df.index.year * 10000 + df.index.month * 100 + 1,
                                      format='%Y%m%d')
    for column in df.columns:
        if column == 'year_month':
            continue
        mask = gap_expand(path_out, column)

        '''
        # Toggle block comment to plot
        response = ' '  # just to make pyCharm happy.
        while True:
            prompt_message = 'Plot of: %s (Y/N)? ' % column
            #prompt_message = 'Histogram of: %s (Y/N)? ' % column
            response = raw_input(prompt_message).upper()
            if response in ['Y', 'N']:
                break
            else:
                print "Not a valid entry"
        if response == 'Y':
            #df[[column]].plot(kind='hist')
            df[[column]].plot()
            plt.show()
        '''
        if len(mask) == 0:
            print "Nothing to pad for column: ", column
        else:
            # Fill missing timestamps by interpolation
            # This must not happen before gap analysis.
            #df = impute_simple(path_out, df, 'met', 'True')
            print '********************************************'
            print "Calling impute_strata for column: ", column
            df = impute_strata(path_out, df, df_month_mn, column, mask)
            print "Values Imputed for column: ", column
            print '********************************************'
    df = df.join(df_month_mn, rsuffix='_mnth')
    df.to_csv(path_out + 'impute_strata_final.csv', index=True, header=True)
    return df


def impute_strata(path_out, df, df_month_mn, column, mask):
    """
    Fill one column that has missing data from a dataframe using methods based on length missing.

    :param path_out: where to write check files
    :param df: dataframe to impute
    :param df_month_mn: monthly means
    :param column: current column name
    :param mask: even interval series of missing lengths for column
    :return: dataframe
    """

    # done: bad data filters moved to loop in inventory() after calls to concat_df()
    # need time span of data for clipping after join with mask
    min_index = min(df.index)
    max_index = max(df.index)
    # outer join gap lengths to data to get even interval
    df = df.join(mask, how='outer')
    df = df[(df.index >= min_index) & (df.index <= max_index)]
    max_interpolate = 5
    # condition for small gaps (to interpolate, but done 'automatically' by pandas call)
    # condition = (0 < df['mask']) & (df['mask'] <= max_interpolate)

    # condition for medium gaps (to substitute monthly mean)
    # need to add max substitution criterion
    condition = max_interpolate < df['mask']

    df.ix[condition, column] = df_month_mn.lookup(
        df.ix[condition, 'year_month'],
        [column] * len(df.ix[condition])
    )

    df[column] = df[column].interpolate(method='time')
    df.index.name = 'TIMESTAMP'
    df.to_csv(path_out + 'impute_strata_temp.csv', index=True, header=True)
    df.drop(['mask'], axis=1, inplace=True)
    return df


def impute_simple(path_out, df, dtype, print_flag):  # Obsolete?
    """Function to impute entire missing records"

    :param df: Dataframe to impute.
    :param print_flag: Flag to indicate if diagnostic files should be printed
    :return: dataframe with imputed values and an added flag column
    """
    if print_flag:
        print "Before reindex, df.index, len(df) = ", df.index.names, len(df)
        print 'min=  ', min(df.index), 'max = ', max(df.index)
    # Need to flag interpolated values. Create Measured flag = 1 if record exists
    #   prior to reindexing.
    colname = dtype + '_Imputed'
    df[colname] = 0
    df = df.reindex(index=pd.date_range(min(df.index), max(df.index), freq='30min'))
    df.index.names = ['TIMESTAMP']
    # After reindex, if Met_Measured flag is null, then record will be interpolated to set
    #  measured flag to zero before interp.
    df.ix[(df[colname].isnull()), [colname]] = 1
    df.to_csv(path_out + 'test_reindexed.csv')
    if print_flag:
        print "After reindex, df.index, len(df) = ", df.index.names, len(df)
        print 'Interpolate works surprisingly well even over longer intervals'
    print "Verify this is still true:"
    print "Whenever solar and net_rad are NA, all wind var's are 0.  Set them to NA."
    df.ix[(df.solar.isnull()), ['wind_speed', 'wind_direction', 'wind_direction_sdv']] = np.nan
    df = df.apply(lambda x: x.interpolate(method='time'))  # limit=4))
    df.to_csv(path_out + 'test_interpolated.csv')
    return df


def s_met_calcs(path_out, df_m, df_r, df_e, station, make_calc_plots=True):
    """Do met calcs for fortran program input.

    :param path_out: Output directory relative to program folder
    :param df_m: Concatenated data logger met data over analysis interval
    :param df_r: Concatenated data logger rad data over analysis interval
    :param df_e: Concatenated data logger ec data over analysis interval
    :param station: Placeholder for eventual refactor
    :param make_calc_plots: logical plot toggle
    :return: df_out
    """
    # code to build met file for fortran input for Starkey
    # gets needed variables from three types of Campbell files
    # reads from generically named (SM, SR, & SE) files from python folder.
    # 6/27/14
    # 2015 update: now reads from and writes to path_out
    # VERY IMPORTANT:
    # note that input dataframes are modified BY REFERENCE so upon return to MAIN,
    # the modifications remain. SO:
    # NEED TO REFINE TREATMENT OF df_m, df, df_out, and df_out_all !!!!!!!!!!!!!!!!
    # perhaps use copy on df_m initially so that original df_met remains intact.
    print '#################################################'
    print 'Starting s_met_calcs'

    # Ensure even interval met data. Analysis timespan fixed to met data
    # df_m.reindex(index=pd.date_range(min(df_m.index), max(df_m.index), freq='30min'), inplace=True)
    # reindex now in main also?

    # net rad from 4-comp (all that's needed from rad so get it out of the way)
    df_r['Rn'] = (df_r.SR01Up_Avg >= 0) * df_r.SR01Up_Avg - \
                 (df_r.SR01Dn_Avg >= 0) * df_r.SR01Dn_Avg + \
                 df_r.IR01UpCo_Avg * (1 - 0.066) - \
                 df_r.IR01DnCo_Avg

    # Drop columns for speed but:
    # need minimum of 2 columns for upcoming dataframe merge
    df_r = df_r[['Rn', 'SR01Up_Avg']]

    #######################################
    # excel equation for REBS wind correction mfull_201402_template.xlsx:
    # Excel Columns:
    # AD (wind factor) =IF(AC7460<0,   (0.00174*X7460)+0.99755,     1+(0.066*0.2*X7460)/(0.066+(0.2*X7460)))
    # AC = raw_REBS_net_radiation; X = wind_speed(m/s)
    #  adjustment equation depends on sign of raw rebs
    # AE (adjusted REBS) = wind_factor * raw rebs
    # result missing if wind speed missing
    df_m['REBS'] = df_m.net_rad_1 * ((df_m.net_rad_1 < 0) * ((0.00174 * df_m.wind_speed) + 0.99755) +
                                     (df_m.net_rad_1 >= 0) *
                                     (1 + (0.066 * 0.2 * df_m.wind_speed)
                                      / (0.066 + (0.2 * df_m.wind_speed)))
    )

    df_m.to_csv(path_out + 'REBS_test.csv')
    # truncate single component pyranometer (no negative light)
    df_m['solar'] = (df_m.solar >= 0) * df_m.solar

    # Calculate G precursors
    df_m['Cp'] = 1350 * (840 + (df_m.soil_moisture4 / 1350. * 1000) * 4190)
    df_m['soil_temp_smoothed'] = pd.rolling_mean(df_m.soil_tmp4, 2)

    # Fudge initial values caused by smoothing and differencing
    # (remove this kludge after prepending earlier data!)
    df_m.soil_temp_smoothed[0] = df_m.soil_tmp4[0]  # initial smoothed equals first unsmoothed
    df_m['del_T'] = df_m.soil_temp_smoothed - df_m.soil_temp_smoothed.shift()
    df_m.del_T[0] = df_m.del_T[1]  # initial equals next
    df_m['storage'] = (0.08 / 1800) * df_m.del_T \
                      * df_m.Cp
    df_m['soil_heat_flux_average'] = (df_m.soil_heat_flux8_1 + df_m.soil_heat_flux8_2) / 2
    df_m.storage[0] = df_m.storage[1]

    # Needed to break up the below because of the initial timestep gap
    #df['storage'] = (0.08 / 1800) * (pd.rolling_mean(df.soil_tmp4, 2) - pd.rolling_mean(df.soil_tmp4, 2).shift()) \
    #    * df.Cp

    ########################
    # Merge in net rad from 4-comp radiometer file (waited until it's needed),
    # drop spacer column
    df = pd.merge(df_m, df_r, left_index=True, right_index=True, how='outer')
    # changed Dn_Avg to Up_Avg to use as backup for solar so don't drop this:
    #df.drop('SR01Dn_Avg', axis=1, inplace=True)

    #df.plot(x='logREBS', y='logRn', lw=0, marker='o', title='log(Rn+100) vs. log(REBS + 100)')
    #df.plot(y='WL', lw=0, marker='o', title='Water Level')

    # todo: make WL plot an option

    # WATER LEVEL PLOT
    print 'Making Water Level Plot'
    df.plot(y='WL', title='Water Level')
    plt.show()

    '''
    print 'm-r Merged Length = ', len(df)
    print 'm-r Merged Start TIMESTAMP: ', min(df.index), 'Stop TIMESTAMP: ', max(df.index)
    print ' '
    '''

    # Calculate G two ways depending on data availability
    df['G'] = df.soil_heat_flux_average + df.storage
    # todo: New error here?
    #df.ix[pd.isnull(df.G), 'G'] = -0.00003 * df.Rn ** 2 + 0.0854 * df.Rn - 11.5
    df['avail'] = df.Rn - df.G

    # Original outer merge commented out
    # Inner merge for intersection -> leave out times missing from both
    # should get number of records = len(df_ec) after filtering (8382)  - YES!
    #df_out = pd.merge(df, df_e, left_index=True, right_index=True, how='outer')
    # Merge in airtemp and RH from EC which also adds times in EC missing in MET
    df_e = df_e[['airtemp', 'RH']]
    #df_out = pd.merge(df, df_e, left_index=True, right_index=True, how='inner')
    df_out = pd.merge(df, df_e, left_index=True, right_index=True, how='inner')

    '''
    # Interpolate commands and tracking outputs. Need to interpolate on
    # reindexed data with all time intervals present.
    df_out['airtemp'].to_csv('ts_Stark_airDEBUG.csv', index=True, header=True)
    df_csv(df_out, 'df_Stark_airDEBUG.csv', 'DEBUG', echo=True)
    df_out['airtemp'] = df_out['airtemp'].interpolate(method='time', limit=4)
    df_csv(df_out, 'df_Stark_airDEBUG_Interpolated.csv', 'DEBUG', echo=True)
    print
    print 'Airtemp Interpolated'
    print
    df_out['avail'] = df_out['avail'].interpolate(method='time', limit=4)
    print
    print 'AVAIL Interpolated'
    '''

    '''
    print
    print '#################################'
    print '#################################'
    print 'After duplicates dropped len(df_e), = ', len(df_e)
    print 'Merged length len(df_out), = ', len(df_out)
    print '#################################'
    print '#################################'
    '''

    # Output
    '''
    print '#################################'
    df['logREBS'] = np.log10(df['REBS'] + 100)
    df['logRn'] = np.log10(df['Rn'] + 100)
    # REBS error check
    print 'Error, logREBS > 3.5 if next line not "Empty DataFrame'
    print df.ix[(df.logREBS > 3.5), :]
    print '#################################'
    '''
    # Plot to compare Rn with REBS
    if make_calc_plots:
        #df.plot(x='logREBS', y='logRn', lw=0, marker='o', title='log(Rn+100) vs. log(REBS + 100)')
        df.plot(x='REBS', y='Rn', lw=0, marker='o', title='Rn vs. REBS')
        plt.show()

    print
    print "In met_calc's df_out.columns after EC Merge, before drops"
    print df_out.columns
    print 'df_out Length = ', len(df_out)
    print 'df_out Start TIMESTAMP: ', min(df_out.index), 'Stop TIMESTAMP: ', max(df_out.index)
    print

    # Copy processed data to this point to keep drop order for fort output
    # todo: reorganize so copy not needed and column prints in met_fort() still work
    # todo: added RnUp variable so need to fix column order for fortran?
    df_out_all = df_out.copy()
    df_out.drop(
        ['0', 'soil_tmp4', 'soil_heat_flux8_1', 'soil_heat_flux8_2', 'net_rad_1', 'battery', 'wind_direction_sdv',
         'WL', 'WL_temp', 'REBS', 'Cp'], axis=1, inplace=True)

    met_fort(path_out, df_out, station)  # or call output from calling function?


    # do some more met processing (from excel qMMMDD file (like qFeb06A.xlsx in process folder for 201402))
    # but precip and DTW do not depend on fortran program so they can be done in pre-processing!
    # should redo above and adjust columns printed by met_fort() to better organize below
    df_out_all['precip_corrected'] = 1.05 * df_out_all['precip']
    df_out_all['DTW'] = 0.3048 * (48 - df_out_all['WL'])

    return df_out_all


def d_met_calcs(path_out, df_m, df_r, df_e, station, make_calc_plots=True):
    """Do met calcs for fortran program input.

    :param df_m: Concatenated data logger met data over analysis interval
    :param df_r: Concatenated data logger rad data over analysis interval
    :param df_e: Concatenated data logger ec data over analysis interval
    :param make_calc_plots: logical plot toggle
    :return: df_out
    """
    # code to build met file for fortran input for Starkey
    # gets needed variables from three types of Campbell files
    # reads from generically named (SM, SR, & SE) files from python folder.
    # 6/27/14

    # Ensure even interval met data. Analysis timespan fixed to met data
    # df_m.reindex(index=pd.date_range(min(df_m.index), max(df_m.index), freq='30min'), inplace=True)
    # reindex now in main also?
    '''
    print '#################################################'
    print "From s_met_calcs, describe df_r:"
    print df_r.describe()
    print df_r.dtypes
    print '#################################################'
    '''
    # net rad from 4-comp (all that's needed from rad so get it out of the way)
    df_r['Rn'] = (df_r.SW_in >= 0) * df_r.SW_in - \
                 (df_r.SW_out >= 0) * df_r.SW_out + \
                 df_r.LW_in - df_r.LW_out

    # Drop columns for speed but:
    # need minimum of 2 columns for upcoming dataframe merge

    df_r = df_r[['Rn', 'SW_in']]

    #######################################
    # excel equation for REBS wind correction mfull_201402_template.xlsx:
    # AD (wind factor) =IF(AC7460<0,   (0.00174*X7460)+0.99755,     1+(0.066*0.2*X7460)/(0.066+(0.2*X7460)))
    # AC = raw_REBS_net_radiation; X = wind_speed(m/s)
    #  adjustment equation depends on sign of raw rebs
    # AE (adjusted REBS) = wind_factor * raw rebs
    # Net radiometer ( todo: handle radiometer calibration by model & serial number )

    # nr lite wind speed correction:
    # L = wind speed, P = nr lite
    # =IF(L18575>5,P18575*(1+0.021286*(L18575-5)),P18575)
    df_m['NR_Lite'] = (df_m.wind_speed <= 5) * df_m.net_rad_1 + \
                      (df_m.wind_speed > 5) * df_m.net_rad_1 * (1 + 0.021286 * (df_m.wind_speed - 5))

    # truncate single component pyranometer (no negative light)
    # No Dead River pyranometer anymore :(
    #df_m['solar'] = (df_m.solar >= 0) * df_m.solar

    # Calculate G precursors
    df_m['Cp'] = 1350 * (840 + (df_m.soil_moisture / 1350. * 1000) * 4190)
    # undo soil temp smoothing for comparison
    #df_m['soil_temp_smoothed'] = pd.rolling_mean(df_m.soil_tmp, 2)
    df_m['soil_temp_smoothed'] = pd.rolling_mean(df_m.soil_tmp, 1)

    # Fudge initial values caused by smoothing and differencing
    # (remove this kludge after prepending earlier data!)
    df_m.soil_temp_smoothed[0] = df_m.soil_tmp[0]  # initial smoothed equals first unsmoothed
    df_m['del_T'] = df_m.soil_temp_smoothed - df_m.soil_temp_smoothed.shift()
    df_m.del_T[0] = df_m.del_T[1]  # initial equals next
    df_m['storage'] = (0.08 / 1800) * df_m.del_T \
                      * df_m.Cp

    df_m.storage[0] = df_m.storage[1]

    ########################
    # Merge in net rad from 4-comp radiometer file (waited until it's needed),
    df = pd.merge(df_m, df_r, left_index=True, right_index=True, how='outer')

    # If 4-comp Rn missing, update Rn from 4-comp with Rn from NR_lite from met file
    df['Rn'] = df['Rn'].combine_first(df['NR_Lite'])
    df['solar'] = df['solar'].combine_first(df['SW_in'])
    df.to_csv('DR_met_tests.csv')

    # changed Dn_Avg to Up_Avg to use as backup for solar so don't drop this:
    #df.drop('SW_out', axis=1, inplace=True)
    print 'm-r Merged Length = ', len(df)
    print 'm-r Merged Start TIMESTAMP: ', min(df.index), 'Stop TIMESTAMP: ', max(df.index)
    print ' '

    # Calculate G two ways depending on data availability
    #df['G'] = df.soil_heat_flux + df.storage
    # Calculate the incorrect way to check consistency
    df['G'] = df_m.soil_moisture + df.storage
    # todo: check G = f(Rn) equation
    #df.ix[pd.isnull(df.G), 'G'] = -0.00003 * df.Rn ** 2 + 0.0854 * df.Rn - 11.5
    df['avail'] = df.Rn - df.G

    # merge any EC column into MET just to add timesteps in EC missing from MET
    #df = pd.merge(df, pd.DataFrame(df_e['Uz']), left_index=True, right_index=True, how='outer')
    # right join gets MET rows to match EC rows.
    df = pd.merge(df, pd.DataFrame(df_e['Uz']), left_index=True, right_index=True, how='right')
    df = df.drop(['Uz'], axis=1)

    df_out = df.rename(columns={'CS215_temp': 'airtemp', 'CS215_RH': 'RH'})

    # todo: check for need for interpolation
    '''
    df_out['airtemp'] = df_out['airtemp'].interpolate(method='time', limit=4)
    print
    print 'Airtemp Interpolated'
    print
    df_out['avail'] = df_out['avail'].interpolate(method='time', limit=4)
    print
    print 'AVAIL Interpolated'
    '''
    print
    print '#################################'
    print '#################################'
    print 'Merged length len(df_out), = ', len(df_out)
    print '#################################'
    print '#################################'

    # Output
    print '#################################'
    df['logNR_Lite'] = np.log10(df['NR_Lite'] + 100)
    df['logRn'] = np.log10(df['Rn'] + 100)
    # REBS error check
    print 'Error, logNR_Lite > 3.5 if next line not "Empty DataFrame'
    print df.ix[(df.logNR_Lite > 3.5), :]
    print '#################################'
    # Plot to compare Rn with REBS
    if make_calc_plots:
        df.plot(x='logNR_Lite', y='logRn', lw=0, marker='o', title='log(Rn+100) vs. log(logNR_Lite + 100)')
        plt.show()

    print
    print "In met_calc's df_out.columns after EC Merge, before drops"
    print df_out.columns
    print 'df_out Length = ', len(df_out)
    print 'df_out Start TIMESTAMP: ', min(df_out.index), 'Stop TIMESTAMP: ', max(df_out.index)
    print

    # Copy processed data to this point to keep drop order for fort output
    # todo: reorganize so copy not needed and column prints in met_fort() still work
    # todo: added RnUp variable so need to fix column order for fortran?
    df_out_all = df_out.copy()
    df_out.drop(['0', 'soil_tmp', 'KH2O_volt', 'net_rad_1', 'battery', 'wind_direction_sdv',
                 'WL', 'WL_temp', 'NR_Lite', 'Cp'], axis=1, inplace=True)

    met_fort(path_out, df_out, station)  # or call output from calling function?


    # do some more met processing (from excel qMMMDD file (like qFeb06A.xlsx in process folder for 201402))
    # but precip and DTW do not depend on fortran program so they can be done in pre-processing!
    # should redo above and adjust columns printed by met_fort() to better organize below
    # comment out Starkey correction for now
    #df_out_all['precip_corrected'] = 1.05 * df_out_all['precip']
    df_out_all['precip_corrected'] = df_out_all['precip']
    df_out_all['DTW'] = 0.3048 * (40 - df_out_all['WL'] - 2.7)

    return df_out_all


def met_fort(path_out, df_out, station):
    """Output alldata.prn for fortran met data input.

    :param path_out: Output directory relative to program location
    :param df_out: Timeseries DataFrame of output variables
    :return: nothing
    """

    print '##################################'
    print 'In met_fort to write fortran  met prn file for station = ', station
    df_out = df_out.replace('nan', -99999)
    rec = df_out.to_records(convert_datetime64=True)

    print
    print ' In met_fort: List of tuples with records data structure and column numbers for output:'
    print[(i, rec.dtype.names[i]) for i in range(len(rec.dtype.names))]
    #print 'Year Fraction = ', toYearFraction(rec['index'][1])

    print 'Year Fraction = ', toYearFraction(rec['TIMESTAMP'][1])
    print

    '''
     dec.year   year  day  hour  rain,  soil_temp  flux moist speed direction  sw_in  Rn    avail_     G     air_temp rh
    iyear=d(2)
    day=d(3)
    time=d(4)
    xmoiss=d(7)
    xmoisd=d(8)
    wspeed=d(9)
    wdir=d(10)
    solar=d(11)
    rn=d(12)
    avail=d(13)
    g=d(14)
    airtemp=d(15)
    rh=d(16)

    '''
    # Starkey fortran input file order:
    #    1,       2,    3,    4,   5,           6,               7 ,                8,                  9,
    # dec.year, year, day, hour, rain, soil_temp_smoothed, soil_moisture_4cm, soil_moisture_20cm, wind_speed,

    #           10,               11,                12,           13,  14,        15,         16
    # wind_direction, solar_truncated, Hukeflux_Rn_Reconstructed, avail, G, airtemp_HMP45C, RH_HMP45C
    fname = 'alldata.prn'
    f = open(path_out + fname, 'w')
    if station == 's':
        f.write('dec.year  year   day   hour    rain    soil_temp  mois_4cm  mois_20cm\
 wind_speed      wind_dir      solar      Rn    avail       G       air_temp    RH\n')
        for i in range(len(rec[rec.dtype.names[1]])):
            f.write('{:11.6f} {:4d} {:2d} {:02d}{:02d}\
     {:8.1f} {:9.2f} {:10.3f} {:10.3f} {:10.3f} {:8.1f} {:9.2f} {:10.3f} {:10.3f} {:10.3f} {:10.3f} {:10.3f}\n'.format(
                toYearFraction(rec['TIMESTAMP'][i]),
                rec['TIMESTAMP'][i].timetuple().tm_year,  # year
                rec['TIMESTAMP'][i].timetuple().tm_yday,  # day
                rec['TIMESTAMP'][i].timetuple().tm_hour,  # hour
                rec['TIMESTAMP'][i].timetuple().tm_min,  # minute
                rec[rec.dtype.names[2]][i], rec[rec.dtype.names[9]][i],  # rain soil_temp_smoothed
                rec[rec.dtype.names[3]][i], rec[rec.dtype.names[4]][i],  # soil_moisture_4cm, soil_moisture_20cm,
                rec[rec.dtype.names[5]][i], rec[rec.dtype.names[6]][i],  # wind_speed, wind_direction
                rec[rec.dtype.names[7]][i], rec[rec.dtype.names[13]][i],  # solar, Rn,
                rec[rec.dtype.names[16]][i], rec[rec.dtype.names[15]][i],  # avail, G,
                rec[rec.dtype.names[17]][i], rec[rec.dtype.names[18]][i])  # airtemp_HMP45C, RH_HMP45C
            )
    else:
        # Original Dead River input file:
        #  dummy   year  day  hour  rainfall,  soil_temp  heat_flux moisture wind_speed wind_direct  solar_rad
        # net_rad    avail     G    air_temperature    rh
        f.write('dec.year   year  day  hour       rain     soil_temp    flux      moist      speed   direction\
  solar       Rn        avail       G      air_temp      rh\n')
        for i in range(len(rec[rec.dtype.names[1]])):
            f.write('{:11.6f} {:4d} {:2d} {:02d}{:02d}\
     {:8.2f} {:9.2f} {:10.3f} {:10.3f} {:10.3f} {:8.1f} {:9.2f} {:10.3f} {:10.3f} {:10.3f} {:10.3f} {:10.3f}\n'.format(
                toYearFraction(rec['TIMESTAMP'][i]),
                rec['TIMESTAMP'][i].timetuple().tm_year,
                rec['TIMESTAMP'][i].timetuple().tm_yday,
                rec['TIMESTAMP'][i].timetuple().tm_hour,
                rec['TIMESTAMP'][i].timetuple().tm_min,
                rec[rec.dtype.names[2]][i], rec[rec.dtype.names[10]][i],
                rec[rec.dtype.names[4]][i], rec[rec.dtype.names[3]][i],
                rec[rec.dtype.names[6]][i], rec[rec.dtype.names[7]][i],
                rec[rec.dtype.names[5]][i], rec[rec.dtype.names[13]][i],
                rec[rec.dtype.names[16]][i], rec[rec.dtype.names[15]][i],
                rec[rec.dtype.names[8]][i], rec[rec.dtype.names[9]][i])
            )
    f.close()

    print '%s  PRN file closed' % (path_out + fname)
    print '##################################'
    return


def ec_fort(df_ec):
    """Output ec data to fortran input file.

    :param df_ec: DataFrame of ec data for fortran input
    :return: Nothing, writes text file alldata.dat for fortran input
    """

    # write ec data to np.records for control of output format
    rec = df_ec.to_records(convert_datetime64=True)
    print 'df_ec.columns = ', df_ec.columns
    print
    print ' In ec_fort: # rows = len(rec[rec.dtype.names[1]]) = ', len(rec[rec.dtype.names[1]])
    print ' In ec_fort: # columns = len(rec[rec.dtype.names]) = ', len(rec[0])
    print
    print ' In ec_fort: List of tuples with records data structure and column numbers for output:'
    print[(i, rec.dtype.names[i]) for i in range(len(rec.dtype.names))]
    print 'Year Fraction = ', toYearFraction(rec['TIMESTAMP'][1])
    print

    # boring output name for now (input for fortran):
    fname = 'alldata.dat'
    f = open(fname, 'w')
    for i in range(len(rec[rec.dtype.names[1]])):
        print 'i = ', i
        # need 42 columns
        #    f.write('{:3d},{:4d},{:3d},{:4d},{:11.5f},{:11.5f},{:11.5f},{:11.5f}'.format(
        #    f.write('{:3d},{:4},{},{},{:5.5f},{:6.5f},{:7.5f},{:5.5f}\n'.format(
        f.write('{:3d},{:4},{},{},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},\
    {:.5f},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},\
    {:.5f},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},\
    {:.5f},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},{:.5f},\
    {:.5f},{:.5f}\n'.format(
            rec[rec.dtype.names[1]][i], rec[rec.dtype.names[2]][i],
            rec[rec.dtype.names[3]][i], rec[rec.dtype.names[4]][i],
            rec[rec.dtype.names[5]][i], rec[rec.dtype.names[6]][i],
            rec[rec.dtype.names[7]][i], rec[rec.dtype.names[8]][i],
            rec[rec.dtype.names[9]][i], rec[rec.dtype.names[10]][i],

            rec[rec.dtype.names[11]][i], rec[rec.dtype.names[12]][i],
            rec[rec.dtype.names[13]][i], rec[rec.dtype.names[14]][i],
            rec[rec.dtype.names[15]][i], rec[rec.dtype.names[16]][i],
            rec[rec.dtype.names[17]][i], rec[rec.dtype.names[18]][i],
            rec[rec.dtype.names[19]][i], rec[rec.dtype.names[20]][i],

            rec[rec.dtype.names[21]][i], rec[rec.dtype.names[22]][i],
            rec[rec.dtype.names[23]][i], rec[rec.dtype.names[24]][i],
            rec[rec.dtype.names[25]][i], rec[rec.dtype.names[26]][i],
            rec[rec.dtype.names[27]][i], rec[rec.dtype.names[28]][i],
            rec[rec.dtype.names[29]][i], rec[rec.dtype.names[30]][i],

            rec[rec.dtype.names[31]][i], rec[rec.dtype.names[32]][i],
            rec[rec.dtype.names[33]][i], rec[rec.dtype.names[34]][i],
            rec[rec.dtype.names[35]][i], rec[rec.dtype.names[36]][i],
            rec[rec.dtype.names[37]][i], rec[rec.dtype.names[38]][i],
            rec[rec.dtype.names[39]][i], rec[rec.dtype.names[40]][i],

            rec[rec.dtype.names[41]][i], rec[rec.dtype.names[42]][i]))
    f.close()


# function to get year fraction from a datetime (gotta love the internet)
# http://stackoverflow.com/questions/6451655/python-how-to-convert-datetime-dates-to-decimal-years
def toYearFraction(date):
    """ Get decimal year from a scalar python datetime variable.

    :param date: datetime variable
    :return: date as year fraction

    http://stackoverflow.com/questions/6451655/python-how-to-convert-datetime-dates-to-decimal-years
    Dependant: write_met_fort()
    External Dependencies: datetime, time
    """

    def sinceEpoch(date):  # returns seconds since epoch
        return time.mktime(date.timetuple())

    s = sinceEpoch
    year = date.year
    startOfThisYear = dt(year=year, month=1, day=1)
    startOfNextYear = dt(year=year + 1, month=1, day=1)
    yearElapsed = s(date) - s(startOfThisYear)
    yearDuration = s(startOfNextYear) - s(startOfThisYear)
    fraction = yearElapsed / yearDuration
    return date.year + fraction


def df_csv(df_v, fname, df_type, df_name='df', echo=False, index=True, header=True):
    """ Output a DataFrame to csv file with option for feedback.

    :rtype : object
    :param df_v: DataFrame to write
    :param fname: Filename (path) to write to.
    :param df_type: Data type (met, ec, or rad)
    :param df_name: Name to pass to echo_description
    :param echo: Logical to print description to console
    :param index: Logical to include df index in file (False for raw (all) ec data)
    :param header: Logical to include header in file (False for raw (all) ec data)
    :return: Nothing
    """
    print 'Writing %s to file %s in df_csv:' % (df_name, fname)
    print 'echo, index = ', echo, index
    df_v.to_csv(fname, index=index, header=header)  #, date_format='%#m/%#d/%Y %#H:%M')
    if echo:
        echo_description(df_v, fname, df_type, df_name)


def echo_description(df_v, fname='Dataframe', df_type='default', df_name='df'):
    """ Prints custom timeseries dataframe description with lengths and index names.
    """
    print
    print '#################################################'
    print 'Description of %s  %s type named %s:' % (fname, df_type, df_name)
    print df_v.describe()
    print
    print '%s Length = ' % df_type, len(df_v)
    print '%s Start TIMESTAMP: ' % df_type, min(df_v.index), 'Stop TIMESTAMP: ', max(df_v.index)
    print
    print '%s.index' % df_name
    print df_v.index
    print
    print '%s.columns' % df_name
    print df_v.columns
    print '#################################################'
    print ' '


############################################
############
# Orphan code functions
############
def df_dat(df_v, fname):
    """ This doesn't do anything yet.
    """
    # this one a stub for above
    t4 = str(df_v.iloc[:, 3])
    if len(t4) < 4:
        for i in range(0, 4 - len(t4)):
            t4 = '0' + t4
            print '############ t4 #############'
            print t4
    df_v.iloc[:, 3] = str(t4)

    df_v.to_csv(fname, float_format='%10.4f', columns=[i for i in range(40)], header=False, index=False)
    print 'After df_dat'


def missnaec(df):
    """ Replace values out of bounds with missing (should count them).

    :param df: Input DataFrame
    :return: df - Output DataFrame
    
    Immediately obsolete but structure may be useful again:
    """

    df.ix[(df.Uz == 99999) | (df.Uz == -99999), 'Uz'] = np.nan
    df.ix[(df.Ux == 99999) | (df.Ux == -99999), 'Ux'] = np.nan
    df.ix[(df.Uy == 99999) | (df.Uy == -99999), 'Uy'] = np.nan
    df.ix[(df.LE == 99999) | (df.LE == -99999), 'LE'] = np.nan
    df.ix[(df.hygro_volt == 99999) | (df.hygro_volt == -99999), 'hygro_volt'] = np.nan
    return df
