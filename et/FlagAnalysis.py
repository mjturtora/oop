

def get_colors(num_colors):
    import numpy as np
    import colorsys
    colors=[]
    for i in np.arange(0., 360., 360. / num_colors):
        hue = i/360.
        lightness = (50 + np.random.rand() * 10)/100.
        saturation = (90 + np.random.rand() * 10)/100.
        colors.append(colorsys.hls_to_rgb(hue, lightness, saturation))
    return colors


def get_df_from_file(path_name):
    return pd.read_csv(path_name, header=0, parse_dates=[0], na_values=[' '])


def refactor_time(df):
    df['date'] = pd.to_datetime(pd.DatetimeIndex(df['datetime']).date)
    df['Hour'] = pd.DatetimeIndex(df['datetime']).hour
    df['Minute'] = pd.DatetimeIndex(df['datetime']).minute
    df['hm'] = 100 * df['Hour'] + df['Minute']
    df.drop(['datetime', 'Hour', 'Minute'], axis=1, inplace=True)
    return


def save_to_csv(df, path_name):
    df.to_csv(path_name)
    return


def make_plot(df_pivt, number_of_plot_colors, plot_title):

    colors = get_colors(number_of_plot_colors)
    plt.rc('axes', color_cycle=colors)
    df_pivt.plot(linewidth=1, title=plot_title, legend=False)
    plt.xlabel('Time of day')
    plt.ylabel('Number of filled measurements')
    #plt.legend(loc=6, ncol=2, prop={'size': 7})
    plt.show()


if __name__ == '__main__':
    __author__ = 'mturtora'
    import pandas as pd
    import matplotlib.pyplot as plt

    # Runtime variables:
    path = '..\\..\\Starkey\\process\\201505\\'
    # 120 colors for one cycle, 12 for monthly.
    '''
    number_of_plot_colors = 12
    pivot_column_name = 'ET'
    #plot_title = 'Total Half-hour ET Each Month'
    # comment out plot title for MS/Word import
    plot_title = ' '

    number_of_plot_colors = 120
    pivot_column_name = 'DaytimeFlag'
    plot_title = 'Number of Daytime Flags Each Month'
    '''

    number_of_plot_colors = 120
    pivot_column_name = 'MeasureFlag'
    #plot_title = 'Number of ET Measurements Filled'
    plot_title = ' '

    df = get_df_from_file(path + 'FlagAnalysis.csv')
    # half hour measurements but ET values based on day
    df['ET'] /= 48.

    # Need to split datetime to get date index and column of timestamps for pivot
    refactor_time(df)
    # pivot gets hour/minutes as columns
    df_piv = df.pivot(index='date', columns='hm', values=pivot_column_name)
    save_to_csv(df_piv, path + pivot_column_name + '_Daily.csv')

    # Need all cells numeric for monthly sum
    df_piv = df_piv.fillna(0)
    df_piv = df_piv.resample('M', how='sum')
    print df_piv.describe()
    # save with year/month rows and time columns
    save_to_csv(df_piv, path + pivot_column_name + '_Monthly.csv')

    # transpose to plot line for each month with times on x-axis
    df_pivt = df_piv.T
    make_plot(df_pivt, number_of_plot_colors, plot_title)
