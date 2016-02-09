

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

    """ Extracts day fraction and year

    :param df:
    :return:
    """
    df['dates'] = pd.DatetimeIndex(df['datetime']).date
    df['hours'] = pd.DatetimeIndex(df['datetime']).hour
    df['minutes'] = pd.DatetimeIndex(df['datetime']).minute
    df['times'] = (df['hours'] + df['minutes'] / 60.)
    return df.drop(['datetime', 'hours', 'minutes'], axis=1)


def save_to_csv(df, path_name):
    df.to_csv(path_name)
    return

def relabel_axis(axis, plt):
    """ Don't like axis format for (hours, years) so redo

    :param axis: string, 'x' or 'y'
    :param plt: imshow plot object
    :return: x or y ticks vectors of locs and labels
    """

    # Start with auto-generated tick locations and labels and
    # use locations as basis for modified labels
    if axis == 'y':
        locs, labels = plt.yticks()
    else:
        locs, labels = plt.xticks()

    locs = map(int, locs)
    strings = map(str, locs)
    strings[-1] = ' '  # dropping first and last axis labels looks better
    strings[0] = ' '
    return locs, strings


def make_contour(df, df_piv):
    """ Contour plot of date and time data.  Could improve by combining df and df_piv

    :param df: dataframe with date and time vectors
    :param df_piv: dataframe with array data
    :return: nothing, plots to screen
    """
    import numpy as np
    import et_util as et
    print 'Making Contour'
    Ydt = df['dates']
    Yfrac = Ydt.apply(et.toYearFraction).unique()
    Xtm = df['times'].unique()
    Z = df_piv.values
    axes_shape = (len(Yfrac), len(Xtm))
    print
    print 'axes_shape = ', axes_shape
    print 'Array shape = ', Z.shape
    print
    assert (axes_shape == Z.shape), \
        "Contour array shape error, axes= %s, array = %s " % (axes_shape, Z.shape)
    Xi, Yi = np.meshgrid(Xtm, Yfrac)
    #print df_piv.iloc[:, 0:5].describe()
    print 'Xi.min() = ', Xi.min(), 'Xi.max() = ', Xi.max()
    print 'Yi.min() = ', Yi.min(), 'Yi.max() = ', Yi.max()
    #CS = plt.contourf(Xi, Yi, Z, alpha=0.7)  #, cmap=plt.cm.jet)#
    CS = plt.imshow(Z, aspect='auto', interpolation='none', origin='lower',
                    extent=(Xi.min(), Xi.max(), Yi.min(), Yi.max()),
                    #cmap=plt.cm.Blues)
                    #cmap=plt.cm.binary)
                    cmap=plt.cm.Oranges)
                    #cmap=plt.cm.jet)
    cbar = plt.colorbar(CS)

    # custom axes relabels:
    locs, labels = relabel_axis('y', plt)
    plt.yticks(locs, labels)
    locs, labels = relabel_axis('x', plt)
    plt.xticks(locs, labels)

    # plot annotation
    #plt.title('ET')
    plt.title(' ')
    plt.xlabel('Hour of day')
    plt.ylabel('Year')
    plt.show()
    print 'after show'
    return


if __name__ == '__main__':
    __author__ = 'mturtora'
    import pandas as pd
    import matplotlib.pyplot as plt

    # Runtime variables:
    path = '..\\..\\Starkey\\process\\201505\\'
    pivot_column_name = 'ET'
    df = get_df_from_file(path + 'FlagAnalysis.csv')
    df = df.drop(['MeasureFlag', 'DaytimeFlag'], axis=1)
    # half hour measurements but ET values based on day
    df['ET'] /= 48.

    # Need to split datetime to get date index and column of timestamps for pivot
    refactor_time(df)
    # pivot gets hour/minutes as columns
    df_piv = df.pivot(index='dates', columns='times', values=pivot_column_name)
    save_to_csv(df_piv, path + pivot_column_name + '_Daily.csv')

    # Need all cells numeric for monthly sum
    df_piv = df_piv.fillna(0)
    #df_piv = df_piv.resample('M', how='sum')

    # transpose to plot line for each month with times on x-axis
    #df_pivt = df_piv.T

    make_contour(df, df_piv)