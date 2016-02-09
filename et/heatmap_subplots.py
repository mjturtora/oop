__author__ = 'mturtora'
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import statsmodels.formula.api as smf
import et_util as et

def read_file(path):
    dataframe = pd.read_csv(path, index_col='Index')
    return dataframe

def get_xyc(df):
    x = df.Begin.values
    y = df.Slope.values
    c = df.End.values
    return (x, y, c)



if __name__ == '__main__':
    pd.set_option('display.width', 170)
    path = 'I:\\ALL\\TampaET\\Starkey\\process\\201505\\'
    df = read_file(path + 'slopes.csv')
    print df.describe()
    df_13 = df.ix[df.Year == 2013]
    df_14 = df.ix[df.Year == 2014]

    plt.figure(1)
    plt.suptitle('Figure A-7: Optimum Growing Season Determination using \n slope of ET vs. xpt')


    # 2013 Heatmap
    plt.subplot(2, 2, 1)
    plt.title('2013')
    df_piv = df_13.pivot(index='End', columns='Begin', values='Slope')
    Z = df_piv.values
    begin = df_13['Begin'].unique()
    end = df_13['End'].unique()
    Xi, Yi = np.meshgrid(begin, end)
    CS = plt.imshow(Z, aspect='auto', interpolation='none', origin='lower',
                    extent=(Xi.min(), Xi.max(), Yi.min(), Yi.max()),
                    #cmap=plt.cm.Blues)
                    #cmap=plt.cm.binary)
                    cmap=plt.cm.Oranges)
    cbar = plt.colorbar(CS)
    plt.text(250, 100, '(a)')
    plt.ylabel('End date')

    # 2014 Heatmap
    plt.subplot(2, 2, 2)
    plt.title('2014')
    df_piv = df_14.pivot(index='End', columns='Begin', values='Slope')
    Z = df_piv.values
    begin = df_14['Begin'].unique()
    end = df_14['End'].unique()
    Xi, Yi = np.meshgrid(begin, end)
    CS = plt.imshow(Z, aspect='auto', interpolation='none', origin='lower',
                    extent=(Xi.min(), Xi.max(), Yi.min(), Yi.max()),
                    #cmap=plt.cm.Blues)
                    #cmap=plt.cm.binary)
                    cmap=plt.cm.Oranges)
    cbar = plt.colorbar(CS)
    plt.text(240, 65, '(b)')

    # 2013 slope by begin date
    x, y, custom_colors = get_xyc(df_13)
    plt.subplot(2, 2, 3)
    plt.scatter(x=x, y=y, c=custom_colors, marker='+', cmap=plt.cm.Oranges)
    plt.colorbar()
    plt.text(250, .2, '(c)')

    plt.xlabel('Begin day of season')
    plt.ylabel('Slope colored by end date')

    # 2014 slope by begin date
    x, y, custom_colors = get_xyc(df_14)
    plt.subplot(2, 2, 4)
    plt.scatter(x=x, y=y, c=custom_colors, marker='+', cmap=plt.cm.Oranges)
    plt.colorbar()
    plt.text(250, .3, '(d)')

    plt.xlabel('Begin day of season')
    plt.show()
