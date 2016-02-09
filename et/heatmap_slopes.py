__author__ = 'mturtora'
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import statsmodels.formula.api as smf
import et_util as et

def read_file(path):
    dataframe = pd.read_csv(path, index_col='Index')
    return dataframe

if __name__ == '__main__':
    pd.set_option('display.width', 170)
    path = 'I:\\ALL\\TampaET\\Starkey\\process\\201505\\'
    df = read_file(path + 'slopes.csv')
    df = df.ix[df.Year == 2013]

    print df.describe()

    x = df.Begin.values
    y = df.Slope.values
    custom_colors = df.End.values

    plt.scatter(x=x, y=y, c=custom_colors, marker='+', cmap=plt.cm.Oranges)
    #plt.markers.MarkerStyle().markers('pixel')
    plt.colorbar()
    #df.plot(x="Begin", y="Slope", c=custom_colors, linestyle='None', marker=',', cmap=plt.cm.Oranges)
    #plt.colors("")

    '''
    df_piv = df.pivot(index='End', columns='Begin', values='Slope')
    Z = df_piv.values
    begin = df['Begin'].unique()
    end = df['End'].unique()
    Xi, Yi = np.meshgrid(begin, end)
    CS = plt.imshow(Z, aspect='auto', interpolation='none', origin='lower',
                    extent=(Xi.min(), Xi.max(), Yi.min(), Yi.max()),
                    #cmap=plt.cm.Blues)
                    #cmap=plt.cm.binary)
                    cmap=plt.cm.Oranges)
    cbar = plt.colorbar(CS)
    '''

    plt.title('2013 Optimum Growing Season Determination \n using slope of ET vs. xpt')
    #plt.title('2014 Optimum Growing Season Determination \n using slope of ET vs. xpt')
    plt.xlabel('Begin day of season')
    #plt.ylabel('End day of season')
    plt.ylabel('Slope colored by end date')
    plt.show()
