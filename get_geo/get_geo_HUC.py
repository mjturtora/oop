#!/usr/bin/pythonl
#
# get_geo.py - gets County, state, and HUC code for a point entered by user
#    in decimal latitude/longitude.
#
# Depends on Google and USGS webservices and EPA Surf Your Watershed
# webpage.  Internet connection required.
#
# Programmer: Michael Turtora
# First Version: 5/21/12
#
from Tkinter import *

class Application(Frame):
# Class for GUI Application
    def createWidgets(self):
    # GUI widget definition
        self.location = Label(self,  \
                text = 'Enter Decimal lat/lon \n Separated by a comma')
        self.location.pack()
        # lat/lon entry text box
        self.e = Entry(self, width=25)
        self.e.focus_set()
        self.e.pack()
        self.b = Button(self, text="Get", width=10, command=self.get_geo)
        self.b.pack()
        self.cnty_lab = Label(self, text='County State, Equivalent or Error')
        self.cnty_lab.pack()
        # County/state output text box (selectable to permit copy/paste)
        self.result = StringVar()
        self.t = Entry(self, width=50, textvariable=self.result)
        self.t.pack()
        self.huc_lab = Label(self, text='HUC Code')
        self.huc_lab.pack()
        self.huc_var = StringVar()
        self.t_huc = Entry(self, width = 60, textvariable = self.huc_var) 
        self.t_huc.pack()

    def get_geo(self,*args):
    # get_geo is called by <key-Return> event 
    # in class _init_ to control lookup
        latlng = self.contents.get().replace(' ', '') 

        # get entry from gui to latlng text variable
        # do conversions for after format check
        if self.is_number(latlng[:latlng.find(',')]) \
          and self.is_number(latlng[latlng.find(',')+1:]):
            # separate numbers for conversion to ddmmss.s
            # for pointinhuc webservice
            dec_lat = float(latlng[:latlng.find(',')])
            dec_lon = float(latlng[latlng.find(',')+1:])
            # call conversion for lat and lon from decimal to ddmmss
            dms_lat = geo.convert_dec_lat_lon(g, dec_lat, 'lat')
            dms_lon = geo.convert_dec_lat_lon(g, dec_lon, 'lon')
            # call google map reverse geocode function and 
            # set result textvariable
            self.cnty_state = geo.geocode(g,latlng,sensor="false")
            self.t["textvariable"] = self.result
            self.result.set(self.cnty_state)
            # call usgs/epa pointinhuc function and set result textvariable
            self.huc = geo.huccode(g, dms_lat, dms_lon)
            self.t_huc["textvariable"] = self.huc_var
            self.huc_var.set(self.huc)
        else:
            # bad format entered, replace entry with repeat instruction hint
            self.contents.set("Enter: dec.lat,dec.lon")

    def is_number(self, s):
    # try entry string format for number (floatability)
        try:
            float(s)
            return True
        except ValueError:
            return False
        
    def __init__(self, master=None):
        # initialize gui application class with
        # frame and call widget functions
        Frame.__init__(self, master)
        root.title("Turtora's Get Geo")
        self.pack()
        self.createWidgets()
        # set initial prompt contents and key-return event behavior
        self.contents = StringVar()
        self.contents.set("dec.lat,dec.lon")
        self.e["textvariable"] = self.contents
        self.e.bind('<Key-Return>',self.get_geo)

class geo():
# geo class contains most of the reverse geocoding functions
    import simplejson, urllib
    # import packages needed by class
    def __init__(self, master=None):
    # initialize class (not sure if needed)
    # next import just to have something here
        import simplejson, urllib  

        
    def geocode(self,latlng ,sensor, **geo_args):
    # geocode function uses google map api and webservice to get 
    # county and state of location
        GEOCODE_BASE_URL = \
            'http://maps.googleapis.com/maps/api/geocode/json'
        geo_args.update({         
                         'latlng': latlng,         
                         'sensor': sensor       
                         })      
        url = GEOCODE_BASE_URL + '?' + geo.urllib.urlencode(geo_args)
        result = geo.simplejson.load(geo.urllib.urlopen(url))
        types = geo.simplejson.dumps([s['address_components'] for \
                                      s in result['results']], indent=2) 
        result2 = geo.simplejson.loads(types)
        # ugly code to get desired values from deeply nested json structure
        # returned by google
        if result2:
        # if google returned data, find values
            result = result2[0]
            cnty_state = ''
            for r in result:
                if 'types' in r:
                    if 'administrative_area_level_2' in r['types']:
                        cnty_state = cnty_state + r['long_name']
                    if 'administrative_area_level_1' in r['types']:
                        cnty_state = cnty_state + ' ' + r['long_name']
        
        else:
        # no google data implies user request error, either no country or
        # number not lat/lon should add url tries so if websites
        # down get different errors
            cnty_state = "You're all wet!!!"
        if not cnty_state:
            cnty_state = 'Apolitical Blues'
        return cnty_state 

    def huccode(self, dms_lat,dms_lon, **huc_args):
    # huccode function uses usgs webservice to get epa page for watershed
        HUC_BASE_URL = 'http://water.usgs.gov/cgi-bin/pointinhuc.pl'
        huc_args.update({         
                         'latitude': dms_lat,         
                         'longitude': dms_lon       
                         }) 
        huc_url = HUC_BASE_URL + '?' + geo.urllib.urlencode(huc_args) 
        page =  geo.urllib.urlopen(huc_url).read()
		# find HUC number from page
        index = page.find('USGS Cataloging Unit:')
        huc_num = page[index:index+35]
        num_index = huc_num.find(':')
		# find HUC name from page
        index = page.find('Watershed Name:')
        huc_name = page[index:index+80]
        index = huc_name.find(':')
        return huc_num[num_index+2:num_index+10] + '  ' + huc_name[index+2:huc_name.find('<br>')]
    
    
    def convert_dms(self, d,m,s):
    # convert dms lat/lon format to decimal (not used yet)
        dec_sec = s / 60. ** 2
        dec_m = m / 60.
        if d >= 0:
            dec_d = d + dec_m + dec_sec
        else:
            dec_d = d - dec_m - dec_sec
        return dec_d
    
    def convert_dec_tude(self, decimal):
    # convert decimal lat/lon to dms (used for usgs pointinhuc service)
    # called by convert_dec_lat_lon()
        degrees = int(decimal)
        frac = decimal - degrees
        
        minutes = int(60 * frac)
        m_str = str(minutes)
        if len(m_str) == 1:
            m_str = '0' + m_str
            
        seconds = int((frac * 3600) % 60)
        s_str = str(seconds)
        if len(s_str) == 1:
            s_str = '0' + s_str
        
        dms = str(degrees) + m_str + s_str  
        return dms
    
    def convert_dec_lat_lon(self, decimal, tude):
    # setup conversion for lat or lon, deal with negatives and text suffixes
    # takes lat/lon number and character flag indicating type of 'tude
    # called by get_geo()
        if tude == 'lat':
            if decimal < 0:
                char = 'S'
            else: char = 'N'
        else:
            if decimal < 0:
                char = 'W'
            else: char = 'E'
        return self.convert_dec_tude(abs(decimal)) + char
        # calls convert_dec_tude() to do the arithmetic

##########################
if __name__ == '__main__':     
    g = geo()
    root = Tk()
    app = Application(master=root)
    app.mainloop()

#    geo.geocode(g,app.contents.get(),sensor="false")
# 50.4500000,30.5233333333 
#39.9833333333,16.3
#21.30889,-157.826111111
