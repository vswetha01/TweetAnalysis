import pandas as pd
from datetime import datetime
import csv
from geopy.geocoders import Nominatim
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
from collections import Counter

path = "/MSAN_USF/courses_spring/625_Practicum/data/tweet/"
fn=path + "pinkeye_clean.csv"
fn1 = path+"pinkeye_year_loc.csv"
fn2 = path+"pinkeye_lat_lon.csv"

def read_year_location_data_from_file():
    year_address = []
    with open(fn,'rb') as csvfile:
        rd = csv.reader(csvfile)
        rd.next()

        for row in rd:
            date_year_value = datetime.strptime(row[2], "%m/%d/%Y").year
            location_value = row[7].strip()
            location_value = location_value.encode('ascii', 'ignore').decode('ascii')
            if location_value:
                value = date_year_value, location_value
                year_address.append(value)
    return year_address


def read_data_from_file():
    year_address = []
    locations = []

    with open(fn,'rb') as csvfile:
        rd = csv.reader(csvfile)
        rd.next()

        for row in rd:
            date_year_value = datetime.strptime(row[2], "%m/%d/%Y").year
            location_value = row[7].strip()
            location_value  = location_value.encode('ascii', 'ignore').decode('ascii')
            if location_value:
                value = date_year_value, location_value#row[1],row[5],row[4],
                locations.append(location_value.encode('utf-8'))
                # row1 = guid, row4 = tweet_text, row5=name_of_tweeter
                year_address.append(value)

    ctr_locations = set(locations)
    print (ctr_locations)
    # with open(fn1, 'wb') as csvwrite:
    #     wr = csv.writer(csvwrite)
    #     for item in year_address:
    #         wr.writerow(item)

    return ctr_locations


def write_to_file(filename, data):
    with open(filename, 'wb') as csvwrite:
        wr = csv.writer(csvwrite)
        for item in data:
            wr.writerow(item)
    print("Done wirting %s",filename)
    return


def getLonLatFromAddress(data):
    lats = []
    lons = []
    geolocator = Nominatim()
    print("Creating lat and lon")
    i =0

    for j in range(len(data)):
        try:
            location = geolocator.geocode(data[j])
            # print i
            if location is not None:
                lons.append(location.longitude); lats.append(location.latitude)
            if location is None:
                print data[j]
                lons.append(None); lats.append(None)
            i +=1
        except Exception as e:
            print e, i, j
            geolocator = Nominatim()
            location = geolocator.geocode(data[j])
            print e, i, j, location
            j = i
            continue

    return lons, lats


def create_lat_lon_files():
    locations_ctr = read_data_from_file()
    ctr_rng = range(0,len(locations_ctr),100)
    print ctr_rng
    elements = list(locations_ctr)
    # for i in range(len(ctr_rng)):

    start = 0
    stop = 100
    # if i < len(ctr_rng)-1:
    #     stop = ctr_rng[i+1]
    # elif i == (len(ctr_rng)-1):
    #     stop = len(elements)
    # j = start

    lon_ls, lat_ls = getLonLatFromAddress(elements[start:stop])
    final_ls = zip(elements[start:stop], lon_ls,lat_ls)
    filename = "f1_"+ str(start)+"_"+ str(stop)
    write_to_file(filename, final_ls)
# create_lat_lon_files()
def combine_all_date():
    filenames = ["f1_0_100","f1_100_200.csv","f1_200_300.csv","f1_300_400", "f1_400_500","f1_500_600", "f1_600_700", "f1_700_800", "f1_800_900","f1_900_1000","f1_1000_1100", "f1_1100_1200", "f1_1200_1300", "f1_1301_1400", "f1_1400_1500","f1_1500_1600", "f1_1600_1700", "f1_1700_1795"]

    list_address = []
    for file_name in filenames:

        with open(file_name, "rb") as csvread_file:
            rdr = csv.reader(csvread_file)
            # dt = list(rdr)
            # list_address.append(dt)
            for row in rdr:

                value = row[0], row[1], row[2]
                list_address.append(value)
        csvread_file.close()

    print len(list_address)
    write_to_file("all_lon_lat.csv", list_address)

def get_marker_color(year_data):
    str_color = ''
    lbl = ''
    if year_data == 2012:
        str_color = 'go'
        lbl = "2012"
    elif year_data == 2013:
        str_color = 'yo'
        lbl = "2013"
    elif year_data == 2014:
        str_color = 'ro'
        lbl = "2014"
    elif year_data == 2015:
        str_color = 'bo'
        lbl ="2015"
    return str_color, lbl

# read each file with city names
# combine to a dictionary city name
address_lon_lat_dict = {}
with open("all_lon_lat.csv", "rb") as csvread_file:
    rdr = csv.reader(csvread_file)
    # dt = list(rdr)
    # list_address.append(dt)
    index =0
    for row in rdr:
        try:
            if row[1].strip():
                address_lon_lat_dict[row[0]] = (float(row[1].strip()), float(row[2].strip()))
            index += 1
        except ValueError as e:
            print 'Line {i} is corrupt!'.format(i = index)
            continue


csvread_file.close()

new_addr_lon_lat_list = list()
years = []
locs = []
lons = []
lats =[]
address_year_list = read_year_location_data_from_file()
for item in address_year_list:
    loc = item[1]

    if loc in address_lon_lat_dict:
        val = address_lon_lat_dict[loc]
        lons.append(val[0])
        lats.append(val[1])
        years.append(item[0])
        locs.append(item[1])
c = Counter(years)
# print c

    # else:
    #     lons.append(None)
    #     lats.append(None)
# print len(lons), len(years)
#m = Basemap(projection='mill', llcrnrlat=-90, urcrnrlat=90, llcrnrlon=-180, urcrnrlon=180, resolution='c')

#m = Basemap(projection='mill', llcrnrlat=-60, urcrnrlat=90, llcrnrlon=-180, urcrnrlon=180, resolution='c')

m = Basemap(projection='mill', llcrnrlat=20, urcrnrlat=50, llcrnrlon=-130, urcrnrlon=-60, resolution='c')
#
# # c crude , l -low, h - high
m.drawcoastlines()
m.drawcountries()
m.drawstates()
#m.drawrivers()
m.fillcontinents(color='beige', lake_color='lightblue', alpha=.5)
m.drawmapboundary(fill_color='lightblue')

min_marker_size = 4
print "Plotting start"
for lon, lat, yr in zip(lons, lats, years):
    x,y = m(lon, lat)
    marker_string, label_marker = get_marker_color(yr)
    # print marker_string
    m.plot(x, y, marker_string, markersize=min_marker_size)

print "Plotting End"
# colors =['g','y','r','b']
# plt.legend(colors,["2012","2013", "2014", "2015"])
# m.bluemarble()
title_string = "Pink eye occurance for year 2012-2015"
plt.title(title_string)
plt.show()

# read your original data with year


