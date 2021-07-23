lat_origin = 43.0770611 # changeme
lon_origin = -89.4291742 # changeme

def haversineDistance(lat1, lon1, lat2, lon2):
    # credit: https://stackoverflow.com/a/4913653
    from math import radians, cos, asin, sqrt, atan2, sin
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6373.0 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def haversinePoint(lat1, lon1, d, bearing):
    # credit: https://www.edwilliams.org/avform147.htm#Dist
    # and https://dtcenter.org/sites/default/files/community-code/met/docs/write-ups/gc_simple.pdf
    from math import radians, cos, asin, sqrt, pi, atan2, sin
    lon1, lat1, bearing = map(radians, [lon1, lat1, bearing])
    d = d/6373.0
    lat2 = asin(sin(lat1)*cos(d) + cos(lat1)*sin(d)*cos(bearing))
    lon2 = None
    if (cos(lat1) == 0):
        lon2 = lon1
    else:
        lon2=((lon1-asin(sin(bearing)*sin(d)/cos(lat1))+pi)%(2*pi))-pi
    # alternative but equivalent
    # dlon=atan2(sin(bearing)*sin(d)*cos(lat1),cos(d)-sin(lat1)*sin(lat2))
    # lon2=((lon1-dlon+pi)%(2*pi))-pi
    lat2 = lat2 * (180/pi)
    lon2 = lon2 * (180/pi)
    lat1 = lat1 * (180/pi)
    lon1 = lon1 * (180/pi)
    return (lat2,lon2)

def pullItems(d, itm):
    for k,v in d.items():
        if v == itm:
            return k
def convertValue(v):
    result = v - 90
    if result < 0:
        v_res = 90 - v
        result = 360 - v_res
        return result
    else:
        return result

def convertAngle(a):
    result = a + 180
    if a <= 360:
        return result
    else:
        mod_a = a - 180
        return mod_a


# ###
# layers section
# last angle is only to validate
# layer 0
l0 = [270, 330, 30, 90, 150, 210]
# layer 1
l1 = [
     150,
     210,
     270,
     210,
     270,
     330,
     270,
     330,
     30,
     330,
     30,
     90,
     30,
     90,
     150,
     90,
     150,
     210
     ]
# layer 2
l2 = [150,210,150,210,270,210,270,210,270,330,270,330,270,330,30,330,30,330,30,90,30,90,30,90,150,90,150,90,150,210]
l0_conv = []
for i in l0:
    l0_conv.append(convertValue(i))
l1_conv = []
for i in l1:
    l1_conv.append(convertValue(i))
l2_conv = []
for i in l2:
    l2_conv.append(convertValue(i))
# print(l0_conv, l1_conv, l2_conv)


# first angle: 60 degrees, offset = 90
# first angle to first hexagon vertex in quadrant II == 60 degrees
# update: this places the origin as the center of the layer 0 hexagon
lat1, lon1 = haversinePoint(lat1=lat_origin, lon1=lon_origin, d=30, bearing=60) # 90 == W, 270 == E
print('Starting Lat,Lon is', str(lat1), str(lon1))
d = haversineDistance(lat1=lat_origin,lon1=lon_origin, lat2=lat1, lon2=lon1)
print('Distance is',str(d))

# layer 0
lat_lon_layer0 = []
lat_start = lat1
lon_start = lon1
lat_t = None
lon_t = None
lat_end = None
lon_end = None
ct = 0
for b in l0_conv:
    ct += 1
    if lat_t is None:
        lat_t = lat_start
        lon_t = lon_start
        lat_lon_layer0.append((lat_t, lon_t))
    lat_t, lon_t = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=b)
    lat_lon_layer0.append((lat_t, lon_t))
    if ct == (len(l0_conv)-1):
        lat_end = lat_t
        lon_end = lon_t

print('layer 0 coordinates are:')
for itm in lat_lon_layer0:
    print(itm)

# optional
print('Starting Latitude,Longitude is ',str(lat_start),str(lon_start),'\nEnding Latitude,Longitude is ', str(lat_t), str(lon_t))
print('Error Distance is ', haversineDistance(lat_start, lon_start, lat_t, lon_t))
print('Radial Distance from Origin Point is ', haversineDistance(lat_origin, lon_origin, lat_t, lon_t))

# layer 1
lat_lon_layer1 = []
lat_start = lat_end
lon_start = lon_end
lat_start1, lon_start1 = haversinePoint(lat1=lat_start, lon1=lon_start, d=30, bearing=0)
lat_t = None
lon_t = None
lat_end = None
lon_end = None
ct = 0
center_ct = 1
centerPointList = []
centerPointAngle = None
hexPointList = []
for b in l1_conv:
    ct += 1
    center_ct += 1
    if lat_t is None:
        lat_t = lat_start1
        lon_t = lon_start1
        lat_lon_layer1.append((lat_t, lon_t))
    if center_ct == 2:
        centerPointAngle = b + 180
        centerPointAngle = convertAngle(centerPointAngle)
    if center_ct == 3:
        lat_center, lon_center = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=centerPointAngle)
        centerPointList.append((lat_center, lon_center))
        center_ct = 0
    lat_t, lon_t = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=b)
    # print(lat_t, lon_t)
    lat_lon_layer1.append((lat_t, lon_t))
    if ct == (len(l0_conv)-1):
        lat_end = lat_t
        lon_end = lon_t

# also optional
print('layer 1 coordinates are:')
for itm in lat_lon_layer1:
    print(itm)
print('center points:')
for itm in centerPointList:
    print(itm)        
