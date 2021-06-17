# functions

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
    
def coordsFromLayer(startCoord_lat = start_lat, startCoord_lon = start_lon, angleList=angleList, layerInt=0, pointDistance=d, debugBool=False):
    convAngleList = [convertValue(x) for x in angleList]
    '''
    first, pick an angle for the hexagon grid. 
    the grid starts at the latitude,longitude point, and creates 
    a hexagon shape of circular search radii at each hexagon vertex, 
    with one central search radius inscribed inside the hexagon points
    
    The central search radius must always be larger than the vertex search radii, when in doubt set the overlap to 15 km.
    This ensures that no area is missed by overlapping the search radii, and this accounts 
    for rounding errors formed from building the grid
    
    When in doubt, to start use the default angle of 30 degrees
    '''
    lat_start, lon_start = haversinePoint(lat1=startCoord_lat, lon1=startCoord_lon, d=pointDistance, bearing=30)
    if debugBool:
        dist_start=haversineDistance(lat1=startCoord_lat,lon1=startCoord_lon, lat2=lat_start, lon2=lon_start)
        print('Distance from center-start to starting Latitude, Longitude Coordinates is',str(dist_start))
    lat_t = None
    lon_t = None
    lat_end = None
    lon_end = None
    ct = 0
    '''
    now create a grid of hexagons
    layer 0 is the 'starting' hexagon
    subsequent layers mirror the sides of the starting hexagon, with 2n added sides after a mirror side
    where n == layer > 0
    Example: 
    layer0 (the starting hexagon) has six angles, which could be [180, 240, 300, 0, 60, 120]
    layer1, which is one additional row of hexagons outside of layer0, has the angles 
    [60, 120, 180, 120, 180, 240, 180, 240, 300, 240, 300, 0, 300, 0, 60, 0, 60, 120]
    -> starting at angle 180, the next angle in series is 240, followed by 180, followed by the next mirror angle of 240, and so on
    '''
    returnedCoords = []
    for a in convAngleList:
        ct += 1
        if lat_t is None:
            lat_t = lat_start
            lon_t = lon_start
        lat_t, lon_t = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=a)
        if ct == (len(convAngleList) - 1):
            lat_end = lat_t
            lon_end = lon_t
        if layerInt == 1:
            # pattern for 2n additional sides is angle1 == angle+60, angle2 == angle, angle3 == angle + 60, angle4 == angle
            # assumes starting after a mirror side
            returnedCoords.append((lat_t, lon_t))
            bearingT1 = a - 60
            if bearingT1 < 0:
                bearingtmp = 60 - b
                bearingT1 = 360 - bearingtmp
            lat_t, lon_t = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=bearingT1)
            returnedCoords.append((lat_t, lon_t))
            lat_t, lon_t = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=a)
            returnedCoords.append((lat_t, lon_t))
            # finally, get the point for the central search radius
            bearingM = a + 60
            if bearingM > 360:
                bearingtmp = 360 - b
                bearingM = bearingtmp
            lat_x, lon_x = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=bearingM)
            returnedCoords.append((lat_x,lat_y))
        elif layerInt == 2:
            # add central coords and bearings for layer 2
            # pattern is 2,add_center,3,add_center
            returnedCoords.append((lat_t, lon_t))
            bearingT1 = a - 60
            if bearingT1 < 0:
                bearingtmp = 60 - b
                bearingT1 = 360 - bearingtmp
            lat_t, lon_t = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=bearingT1)
            returnedCoords.append((lat_t, lon_t))
            lat_t, lon_t = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=a)
            returnedCoords.append((lat_t, lon_t))
            # get the point for the central search radius for this hexagon
            bearingM = a + 60
            if bearingM > 360:
                bearingtmp = 360 - b
                bearingM = bearingtmp
            lat_x, lon_x = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=bearingM)
            returnedCoords.append((lat_x,lat_y))
            lat_t, lon_t = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=bearingT1)
            returnedCoords.append((lat_t, lon_t))
            lat_t, lon_t = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=a)
            returnedCoords.append((lat_t, lon_t))
            # get the point for the central search radius for this hexagon
            bearingM = a + 60
            if bearingM > 360:
                bearingtmp = 360 - b
                bearingM = bearingtmp
            lat_x, lon_x = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=bearingM)
            returnedCoords.append((lat_x,lat_y))
        else:
            returnedCoords.append((lat_t, lon_t))
            # finally, get the point for the central search radius
            bearingM = a + 60
            if bearingM > 360:
                bearingtmp = 360 - b
                bearingM = bearingtmp
            lat_x, lon_x = haversinePoint(lat1=lat_t, lon1=lon_t, d=30, bearing=bearingM)
            returnedCoords.append((lat_x,lat_y))
    return (returnedCoords, (lat_end, lon_end))
  
# First, use the coordsFromLayer() function call to generate a list of latitude/longitude coordinates for each search layer

# Then, run the mapping on each layer. If a match is found and validated, or if the search reaches layer 2, stop the search. Report results or 'FILTERED' for no results.
