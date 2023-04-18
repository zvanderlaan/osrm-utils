import pandas as pd
import requests
import numpy as np

iteration = 0

def match(osrm_server, latitudes, longitudes, timestamps=None, bearings=None, radiuses=None,
                       steps='false', geometries='polyline', annotations='false', overview='simplified',  
                       gaps='split', tidy='false', version='v1'):


    """
    Runs OSRM match query and returns API response as a dictionary.  Note that many of
    the parameters are expressed as strings (even ones that look like they could be boolean)
    because OSRM often allows a variety of values (e.g., annotions=true/false/nodes/distance,etc.). 
    Ultimately the routing logic and available parameter options arehandled by the OSRM server
    version and profile that was used during setup, so this function tries to remain flexible and
    doesn't try to limit the values users can pass in for certain parameters.  Invalid queries
    will be quickly apparent from the API response. Most, but not all parameters available in the
    OSRM api are included here.  See http://project-osrm.org/docs for more details.

    ----------
    osmr_server : str 
        OSRM server address containing ip address and port (e.g., 'http://127.0.0.1:5000')
    latitudes : iterable of float values 
        Ordered latitude values
    longitudes : iterable of float values 
        Ordered longitude values
    timestamps : iterable of int values, optional
        Ordered unix timestamps assosicated with coordinates
    bearings:  iterable of int values, optional
        Ordered 0-360 degree bearings associated with coordinates
    radiuses:  iterable of int values, optional
        Ordered search radiuses (meters) associated with coordinates
    steps: str, default='false' 
        Include steps for each route
    geometries: str, default='polyline'
        Geometry format (polyline/polyline6/geojson)
    annotations: str, default='false' 
        Include additional metadata for each coordinate (true/false/nodes/distance/duration/datasources/weight/speed)
    overview: str, default='simplified'
        Include overview geometry (simplified/full/false)
    gaps: str, default='split'
        Allows input track splitting based on large timestamp gaps  (split/ignore)
    tidy: str, default='false'
        Allows input track modification to obtain bettermatching quality for noisy tracks (true/false)


    Returns
    -------
    resp:  dictionary
        Returns the JSON API response as a dictionary
    """


    formatted_lat_lon = ''
    for lat, lon in zip(latitudes, longitudes):
        formatted_lat_lon += '{},{};'.format(lon,lat)
    formatted_lat_lon = formatted_lat_lon[0:-1] # drop final semicolon
    
    # Note:  'profile' can be anything -- it often says "driving" in API 
    #         docs but the behavior entirely depends on what profile the OSRM 
    #         server was setup with.  Leaving as 'profile' to not imply
    query_request = '{}/match/{}/someprofile/{}?geometries={}&steps={}&overview={}&annotations={}'.format(osrm_server, version, formatted_lat_lon, geometries, steps, overview, annotations) 
    
    if timestamps is not None: 
        formatted_timestamps = ';'.join([str(i) for i in timestamps])
        query_request += '&timestamps={}'.format(formatted_timestamps) 
    
    if bearings is not None:
        formatted_bearings = ';'.join([str(int(i)) for i in bearings])
        query_request += '&bearings={}'.format(formatted_bearings) 

    if radiuses is not None:
        formatted_radiuses = ';'.join([str(int(i)) for i in radiuses])
        query_request += '&radiuses={}'.format(formatted_radiuses) 

    r = requests.get(query_request)
    return r.json()  
    


def _mapmatch_custom(osrm_server, latitudes, longitudes, timestamps=None, bearings=None, radiuses=None):
    
    global TMP;

    """ Performs map matching using OSRM matching service with specific parameters and postprocesses the 
        results to return pandas dataframes for (a) snapped tracepoints and (b) routes.  
        Intented to be called by mapmatch_custom function (without leading underscore), which wraps this
        function and can handle large trajecories that need to be split up across API calls.

        ----------
        osmr_server : str 
            OSRM server address containing ip address and port (e.g., 'http://127.0.0.1:5000')
        latitudes : iterable of float values 
            Ordered latitude values
        longitudes : iterable of float values 
            Ordered longitude values
        timestamps : iterable of int values, optional
            Ordered unix timestamps assosicated with coordinates
        bearings:  iterable of int values, optional
            Ordered 0-360 degree bearings associated with coordinates
        radiuses:  iterable of int values, optional
            Ordered search radiuses (meters) associated with coordinates
        ----
    
        Returns
        -------
        (df_tp, df_rte):  tuple of pandas dataframes

            df_tp:  pandas dataframe of snapped tracepoints
                tp_idx: index of tracepoint 
                lat:  original tracepoint latitude
                lon: original tracepoint longitude
                snap_lat: snapped latitude (if successful)
                snap_lon: snapped latitude (if successful)
                timestamp: original tracepoint timestamp

            df_rte:  pandas dataframe of route details
                from_tp: index of tracepoint where leg begins
                to_tp: index of tracepoint where leg ends
                route_idx: route index within query
                leg_idx: leg index within route (each route consists of 1 or more legs)
                node_pair: list of adjacent OSM nodepairs within leg
                distances: list of distances of nodes within leg
                coords: list of coordinates within leg
    """

    # Make sure all iterables have expected indices starting at 0 
    # (can be side effects if you pass in a series with existing indices, so prevent this)
    latitudes = list(latitudes)
    longitudes = list(longitudes)
    timestamps = list(timestamps) if timestamps is not None else None 
    bearings = list(bearings) if bearings is not None else None 
    radiuses = list(radiuses) if radiuses is not None else None
        
    resp = match(osrm_server=osrm_server, latitudes=latitudes, longitudes=longitudes, 
                timestamps=timestamps, bearings=bearings, radiuses=radiuses,
                steps='true', geometries='geojson', annotations='true', overview='full',  
                gaps='split', tidy='false', version='v1')
    
    l_data = []

    global iteration 
    
    code = resp.get('code')
    if code == 'Ok':  
        
        ## 1.  Enumerate over original tracepoints and build a lookup table:
        ##     key = (route_idx, waypoint_idx)
        d_tracepoint_index = {}
        l_tp = []
        for tracepoint_idx, tracepoint in enumerate(resp['tracepoints']):
            snap_lon, snap_lat = None, None
            if tracepoint is not None:
                route_idx = tracepoint['matchings_index'] 
                waypoint_idx = tracepoint['waypoint_index']
                d_tracepoint_index[(route_idx, waypoint_idx)] = tracepoint_idx 
                snap_lon =  tracepoint['location'][0] 
                snap_lat =  tracepoint['location'][1] 
            tstamp = timestamps[tracepoint_idx] if timestamps is not None else None
            l_tp.append({'tp_idx': tracepoint_idx, 'lon': longitudes[tracepoint_idx], 'lat': latitudes[tracepoint_idx], 'snap_lon': snap_lon, 'snap_lat': snap_lat, 'timestamp': tstamp})


        l_rte = []
        for route_idx, route in enumerate(resp['matchings']):
            for leg_idx, leg in enumerate(route['legs']):
                
                # A leg is defined between two matched waypoints. When you have more than one leg in a trip, 
                # the last waypoint in one leg  is the same as the first waypoint in the next legs. 

                from_wp_idx = leg_idx
                to_wp_idx = leg_idx + 1                

                from_tracepoint_idx = d_tracepoint_index[(route_idx, from_wp_idx)]
                to_tracepoint_idx = d_tracepoint_index[(route_idx, to_wp_idx)]

                nodes = leg['annotation']['nodes']                 
                node_pairs = [(nodes[i], nodes[i+1]) for i in range(len(nodes)-1)] 
                distances = [round(i, 3) for i in leg['annotation']['distance']]
                # Collect coordinates from each step, which represents the geometry of the path to the 
                # next step in the leg.
                coords = []
                for step in leg['steps']:
                    assert step['geometry']['type'] == 'LineString', "unexpected geometry type"
                    coords.extend(step['geometry']['coordinates'])
                # Unclear whether this is a bug or not, but it appears that in rare cases the last point
                # is not contained in the geometry.  This coordinate will however be present in the 
                # last step's maneuver location.
                coords.append(leg['steps'][-1]['maneuver']['location'])
                # Deduplicate points that were repeated when stiching together goemetries from different steps
                coords = [coords[i] for i in range(len(coords)) if (i==0) or coords[i] != coords[i-1]]
                
                l_rte.append({'from_tp': from_tracepoint_idx, 'to_tp': to_tracepoint_idx, 'route_idx': route_idx, 'leg_idx': leg_idx, 'node_pairs': node_pairs, 'distances': distances, 'coords': coords})
               
        
        # Make sure final route dataframe has all possible tracepoints -- even those that weren't matched (use left join)
        df_rts_all = pd.DataFrame({'from_tp': [i for i in range(len(latitudes)-1)], 'to_tp': [i+1 for i in range(len(latitudes)-1)]})
        df_rte = pd.DataFrame(l_rte)
        df_rte = pd.merge(df_rts_all, df_rte, on=['from_tp', 'to_tp'], how='left')
        df_rte['matched'] = np.where(df_rte['node_pairs'].notnull(), True, False)
        # Fill unmatched route_idx and leg_idx 
        df_rte['route_idx'] = df_rte['route_idx'].astype('Int64') # Nullable int type
        df_rte['leg_idx'] = df_rte['leg_idx'].astype('Int64') # Nullable int type
        # Fill unmatched list columns with empty lists
        df_rte['node_pairs'] = df_rte['node_pairs'].fillna("").apply(list)
        df_rte['distances'] = df_rte['distances'].fillna("").apply(list)
        df_rte['coords'] = df_rte['coords'].fillna("").apply(list)
        df_rte = df_rte[['from_tp', 'to_tp', 'matched', 'route_idx', 'leg_idx', 'node_pairs', 'distances', 'coords']]

        df_tp = pd.DataFrame(l_tp)
    
    # query not okay
    else:
        df_tp = None 
        df_rte = None
  
    return df_tp, df_rte, code


def mapmatch_custom(osrm_server, latitudes, longitudes, timestamps=None, bearings=None, radiuses=None, max_matching_size=100):

    """  Wrapper for perform_osm_snapping that accounts for the fact that the OSRM server can only process
        a limited number of points at a time. To account for arbitrary trip lengths, this function splits
        each trip into appropriate sized chunks, feeds them to _mapmatch_custom, and then combines the results
        
       ----------
        osmr_server : str 
            OSRM server address containing ip address and port (e.g., 'http://127.0.0.1:5000')
        latitudes : iterable of float values 
            Ordered latitude values
        longitudes : iterable of float values 
            Ordered longitude values
        timestamps : iterable of int values, optional
            Ordered unix timestamps assosicated with coordinates
        bearings:  iterable of int values, optional
            Ordered 0-360 degree bearings associated with coordinates
        radiuses:  iterable of int values, optional
            Ordered search radiuses (meters) associated with coordinates
        max_matching_size: int, default=100
             Max number of waypoints that can be processed by OSRM in a single request (depends on server settings)
        ----
    
        Returns
        -------
        (df_tp, df_rte):  tuple of pandas dataframes

            df_tp:  pandas dataframe of snapped tracepoints
                query_idx: index of query (>0 only where where multiple api calls needed)
                tp_idx: index of tracepoint within query
                lat:  original tracepoint latitude
                lon: original tracepoint longitude
                snap_lat: snapped latitude (if successful)
                snap_lon: snapped latitude (if successful)
                timestamp: original tracepoint timestamp

            df_rte:  pandas dataframe of route details
                query_idx: index of query (>0 only where where multiple api calls needed)
                from_tp: index of tracepoint where leg begins
                to_tp: index of tracepoint where leg ends
                route_idx: route index within query(each query may have 0+ matched routes)
                leg_idx: leg index within route (each route consists of 1 or more legs)
                node_pair: list of adjacent OSM nodepairs within leg
                distances: list of distances of nodes within leg
                coords: list of coordinates within leg

    """
    # Make sure all iterables have expected indices starting at 0 
    # (can be side effects if you pass in a series with existing indices, so prevent this)
    latitudes = list(latitudes)
    longitudes = list(longitudes)
    timestamps = list(timestamps) if timestamps is not None else None 
    bearings = list(bearings) if bearings is not None else None 
    radiuses = list(radiuses) if radiuses is not None else None
        
    
    l_tp = []
    l_rte = []
    l_code = []

    n_waypoints = len(latitudes)

    query_idx = 0

    while True:

        lower_idx = query_idx*(max_matching_size-1) # subtract 1 to start with the last pt of previous iter
        upper_idx = min(lower_idx+max_matching_size, n_waypoints)
        
        #print('lower_idx: {}, upper_idx: {}'.format(lower_idx, upper_idx))
        _latitudes = latitudes[lower_idx:upper_idx]
        _longitudes = longitudes[lower_idx:upper_idx]
        _timestamps = timestamps[lower_idx:upper_idx] if timestamps is not None else None
        _bearings = bearings[lower_idx:upper_idx] if bearings is not None else None
        _radiuses = radiuses[lower_idx:upper_idx] if radiuses is not None else None

        _df_tp, _df_rte, code = _mapmatch_custom(osrm_server, _latitudes, _longitudes, _timestamps, _bearings, _radiuses)
        
        l_code.append(code)

        if code == 'Ok':
            # add index to specify the iteration
            _df_tp.insert(0, 'query_idx', query_idx)
            _df_rte.insert(0, 'query_idx', query_idx)
            l_tp.append(_df_tp)
            l_rte.append(_df_rte)
        
        if upper_idx == n_waypoints:
            break
        query_idx += 1
        global iteration 
        iteration += 1

    df_tp = pd.concat(l_tp).reset_index(drop=True) if len(l_tp) > 0 else None
    df_rte = pd.concat(l_rte).reset_index(drop=True) if len(l_rte) > 0 else None
    return df_tp, df_rte, l_code