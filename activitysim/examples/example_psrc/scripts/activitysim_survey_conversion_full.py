# Convert original survey data to activitysim and/or daysim format

import os
import time
import numpy as np
import pandas as pd
import numpy as np
import geopandas as gpd
import urllib
import pyodbc
import yaml
import sqlalchemy
from shapely import wkt
from scipy.spatial import cKDTree
from shapely.geometry import Point
from sqlalchemy.engine import URL
from pymssql import connect
import logging
import logcontroller
import datetime
pd.options.mode.chained_assignment = None  # default='warn'
from activitysim.abm.models.util import canonical_ids as ci
from activitysim.abm.models.util import tour_frequency as tf
from activitysim.core.util import reindex
from config_activitysim import *

# Set current working directory to script location
#working_dir = r'C:\Workspace\activitysim\activitysim\examples\example_psrc\scripts'
#os.chdir(working_dir)

# Import local module variables
#from lookup import *

#from config_daysim import *

logger = logcontroller.setup_custom_logger('main_logger')
logger.info('--------------------NEW RUN STARTING--------------------')
start_time = datetime.datetime.now()


# Load config files from specified file
# python infer.py data
#args = sys.argv[1:]
#assert len(args) == 2, "usage: python activitysim_survey_conversion_full.py <configs_file>"

#config_dir = args[0]

psrc_crs = 'EPSG:2285'

home = 'Home'    # destination value for Home

# Iterate through each unique person and their travel days
person_id_col = 'person_id'
day_col = 'daynum'
trip_id = 'trip_id' # unique trip ID
hhid = 'household_id'

# Usual school and workplace variables
# Expect this to be defined in person expression files
school_taz ='school_taz'
work_taz = 'work_taz' 
school_parcel = 'school_loc_parcel'
work_parcel = 'work_parcel'

# houeshold weight
home_parcel = 'final_home_parcel'
hh_weight = 'hh_weight_2017_2019'

# Departure/arrival times in minutes after midnight
deptm = 'depart_time_mam'
arrtm = 'arrival_time_mam'

# trip columns
otaz = 'origin'
dtaz = 'destination'
opcl = 'origin_parcel_dim_id'    # not used in activysim
dpcl = 'dest_parcel_dim_id'
# land use type field
oadtyp = 'oadtyp'    # origin land use type
dadtyp = 'dadtyp'    # destination land use type
adtyp_school = 'School'
adtyp_work = 'Work'
purp_work = 'Work'
purp_home = 'Home'
purp_school = 'School'
trip_weight = 'trip_weight_2017_2019'

# tour columns
totaz = 'tomaz'
tdtaz = 'tdtaz'
topcl = 'topcl'
tdpcl = 'tdpcl'
tour_id_col = 'tour_id'
toadtyp = 'toadtyp'
tdadtyp = 'tdadtyp'
work_based_subtour = 'atwork'

# FIXME: will need to assign an activitysim-specific ID; should probably just use for the script in general

# tour data
parent = 'parent_tour_id'

# Trip Purposes
opurp = 'origin_purpose_cat'
dpurp = 'dest_purpose_cat'

# Tour
topurp = 'topurp'
tdpurp = 'tour_type'
tour_mode = 'tour_mode'

# Set input paths
parcel_file = r'R:\e2projects_two\SoundCast\Inputs\dev\landuse\2018\new_emp\parcels_urbansim.txt'

# iner.py 
SURVEY_TOUR_ID = 'survey_tour_id'
SURVEY_PARENT_TOUR_ID = 'survey_parent_tour_id'
SURVEY_PARTICIPANT_ID = 'survey_participant_id'
ASIM_TOUR_ID = 'tour_id'
ASIM_PARENT_TOUR_ID = 'parent_tour_id'

def load_elmer_geo_table(feature_class_name, con, crs):
    """ Load ElmerGeo table as geoDataFrame, applying a specified coordinate reference system (CRS)
    """
    geo_col_stmt = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=" + "\'" + feature_class_name + "\'" + " AND DATA_TYPE='geometry'"
    geo_col = str(pd.read_sql(geo_col_stmt, con).iloc[0,0])
    query_string = 'SELECT *,' + geo_col + '.STGeometryN(1).ToString()' + ' FROM ' + feature_class_name

    df = pd.read_sql(query_string, con)
    df.rename(columns={'':'geometry'}, inplace = True)
    df['geometry'] = df['geometry'].apply(wkt.loads)

    gdf = gpd.GeoDataFrame(df, geometry='geometry')
    gdf.crs = crs

    return gdf

def mangle_ids(ids):
    return ids * 10


def unmangle_ids(ids):
    return ids // 10

def convert_hhmm_to_mam(x):

    if x == -1:
        mam = -1
    else:

        # Convert string of time in HH:MM AM/PM format to minutes after minute
        ampm = x.split(' ')[-1]
        hr = int(x.split(':')[0])
        if (ampm == 'PM') & (hr != 12):
            hr += 12
        min = int(x.split(':')[-1].split(' ')[0])

        mam = (hr*60) + min

    return mam



def assign_tour_mode(_df, tour_dict, tour_id, mode_heirarchy=mode_heirarchy):
    """ Get a list of transit modes and identify primary mode
        Primary mode is the first one from a heirarchy list found in the tour.   """
    mode_list = _df['mode'].unique()
    for mode in mode_heirarchy:
        if mode in mode_list:
            return mode

def find_nearest(gdA, gdB):
    """ Find nearest value between two geodataframes.
        Returns "dist" for distance between nearest points.
    """

    nA = np.array(list(gdA.geometry.apply(lambda x: (x.x, x.y))))
    nB = np.array(list(gdB.geometry.apply(lambda x: (x.x, x.y))))
    btree = cKDTree(nB)
    dist, idx = btree.query(nA, k=1)
    gdB_nearest = gdB.iloc[idx].drop(columns="geometry").reset_index(drop=True)
    gdf = pd.concat(
        [
            gdA.reset_index(drop=True),
            gdB_nearest,
            pd.Series(dist, name='dist')
        ], 
        axis=1)

    return gdf




################################
# Initialize
################################

# TEMP
#person_file_dir = r'R:\e2projects_two\2018_base_year\survey\geocode_parcels\2018\2_person.csv'
#person_csv = pd.read_csv(person_file_dir, encoding='latin-1')
#household_file_dir = r'R:\e2projects_two\2018_base_year\survey\geocode_parcels\2018\1_household.csv'
#hh_csv = pd.read_csv(household_file_dir, encoding='latin-1')


# READ from Elmer when data is ready
conn_string = "DRIVER={ODBC Driver 17 for SQL Server}; SERVER=AWS-PROD-SQL\Sockeye; DATABASE=Elmer; trusted_connection=yes"
sql_conn = pyodbc.connect(conn_string)
params = urllib.parse.quote_plus(conn_string)
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
# TEMP; should read directly from Elmer when possible
#person = pd.read_sql(sql='SELECT * FROM HHSurvey.v_persons WHERE survey_year IN (2017, 2019)', con=engine)
#hh = pd.read_sql(sql='SELECT * FROM HHSurvey.v_households WHERE survey_year IN (2017, 2019)', con=engine)
#trip = pd.read_sql(sql='SELECT * FROM HHSurvey.v_trips WHERE survey_year IN (2017, 2019)', con=engine)

# FIXME: for now, use a CSV version instead of ELmer data because loading is inconsistent
#person.to_csv(r'R:\e2projects_two\activitysim\survey\elmer_person.csv', index=False)
#hh.to_csv(r'R:\e2projects_two\activitysim\survey\elmer_hh.csv', index=False)
#trip.to_csv(r'R:\e2projects_two\activitysim\survey\elmer_trip.csv', index=False)

#maz_shp = gpd.read_file(r'R:\e2projects_two\activitysim\conversion\maz_bgs.shp')
#maz_shp.crs = psrc_crs

##trip = pd.read_csv(r'R:\e2projects_two\activitysim\survey\elmer_trip.csv')

##### Load a parcel to MAZ lookup
#parcel_block_file = r'R:\e2projects_two\activitysim\conversion\geographic_crosswalks\parcel_taz_block_lookup.csv'
#parcel_block = pd.read_csv(parcel_block_file)
#parcel_block.index = parcel_block['parcel_id']
#parcel_maz_dict = parcel_block[['maz_id']].to_dict()['maz_id']
#parcel_maz_dict[-1] = -1   # retain -1 flag

####################################
## geolocate parcels; existing parcels on data don't make sense
####################################

##elmergeo_conn_string = 'AWS-Prod-SQL\Sockeye'
##elmergeo_con = connect('AWS-Prod-SQL\Sockeye', database="ElmerGeo")

##block_grp_gdf = load_elmer_geo_table('blockgrp2010_nowater', elmergeo_con, psrc_crs)

## Exclude trips with null lat/lng for origins and dest
#trip = trip[~trip['origin_lng'].isnull()]
#trip = trip[~trip['origin_lat'].isnull()]
#trip = trip[~trip['dest_lng'].isnull()]
#trip = trip[~trip['dest_lat'].isnull()]
#trip_gdf = gpd.GeoDataFrame(trip[['trip_id','origin_lng','origin_lat']], geometry=gpd.points_from_xy(x=trip['origin_lng'],y=trip['origin_lat']))
#trip_gdf.crs = 'EPSG:4326'
#trip_gdf = trip_gdf.to_crs(psrc_crs)

## Intersect trip ends with block group
#trip_gdf = trip_gdf.sjoin(maz_shp[['MAZ','geometry']], how="left")
#trip = trip.merge(trip_gdf, on='trip_id', how='left')
#trip.rename(columns={'MAZ': otaz}, inplace=True)

#trip_gdf = gpd.GeoDataFrame(trip[['trip_id','dest_lng','dest_lat']], geometry=gpd.points_from_xy(x=trip['dest_lng'],y=trip['dest_lat']))
#trip_gdf.crs = 'EPSG:4326'
#trip_gdf = trip_gdf.to_crs(psrc_crs)
#trip_gdf = trip_gdf.sjoin(maz_shp[['MAZ','geometry']], how="left")
#trip = trip.merge(trip_gdf, on='trip_id', how='left')
#trip.rename(columns={'MAZ': dtaz}, inplace=True)

#trip[[otaz,dtaz]] = trip[[otaz,dtaz]].fillna(-1).astype('int')

## TEMP, only run when needed
#trip.to_csv(r'R:\e2projects_two\activitysim\survey\elmer_trip_geocoded.csv', index=False)

###################################
## Load CSV data
person = pd.read_csv(r'R:\e2projects_two\activitysim\survey\elmer_person.csv')
#hh = pd.read_csv(r'R:\e2projects_two\activitysim\survey\elmer_hh.csv')
#trip = pd.read_csv(r'R:\e2projects_two\activitysim\survey\elmer_trip_geocoded.csv')

####################################
## Person
####################################

## Load an expression file

#expr_df = pd.read_csv(r'\\modelstation2\c$\Workspace\activitysim\activitysim\examples\example_psrc\scripts\person_expr_activitysim.csv')

#for index, row in expr_df.iterrows():
#    expr = 'person.loc[' + row['filter'] + ', "' + row['result_col'] + '"] = ' + str(row['result_value'])
#    print(row['index'])
#    exec(expr)


## Check that all person types are filled in
#assert person['ptype'].count() == len(person)

## If person makes a school tour but has no school_col field, set as tour destination zone for school tours
## Select tours that have purpose of school and people who do not have a school zone
##df_school = df_tour[(df_tour[tdpurp] == 2) & (df_tour['destination'] != -1)]
##if zone_type == 'TAZ':
##    dest_col = 'taz'
##else:
##    dest_col = 'pcl'
##df_school['updated_school_taz'] = df_school['td'+dest_col]

## Calculate PERNUM (person sequence in household)
## FIXME: move to Person section
person[['person_id_str','household_id_str']] = person[['person_id','household_id']].astype('str')
person['PNUM'] = person.apply(lambda x: x['person_id_str'].replace(x['household_id_str'], '').strip(), axis=1).astype('int')

#person.to_csv(r'survey_data\survey_person.csv')


##################################
### Household
##################################

#expr_df = pd.read_csv(r'\\modelstation2\c$\Workspace\activitysim\activitysim\examples\example_psrc\scripts\hh_expr_activitysim.csv')

## Merge parcel files to households to get data on single/multi-family homes
#raw_parcels_df = pd.read_csv(parcel_file, delim_whitespace=True, usecols=['parcelid', 'sfunits', 'mfunits']) 
#hh = hh.merge(raw_parcels_df, left_on=home_parcel, right_on='parcelid')

#for index, row in expr_df.iterrows():
#    expr = 'hh.loc[' + row['filter'] + ', "' + row['result_col'] + '"] = ' + str(row['result_value'])
#    print(row['index'])
#    #pd.eval(expr, engine='python').reset_index()
#    # FIXME: is exec bad form instead of pd.eval? Why is pd.eval not working?
#    exec(expr)

## Calculate the total number of people in each person_type field; necessary for Daysim only
#person_type_field = 'ptype'
#hhid_col = 'household_id'
#for person_type in person[person_type_field].unique():
#    print(person_type_dict[person_type])
#    df = person[person['ptype'] == person_type]
#    df = df.groupby('household_id').count().reset_index()[[hh_weight,hhid_col]]
#    df.rename(columns={hh_weight: person_type_dict[person_type]}, inplace=True)
    
#     # Join to households
#    hh = pd.merge(hh, df, how='left', on=hhid_col)
#    hh[person_type_dict[person_type]].fillna(0, inplace=True)
#    hh[person_type_dict[person_type]] = hh[person_type_dict[person_type]].astype('int')


### Remove households without parcels
##hh = hh[-hh['hhparcel'].isnull()]

##daysim_fields = [hhid,'hhsize','hhvehs','hhwkrs','hhftw','hhptw','hhret','hhoad','hhuni','hhhsc','hh515',
##                'hhcu5','hhincome','hownrent','hrestype','hhtaz','hhparcel','hhexpfac','samptype']

##hh = hh[daysim_fields]
#hh.to_csv('household.csv')


#########################################
## Trip
#########################################

# TEMP REMOVE FIXME
#household = pd.read_csv('household.csv')
#person = pd.read_csv('person.csv')

#expr_df = pd.read_csv(r'\\modelstation2\c$\Workspace\activitysim\activitysim\examples\example_psrc\scripts\trip_expr_activitysim.csv')

## Need some values from the person file
#trip = trip.merge(person[[person_id_col,school_taz,work_taz]], how='left', on=person_id_col)

#for index, row in expr_df.iterrows():
#    expr = 'trip.loc[' + row['filter'] + ', "' + row['result_col'] + '"] = ' + str(row['result_value'])
#    print(row['index'])
#    #pd.eval(expr, engine='python').reset_index()
#    # FIXME: is exec bad form instead of pd.eval? Why is pd.eval not working?
#    exec(expr)


# #additional filters to consider
# #day of week
# #Trips with same origin/destination

#trip[SURVEY_TOUR_ID] trip['tour'].copy()

# Trim to only required columns


#######################################
# Tour
#######################################

## temp remove
trip = pd.read_csv('trip.csv')

# Filter for a single day
trip = trip[trip['daynum'] == 3]

#Create tours from from trip file that has already been partially processed; note that
#once tours are built the trips will need to be udpated with tour information

expr_df = pd.read_csv(r'\\modelstation2\c$\Workspace\activitysim\activitysim\examples\example_psrc\scripts\tour_expr_activitysim.csv')

bad_trips = ()

# Filtering trips
def flag_trips(df, bad_trips, msg):

    for i in df[trip_id].to_list():
        bad_trips +=(msg, i)

    return bad_trips

# Filter out trips that have the same origin and destination of home
# Should we 
filter = ((trip[opurp] == trip[dpurp]) & (trip[opurp] == home))
if len(trip[filter]) > 0:
    bad_trips = flag_trips(trip[filter], bad_trips, 'trips have the same origin and destination of home')
    trip = trip[~filter]

filter = ~trip[opurp].isin(purpose_map.keys())
if len(trip[filter]) > 0:
    bad_trips = flag_trips(trip[filter], bad_trips, 'missing trip origin purpose')
    trip = trip[~filter]

filter = ~trip[dpurp].isin(purpose_map.keys())
if len(trip[filter]) > 0:
    bad_trips = flag_trips(trip[filter], bad_trips, 'missing trip destination purpose')
    trip = trip[~filter]

# FIXME:
# Some trips have odd departure and arrival times

tour_dict = {}
tour_id = 1
counter = 0

for personid in trip[person_id_col].unique():
#for personid in [1710005901]:
    print(counter)
    counter += 1 
    person_df = trip.loc[trip[person_id_col] == personid]
    # Loop through each day
    for day in person_df[day_col].unique():
        # FIXME: make sure we're only using T/W/Th (maybe monday?)
        df = person_df.loc[person_df[day_col] == day]

        # First o and last d of person's travel day should be home
        if (df.groupby(person_id_col).first()[opurp].values[0] != 'Home') or df.groupby(person_id_col).last()[dpurp].values[0] != 'Home':
        #    # Flag this set
            for i in df[trip_id].to_list():
                bad_trips += ('travel day does not start or end at home', i)
            continue

        # Some people do not report sequential trips. 
        # If the dest_purpose of the previous trip does not match the origin_purpose of the next trip, skip
        # similarly for activity type
        #df['next_origin_purpose'] = df.shift(-1)[[opurp]]
        #df['prev_origin_purpose'] = df.shift(1)[[opurp]]
        #df['next_oadtyp'] = df.shift(-1)[[oadtyp]]
        #if len(df.iloc[:-1][(df.iloc[:-1]['next_origin_purpose'] != df.iloc[:-1][dpurp])]) > 0:
        #    bad_trips += df[trip_id].tolist()
        #    continue
        #if len(df.iloc[:-1][(df.iloc[:-1]['next_oadtyp'] != df.iloc[:-1][dadtyp])]) > 0:
        #    bad_trips += df[trip_id].tolist()
        #    continue

        
        # Identify home-based tours 
        # These will be used as the bookends for creating tours and subtours
        home_tours_start = df[df[opurp] == home]
        home_tours_end = df[df[dpurp] == home]

        ## skip person if they have a different number of tour starts/ends at home
        if len(home_tours_start) != len(home_tours_end):
            for i in df[trip_id].to_list():
                bad_trips += ('different number of tour starts/ends at home', i)
            continue
        
        # Loop through each set of home-based tours
        # These trips will be scanned for any subtours and assigned trip components
        
        for tour_start_index in range(len(home_tours_start)):

            tour_dict[tour_id] = {}       
    
            # start/end row for this set
            start_row_id = home_tours_start.index[tour_start_index]
            end_row_id = home_tours_end.index[tour_start_index]

            # iterate between the start row id and the end row id to build the tour
            # Select slice of trips that correspond to a trip set
            _df = df.loc[start_row_id:end_row_id]

            # calculate duration at location, as difference between arrival at a place and start of next tripi
            _df.loc[:,'duration'] = _df.shift(-1).iloc[:-1][deptm]-_df.iloc[:-1][arrtm]

            # First row contains origin information for the primary tour
            tour_dict[tour_id]['tlvorig'] = _df.iloc[0][deptm]              # Time leaving origin
            tour_dict[tour_id][totaz] = _df.iloc[0][otaz]                 # Tour origin TAZ
            tour_dict[tour_id][topcl] = _df.iloc[0][opcl]                 # Tour origin parcel
            tour_dict[tour_id][toadtyp] = _df.iloc[0][oadtyp]           # Tour origin address type

            # Last row contains return information
            tour_dict[tour_id]['tarorig'] = _df.iloc[-1][arrtm]             # Tour arrive time at origin (return time)

            # Household and person info
            tour_dict[tour_id][hhid] = _df.iloc[0][hhid]
            tour_dict[tour_id][person_id_col] = _df.iloc[0][person_id_col]
            tour_dict[tour_id]['day'] = day
            #tour_dict[tour_id][tour_id_col] = local_tour_id

            # For sets with only 2 trips, the halves are simply the first and second trips
            if len(_df) == 2:
                #if _df.iloc[0][dpurp] in [0,10]:   # ignore tours that have purposes to home or changemode
                #    bad_trips += _df[trip_id].to_list()
                #    continue

                # ----- Generate Tour Record -----
                # Apply standard rules for 2-leg tours
                tour_dict[tour_id][tdpurp] = _df.iloc[0][dpurp]
                tour_dict[tour_id]['tripsh1'] = 1
                tour_dict[tour_id]['tripsh2'] = 1
                tour_dict[tour_id][tdadtyp] =  _df.iloc[0][dadtyp]
                tour_dict[tour_id][toadtyp] =  _df.iloc[0][oadtyp]
                tour_dict[tour_id][tdtaz] = _df.iloc[0][dtaz]
                tour_dict[tour_id][tdpcl] = _df.iloc[0][dpcl]
                tour_dict[tour_id]['tardest'] = _df.iloc[0][arrtm]
                tour_dict[tour_id]['tlvdest'] = _df.iloc[-1][deptm]
                tour_dict[tour_id]['tarorig'] = _df.iloc[-1][arrtm]
                tour_dict[tour_id][parent] = 0    # No subtours for 2-leg trips
                tour_dict[tour_id]['subtrs'] = 0    # No subtours for 2-leg trips
                tour_dict[tour_id][tour_id_col] = tour_id
                #tour_dict[tour_id]['tpathtp'] = _df.iloc[0]['pathtype']    # Path type

                # ----- Update Related Trip Record -----
                # Set tour half and tseg within half tour for trips
                # for tour with only two records, there will always be two halves with tseg = 1 for both
                trip.loc[trip[trip_id] == _df.iloc[0][trip_id], 'half'] = 1
                trip.loc[trip[trip_id] == _df.iloc[-1][trip_id], 'half'] = 2
                trip.loc[trip[trip_id].isin(_df[trip_id]),'tseg'] = 1
                tour_dict[tour_id][tour_mode] = assign_tour_mode(_df, tour_dict, tour_id)
                trip.loc[trip[trip_id].isin(_df[trip_id].values),'tour'] = tour_id

                # Done with this tour; increment tour IDs
                tour_id += 1

            # For tour groups with > 2 trips, calculate primary purpose and halves; first deal with subtours
            
            else: 
                # Could be dealing with work-based subtours
                # subtours exist if set of trips contains destinations at usual workplace more than 2 times
                # Minimum trips required for a subtour is 4 (2 legs to/from home and 2 legs to/from work for the subtour)
                if (len(_df) >= 4) & (len(_df[_df[oadtyp] == adtyp_work]) >= 2) & (len(_df[_df[opurp] == purp_work]) >= 2) & \
                    (len(_df[(_df[oadtyp] == adtyp_work) & (~_df[dadtyp].isin([purp_work,purp_home]))]) >= 1):

                    subtour_index_start_values = _df[(((_df[oadtyp] == adtyp_work) & (~_df[dadtyp].isin([purp_work,purp_home]))) | 
                                                        ((_df[opurp] == purp_work) & (~_df[dpurp].isin([purp_work,purp_home]))))].index.values

                    print('processing subtour ---------------')
                    subtours_df = pd.DataFrame()

                    # Loop through each potential subtour
                    # the following trips must eventually return to work for this to qualify as a subtour
                    # Subtour ID will start as one index higher than the parent tour
                    subtour_count = 0

                    parent_tour_id = tour_id

                    for subtour_start_value in subtour_index_start_values:
                        #print(subtour_start_value)
                        # Potential subtour
                        # Loop through the index from subtour start 
                        next_row_index_start = np.where(_df.index.values == subtour_start_value)[0][0]+1
                        for i in _df.index.values[next_row_index_start:]:
                            next_row = _df.loc[i]
                            if next_row[dadtyp] == adtyp_work:    # Assuming we only have work-based subtours

                                tour_id += 1

                                subtour_df = _df.loc[subtour_start_value:i]

                                tour_dict[tour_id] = {}
                                # Process this subtour
                                # Create a new tour record for the subtour
                                subtour_df['tour_id'] = tour_id     # need a unique ID
                                subtours_df = subtours_df.append(subtour_df)

                                # add this as a tour
                                tour_dict[tour_id][tour_id_col] = tour_id
                                tour_dict[tour_id][hhid] = subtour_df.iloc[0][hhid]
                                tour_dict[tour_id][person_id_col] = subtour_df.iloc[0][person_id_col]
                                tour_dict[tour_id]['day'] = day
                                tour_dict[tour_id]['tlvorig'] = subtour_df.iloc[0][deptm]
                                tour_dict[tour_id]['tarorig'] = subtour_df.iloc[-1][arrtm]
                                tour_dict[tour_id][totaz] = subtour_df.iloc[0][otaz]
                                tour_dict[tour_id][topcl] = subtour_df.iloc[0][opcl]
                                tour_dict[tour_id][toadtyp] = subtour_df.iloc[0][oadtyp]
                                tour_dict[tour_id][parent] = parent_tour_id    # Parent is the main tour ID
                                tour_dict[tour_id]['subtrs'] = 0    # No subtours for subtours
                                tour_dict[tour_id][tdpurp] = work_based_subtour

                                trip.loc[trip[trip_id].isin(subtour_df[trip_id].values),'tour'] = tour_id

                                if len(subtour_df) == 2:

                                    tour_dict[tour_id][tdpurp] = subtour_df.iloc[0][dpurp]
                                    tour_dict[tour_id]['tripsh1'] = 1
                                    tour_dict[tour_id]['tripsh2'] = 1
                                    tour_dict[tour_id][tdadtyp] =  subtour_df.iloc[0][dadtyp]
                                    tour_dict[tour_id][toadtyp] =  subtour_df.iloc[0][oadtyp]
                                    #tour_dict[tour_id]['tpathtp'] = subtour_df.iloc[0]['pathtype']
                                    tour_dict[tour_id]['tdtaz'] = subtour_df.iloc[0][dtaz]
                                    tour_dict[tour_id][tdpcl] = subtour_df.iloc[0][dpcl]
                                    tour_dict[tour_id]['tlvdest'] = subtour_df.iloc[-1][deptm]
                                    tour_dict[tour_id]['tardest'] = subtour_df.iloc[0][arrtm]

                                    tour_dict[tour_id][tour_mode] = assign_tour_mode(subtour_df, tour_dict, tour_id)

                                    # Set tour half and tseg within half tour for trips
                                    # for tour with only two records, there will always be two halves with tseg = 1 for both
                                    trip.loc[trip[trip_id] == subtour_df.iloc[0][trip_id], 'half'] = 1
                                    trip.loc[trip[trip_id] == subtour_df.iloc[-1][trip_id], 'half'] = 2
                                    trip.loc[trip[trip_id].isin(_df[trip_id]),'tseg'] = 1

                                # If subtour length > 2, find the primary purpose/destination
                                else:
                                    subtour_df['duration'] = subtour_df.shift(-1).iloc[:-1][deptm]-subtour_df.iloc[:-1][arrtm]
                                    # Assume location with longest time spent at location (duration) is main subtour purpose
                                    primary_subtour_purp_index = subtour_df[subtour_df[dpurp]!='Change mode']['duration'].idxmax()
                                    tour_dict[tour_id][tdpurp] = subtour_df.loc[primary_subtour_purp_index][dpurp]

                                    # Get subtour data based on the primary destination trip
                                    # We know the tour destination parcel/TAZ field from that primary trip, as well as destination type
                                    tour_dict[tour_id][tdtaz] = subtour_df.loc[primary_subtour_purp_index][dtaz]
                                    tour_dict[tour_id][tdpcl] = subtour_df.loc[primary_subtour_purp_index][dpcl]
                                    tour_dict[tour_id][tdadtyp] = subtour_df.loc[primary_subtour_purp_index][dadtyp]

                                    # Calculate tour halves, etc
                                    tour_dict[tour_id]['tripsh1'] = len(subtour_df.loc[0:primary_subtour_purp_index])
                                    tour_dict[tour_id]['tripsh2'] = len(subtour_df.loc[primary_subtour_purp_index+1:])

                                    # Set tour halves on trip records
                                    trip.loc[trip[trip_id].isin(subtour_df.loc[0:primary_subtour_purp_index].trip_id),'half'] = 1
                                    trip.loc[trip[trip_id].isin(subtour_df.loc[primary_subtour_purp_index+1:].trip_id),'half'] = 2

                                    # set trip segment within half tours
                                    trip.loc[trip[trip_id].isin(subtour_df.loc[0:primary_subtour_purp_index].trip_id),'tseg'] = range(1,len(subtour_df.loc[0:primary_subtour_purp_index])+1)
                                    trip.loc[trip[trip_id].isin(subtour_df.loc[primary_subtour_purp_index+1:].trip_id),'tseg'] = range(1,len(subtour_df.loc[primary_subtour_purp_index+1:])+1)

                                    # Departure/arrival times
                                    tour_dict[tour_id]['tlvdest'] = subtour_df.loc[primary_subtour_purp_index][deptm]
                                    tour_dict[tour_id]['tardest'] = subtour_df.loc[primary_subtour_purp_index][arrtm]
                                        
                                    tour_dict[tour_id][tour_mode] = assign_tour_mode(subtour_df, tour_dict, tour_id)

                                # Done with this subtour 
                                subtour_count += 1
                                break
                            else:
                                continue
                        
                    if len(subtours_df) < 1:
                        # No subtours found
                        # FIXME: make this a function, because it's called multiple times
                        tour_dict[tour_id]['subtrs'] = 0
                        tour_dict[tour_id][parent] = 0
                        tour_dict[tour_id][tour_id_col] = tour_id

                        # Identify the primary purpose
                        primary_purp_index = _df[-_df[dpurp].isin(purpose_map.values())]['duration'].idxmax()

                        tour_dict[tour_id][tdpurp] = _df.loc[primary_purp_index][dpurp]
                        tour_dict[tour_id]['tlvdest'] = _df.loc[primary_purp_index][deptm]
                        tour_dict[tour_id][tdtaz] = _df.loc[primary_purp_index][dtaz]
                        tour_dict[tour_id][tdpcl] = _df.loc[primary_purp_index][dpcl]
                        tour_dict[tour_id][tdadtyp] = _df.loc[primary_purp_index][dadtyp]

                        tour_dict[tour_id]['tardest'] = _df.iloc[-1][arrtm]
                   
                        tour_dict[tour_id]['tripsh1'] = len(_df.loc[0:primary_purp_index])
                        tour_dict[tour_id]['tripsh2'] = len(_df.loc[primary_purp_index+1:])

                        # Set tour halves on trip records
                        trip.loc[trip[trip_id].isin(_df.loc[0:primary_purp_index].trip_id),'half'] = 1
                        trip.loc[trip[trip_id].isin(_df.loc[primary_purp_index+1:].trip_id),'half'] = 2

                        # set trip segment within half tours
                        trip.loc[trip[trip_id].isin(_df.loc[0:primary_purp_index].trip_id),'tseg'] = range(1,len(_df.loc[0:primary_purp_index])+1)
                        trip.loc[trip[trip_id].isin(_df.loc[primary_purp_index+1:].trip_id),'tseg'] = range(1,len(_df.loc[primary_purp_index+1:])+1)

                        trip.loc[trip[trip_id].isin(_df[trip_id].values),'tour'] = tour_id

                        # Extract main mode 
                        tour_dict[tour_id][tour_mode] = assign_tour_mode(_df, tour_dict, tour_id)  
                        
                        tour_id += 1
                        
                    else:
                        # The main tour destination arrival will be the trip before subtours
                        # the main tour destination departure will be the trip after subtours
                        # trip when they arrive to work -> always the previous trip before subtours_df index begins

                        # Modify the parent tour results
                        main_tour_start_index = _df.index.values[np.where(_df.index.values == subtours_df.index[0])[0][0]-1]   
                        # trip when leave work -> always the next trip after the end of the subtours_df
                        main_tour_end_index = _df.index.values[np.where(_df.index.values == subtours_df.index[-1])[0][0]+1]    
                        # If there were subtours, this is a work tour
                        tour_dict[parent_tour_id][tdpurp] = 'Work'
                        tour_dict[parent_tour_id][tdtaz] = _df.loc[main_tour_start_index][dtaz]
                        tour_dict[parent_tour_id][tdpcl] = _df.loc[main_tour_start_index][dpcl]
                        tour_dict[parent_tour_id][tdadtyp] = _df.loc[main_tour_start_index][dadtyp]

                        # Pathtype is defined by a heirarchy, where highest number is chosen first
                        # Ferry > Commuter rail > Light Rail > Bus > Auto Network
                        # Note that tour pathtype is different from trip path type (?)
                        subtours_excluded_df = pd.concat([df.loc[start_row_id:main_tour_start_index], df.loc[main_tour_end_index:end_row_id]])
                        #tour_dict[tour_id]['tpathtp'] = subtours_excluded_df['pathtype'].max()

                        # Calculate tour halves, etc
                        tour_dict[parent_tour_id]['tripsh1'] = len(_df.loc[0:main_tour_start_index])
                        tour_dict[parent_tour_id]['tripsh2'] = len(_df.loc[main_tour_end_index:])

                        # Set tour halves on trip records
                        trip.loc[trip[trip_id].isin(_df.loc[0:main_tour_start_index].trip_id),'half'] = 1
                        trip.loc[trip[trip_id].isin(_df.loc[main_tour_end_index:].trip_id),'half'] = 2

                        # set trip segment within half tours
                        trip.loc[trip[trip_id].isin(_df.loc[0:main_tour_start_index].trip_id),'tseg'] = range(1,len(_df.loc[0:main_tour_start_index])+1)
                        trip.loc[trip[trip_id].isin(_df.loc[main_tour_end_index:].trip_id),'tseg'] = range(1,len(_df.loc[main_tour_end_index:])+1)

                        # Departure/arrival times
                        tour_dict[parent_tour_id]['tlvdest'] = _df.loc[main_tour_end_index][deptm]
                        tour_dict[parent_tour_id]['tardest'] = _df.loc[main_tour_start_index][arrtm]

                        # ID and Number of subtours 
                        tour_dict[parent_tour_id]['tour_id'] = parent_tour_id
                        tour_dict[parent_tour_id]['subtrs'] = subtour_count
                        tour_dict[parent_tour_id][parent] = 0

                        # Mode
                        tour_dict[parent_tour_id][tour_mode] = assign_tour_mode(_df, tour_dict, tour_id)
                        
                        # add tour ID to the trip records (for trips not in the subtour_df)
                        df_unique_no_subtours = [i for i in _df[trip_id].values if i not in subtours_df[trip_id].values]
                        df_unique_no_subtours = _df[_df[trip_id].isin(df_unique_no_subtours)]
                        trip.loc[trip[trip_id].isin(df_unique_no_subtours[trip_id].values),'tour'] = parent_tour_id

                        tour_id += 1

                else:
                    # No subtours
                    tour_dict[tour_id]['subtrs'] = 0
                    tour_dict[tour_id][parent] = 0
                    tour_dict[tour_id]['tour_id'] = tour_id

                    # Identify the primary purpose
                    # FIXME: need to find the primary purpose here
                    primary_purp_index = _df[-_df[dpurp].isin([0,10])]['duration'].idxmax()

                    tour_dict[tour_id][tdpurp] = _df.loc[primary_purp_index][dpurp]
                    tour_dict[tour_id]['tlvdest'] = _df.loc[primary_purp_index][deptm]
                    tour_dict[tour_id][tdtaz] = _df.loc[primary_purp_index][dtaz]
                    tour_dict[tour_id][tdpcl] = _df.loc[primary_purp_index][dpcl]
                    tour_dict[tour_id][tdadtyp] = _df.loc[primary_purp_index][dadtyp]

                    tour_dict[tour_id]['tardest'] = _df.iloc[-1][arrtm]
                   
                    tour_dict[tour_id]['tripsh1'] = len(_df.loc[0:primary_purp_index])
                    tour_dict[tour_id]['tripsh2'] = len(_df.loc[primary_purp_index+1:])

                    # Set tour halves on trip records
                    trip.loc[trip[trip_id].isin(_df.loc[0:primary_purp_index].trip_id),'half'] = 1
                    trip.loc[trip[trip_id].isin(_df.loc[primary_purp_index+1:].trip_id),'half'] = 2

                    # set trip segment within half tours
                    trip.loc[trip[trip_id].isin(_df.loc[0:primary_purp_index].trip_id),'tseg'] = range(1,len(_df.loc[0:primary_purp_index])+1)
                    trip.loc[trip[trip_id].isin(_df.loc[primary_purp_index+1:].trip_id),'tseg'] = range(1,len(_df.loc[primary_purp_index+1:])+1)

                    trip.loc[trip[trip_id].isin(_df[trip_id].values),'tour'] = tour_id

                    # Extract main mode 
                    tour_dict[tour_id][tour_mode] = assign_tour_mode(_df, tour_dict, tour_id)                

                    tour_id += 1
                            

tour = pd.DataFrame.from_dict(tour_dict, orient='index')

tour.value_counts().to_csv('bad_trip_report.csv')


### Tour category based on tour type
tour['tour_type'] = tour['tour_type'].map(purpose_map).map(str)

tour['tour_category'] = 'non_mandatory'
tour.loc[tour['tour_type'].isin(['work','school']),'tour_category'] = 'mandatory'

#tour.to_csv('tour.csv', index=False)


### Cleanups
#############

###Load tour from file alternatively
###FIXME: remove
#tour = pd.read_csv('tour.csv')

# Fix me: shouldn't this be in the tour file?
expr_df = pd.read_csv(r'\\modelstation2\c$\Workspace\activitysim\activitysim\examples\example_psrc\scripts\joint_tour_expr_activitysim.csv')

for index, row in expr_df.iterrows():
    expr = 'tour.loc[' + row['filter'] + ', "' + row['result_col'] + '"] = ' + str(row['result_value'])
    print(row['index'])

    exec(expr)

## Enforce canonical tours
## There cannot be more than 2 mandatory work tours
## Create an identified for mandatory vs non-mandatory to identify trips by purpose (and include joint non-mandatory trips)
tour['mandatory_status'] = tour['tour_category'].copy()
tour.loc[tour['mandatory_status'] == 'joint', 'mandatory_status'] = 'non_mandatory'
group_cols = ['person_id', 'mandatory_status', 'tour_type']
tour['tour_type_num'] = tour.sort_values(by=group_cols).groupby(group_cols).cumcount() + 1
tour = tour.sort_values(['person_id','day','tour_category','tour_type','tlvorig'])

possible_tours = ci.canonical_tours()
possible_tours_count = len(possible_tours)
tour_num_col = 'tour_type_num'
tour['tour_type_id'] = tour.tour_type + tour['tour_type_num'].map(str)
tour.tour_type_id = tour.tour_type_id.replace(to_replace=possible_tours,
                                    value=list(range(possible_tours_count)))
tour['loc_tour_id'] = tour.tour_type + tour[tour_num_col].map(str)

# Non-numeric tour_type_id results are non-canonical and should be removed. 
# FIXME: For now just remove the offensive tours; is it okay to only use the first set of tours for these people?
# DATA FILTER: we are removing non-typical trips; do we keep other acceptable trips and tours?
filter = pd.to_numeric(tour['tour_type_id'], errors='coerce').notnull()

# Keep track of the records we removed
tour[~filter].to_csv('tours_removed_non_canoncial.csv')
tour = tour[filter]

## Set activitysim tour id
#tour.rename(columns={'tour_id': 'tour'}, inplace=True)
#tour = ci.set_tour_index(tour)
#tour['tour_id'] = tour.index.astype('int')
#tour = tour.reset_index(drop=True)

###########################################
### Joint Tour
###########################################

 #Identify joint tours from tour df
 #each of these tours occur more than once in the data (assuming more than 1 person is on this same tour in the survey)
joint_tour = 1
for index, row in tour.iterrows():
    print(row.tour_id)
    filter = (tour.day==row.day)&(tour.tour_type==row.tour_type)&(tour.topcl==row.topcl)&\
                    (tour.tdpcl==row.tdpcl)&(tour.topcl==row.topcl)&(tour.tdpcl==row.tdpcl)&\
                    (tour.tour_mode==row.tour_mode)&(tour.start==row.start)&\
                    (tour.end==row.end)&(tour.household_id==row.household_id)
                    # exclude all school, work, and escort tours per activiysim tour definitions
    # Get total number of participants (total number of matching tours) and assign a participant number
    # NOTE: this may need to be given a heirarchy of primary tour maker?
    participants = len(tour[filter])
    tour.loc[filter,'joint_tour'] = joint_tour
    tour.loc[filter,'participant_num'] = range(1,participants+1)
    joint_tour += 1

tour['participant_num'] = tour['participant_num'].fillna(0).astype('int')
# Use the joint_tour field to identify joint tour participants
# Output should be a list of people on each tour; use the tour ID of participant_num == 1
joint_tour_list = tour[tour['joint_tour'].duplicated()]['joint_tour'].values
df = tour[((tour['joint_tour'].isin(joint_tour_list)) & (~tour['joint_tour'].isnull()))]

# Drop any tours that are for work, school, or escort
df = df[~df['tour_type'].isin(['Work','School','Escort'])]
joint_tour_list = df[df['joint_tour'].duplicated()]['joint_tour'].values

# Assume Tour ID of first participant, so sort by joint_tour and person ID
df = df.sort_values(['joint_tour','person_id'])
tour = tour.sort_values(['joint_tour','person_id'])
for joint_tour in joint_tour_list:
    df.loc[df['joint_tour'] == joint_tour,'tour_id'] = df[df['joint_tour'] == joint_tour].iloc[0]['tour_id']
    # Remove other tours except the primary tour from tour file completely;
    # These will only be accounted for in the joint_tour_file
    tour = tour[~tour['tour_id'].isin(tour[tour['joint_tour'] == joint_tour].iloc[1:]['tour_id'])]
    # Set this tour as joint category
    tour.loc[tour['joint_tour'] == joint_tour,'tour_category'] = 'joint'

# Define participant ID as tour ID + participant num
df['participant_id'] = df['tour_id'].astype('str') + df['participant_num'].astype('int').astype('str')



df = df[['person_id','tour_id','household_id','participant_num','participant_id']]
#df[SURVEY_TOUR_ID] = df['tour_id'].copy()
#df.to_csv(r'survey_data\survey_joint_tour_participants.csv', index=False)
df.to_csv('joint_tour_participants.csv', index=False)

## Filter to remove any joint work mandatory trips
# FIXME: do not remove all trips, just those of the additional person and modify to be non-joint
tour = tour[~((tour['tour_type'].isin(['school','work','escort'])) & (tour['tour_category'] == 'joint'))]
tour.to_csv('tour.csv', index=False)

#####################################################################
 #infer.py (adopting from RSG script)
 #These processes need to be run after all the initial files have been generated.
#####################################################################

joint_tour_participants = pd.read_csv('joint_tour_participants.csv')
tour = pd.read_csv('tour.csv')
person = pd.read_csv('person.csv')
trips = pd.read_csv('trip.csv')
households = pd.read_csv('household.csv')

person[['person_id_str','household_id_str']] = person[['person_id','household_id']].astype('str')
person['PNUM'] = person.apply(lambda x: x['person_id_str'].replace(x['household_id_str'], '').strip(), axis=1).astype('int')

# Create new shorter person  IDs
person.rename(columns={'person_id': 'original_person_id'}, inplace=True)
person['person_id'] = range(1,len(person)+1)
person.to_csv(r'survey_data\survey_persons.csv')




# Fixme: this should be in the trip calculation, but must be added after tour info is available.
trips['outbound'] = False
trips.loc[trips['half']==1,'outbound'] = True

# Calculate person cdap activity based on tour and joint tour data
#person['cdap_activity'] = infer_cdap_activity(person, tour, joint_tour_participants)

##############################
# Tour
##############################

# Assign activitysim-specific tour ID to trips
trips['trip_num'] = trips['tseg'].copy()
trips.rename(columns={'tour': 'tour_id'}, inplace=True)

########################################
# Day
########################################

# In order to estimate, we need to enforce the mandatory tour totals
# these can only be: ['work_and_school', 'school1', 'work1', 'school2', 'work2']
# If someone has 2 work trips and 1 school trip, must decide a heirarchy of 
# which of those tours to delete

# FIXME: how do we handle people with too many mandatory tours? 
# ? Do we completely ignore all of this person’s tours, select the first tours,
# or use some other logic to identify the primary set of tours and combinations

person_day = tour.groupby('person_id').agg(['unique'])['tour_id']

person_day['flag'] = 0

# Log tour before and after FIXME
print(len(tour))

# Flag 1: person days that have 2 work and 2 school tours
filter = person_day['unique'].apply(lambda x: 'work2'  in x and 'school2' in x)
person_day.loc[filter, 'flag'] = 1
# Resolve by: dropping all work2 and school2 tours (?) FIXME...
tour = tour[~((tour['person_id'].isin(person_day[person_day['flag'] == 1].index)) & 
            tour['tour_id'].isin(['work2','school2']))]

# Flag 2: 2 work tours and 1 school tour
filter = person_day['unique'].apply(lambda x: 'work2' in x and 'school1' in x)
person_day.loc[filter, 'flag'] = 2
# Resolve by: dropping all work2 tours  (?) FIXME...
tour = tour[~((tour['person_id'].isin(person_day[person_day['flag'] == 2].index)) & 
            (tour['tour_id']=='work2'))]

# Flag 3: 2 school tours and 1 work tour
filter = person_day['unique'].apply(lambda x: 'work1' in x and 'school2' in x)
person_day.loc[filter, 'flag'] = 3
# Resolve by: dropping all school2 tours (?) FIXME...
tour = tour[~((tour['person_id'].isin(person_day[person_day['flag'] == 3].index)) & 
            (tour['tour_id']=='school2'))]

# Report number of tours affected
# FIXME: write out a log file
print(str(person_day.groupby('flag').count()))

# stop_frequency- does not include primary stop
tour['outbound_stops'] = tour['tripsh1'] - 1
tour['inbound_stops'] = tour['tripsh2'] - 1
tour['stop_frequency'] = tour['outbound_stops'].astype('int').astype('str') + 'out' + '_' + tour['inbound_stops'].astype('int').astype('str') + 'in'

# DATA FILTER: 
# Filter out tours with too many stops on their tours
df = tour[(tour['tripsh1'] > 4) | (tour['tripsh2'] > 4)]
df.to_csv('too_many_stops.csv')
logger.info(f'Dropped {len(df)} tours for too many stops')
tour = tour[~((tour['tripsh1'] > 4) | (tour['tripsh2'] > 4))]

# Borrowing this from canoncial_ids set_trip_index; FIXME should be an activitysim import when available
MAX_TRIPS_PER_LEG = 4  # max number of trips per leg (inbound or outbound) of tour

# DATA FILTER:
# select trips that only exist in tours - is this necessary or can we use the trip file directly?

# canonical_trip_num: 1st trip out = 1, 2nd trip out = 2, 1st in = 5, etc.
canonical_trip_num = (~trips.outbound * MAX_TRIPS_PER_LEG) + trips.trip_num
trips['trip_id'] = trips['tour_id'] * (2 * MAX_TRIPS_PER_LEG) + canonical_trip_num

# DATA FILTER:
# Some of these IDs are duplicated and it's not clear why - seems to be an issue with the canonical_trip_num definition
# FIXME: what do we do about this? Fix canonical_trip_num? drop duplicates?
duplicated_person = trips[trips['trip_id'].duplicated()]['person_id'].unique()
logger.info(f'Dropped {len(duplicated_person)} persons: duplicate IDs from canonical trip num definition')
trips = trips[~trips['person_id'].isin(duplicated_person)]
trips.set_index('trip_id', inplace=True, drop=False, verify_integrity=True)

# Make sure all trips in a tour have an outbound and inbound component
#trips_per_tour = trips.groupby('tour_id')['person_id'].value_counts()
#missing_trip_persons = trips_per_tour[trips_per_tour == 1].index.get_level_values('person_id').to_list()
#logger.info(f'Dropped {len(missing_trip_persons)} persons: missing an outbound or inbound trip leg')
#drop_list += missing_trip_persons

req_cols = ['trip_id','person_id','trip_num','household_id','outbound','purpose','destination','origin','depart','trip_mode','tour_id','original_person_id']
trips['trip_mode'] = trips['mode']
#trips[SURVEY_TOUR_ID] = trips['tour']

trips = trips.merge(person[['person_id','original_person_id']], left_on='person_id', right_on='original_person_id', how='left')
trips.rename(columns={'person_id_y': 'person_id'}, inplace=True)
trips = trips[req_cols]
trips.to_csv(r'survey_data\survey_trips.csv', index=False)


# Also write out the modified tour file
#tour[SURVEY_TOUR_ID] = tour['tour_id'].copy()
tour = tour.merge(person[['person_id','original_person_id']], left_on='person_id', right_on='original_person_id', how='left')
tour.rename(columns={'person_id_y': 'person_id'}, inplace=True)
tour.drop('person_id_x', axis=1, inplace=True)
tour.to_csv(r'survey_data\survey_tours.csv', index=False)

######### FINAL CLEAN UP 
###joint_tour_participants = pd.read_csv('joint_tour_participants.csv')
###tour = pd.read_csv('tour.csv')
####person = pd.read_csv('person.csv')
###trips = pd.read_csv('trip.csv')
###households = pd.read_csv('household.csv')

#joint_tour_participants = pd.read_csv(r'survey_data\survey_joint_tour_participants.csv')
#tour = pd.read_csv(r'survey_data\survey_tours.csv')
households = pd.read_csv(r'survey_data\survey_households.csv')
person = pd.read_csv(r'survey_data\survey_persons.csv')
#trips = pd.read_csv(r'survey_data\survey_trips.csv')

# Make sure that 

### We cannot have more than 2 joint tours per household. If so, make sure we remove those households/tours
### FIXME: should we remove the households or edit the tours so they are not joint, or otherwise edit them?
joint_tours = tour[tour['tour_category'] == 'joint']
_df = joint_tours.groupby('household_id').count()['tour_id']
too_many_jt_hh = _df[_df > 2].index

# FIXME: For now remove all households; there are 4
# We should figure out how to better deal with these
tour = tour[~tour['household_id'].isin(too_many_jt_hh)]
#joint_tour_participants = joint_tour_participants[~joint_tour_participants['household_id'].isin(too_many_jt_hh)]
#households = households[~households['household_id'].isin(too_many_jt_hh)]
#person = person[~person['household_id'].isin(too_many_jt_hh)]
#trips = trips[~trips['household_id'].isin(too_many_jt_hh)]
# We may also need to remove the associated trips, households, and persons?

# Make sure all joint tour IDs are available in tour file
joint_tour_participants = joint_tour_participants[joint_tour_participants['tour_id'].isin(tour['tour_id'])]

# Make sure trips and tours align
trips = trips[trips['tour_id'].isin(tour['tour_id'])]

# Modify person ID
joint_tour_participants = joint_tour_participants[joint_tour_participants['tour_id'].isin(tour['tour_id'])]
joint_tour_participants = joint_tour_participants.merge(person[['person_id','original_person_id']], left_on='person_id', right_on='original_person_id', how='left')
joint_tour_participants.rename(columns={'person_id_y': 'person_id'}, inplace=True)
joint_tour_participants.drop('person_id_x', axis=1, inplace=True)
joint_tour_participants.to_csv(r'survey_data\survey_joint_tour_participants.csv', index=False)


joint_tour_participants.to_csv(r'survey_data\survey_joint_tour_participants.csv', index=False)
tour.to_csv(r'survey_data\survey_tours.csv', index=False)
households.to_csv(r'survey_data\survey_households.csv', index=False)
person.to_csv(r'survey_data\survey_persons.csv', index=False)
trips.to_csv(r'survey_data\survey_trips.csv', index=False)

# Not sure why infer.py requires the final tables, write them out for now so we can use default script settings
joint_tour_participants.to_csv(r'survey_data\final_joint_tour_participants.csv', index=False)
tour.to_csv(r'survey_data\final_tours.csv', index=False)
households.to_csv(r'survey_data\final_households.csv', index=False)
person.to_csv(r'survey_data\final_persons.csv', index=False)
trips.to_csv(r'survey_data\final_trips.csv', index=False)

# Conclude log
end_time = datetime.datetime.now()
elapsed_total = end_time - start_time
logger.info('--------------------RUN ENDING--------------------')
logger.info('TOTAL RUN TIME %s'  % str(elapsed_total))