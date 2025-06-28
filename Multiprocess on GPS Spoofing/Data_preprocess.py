import time
import pandas as pd

from geopy.distance import geodesic



start = time.time()

data = pd.read_csv(
    '/Users/sazid/Documents/vinlus dataset/GPS Spoofing Detection with Parallel Computing/aisdk-2006-03/aisdk_20060302.csv')




data1 = data.rename(columns={'# Timestamp': 'Timestamp'})
data1['Timestamp'] = pd.to_datetime(data1['Timestamp'], format='%d/%m/%Y %H:%M:%S')
data1['ETA'] = pd.to_datetime(data1['ETA'], format='%d/%m/%Y %H:%M:%S', errors='coerce')




with pd.option_context('display.max_rows', None, 'display.max_columns', None):
     '''
     print(data1[

                data1['ROT'].isnull() &
                data1['SOG'].isnull() &
                data1['Navigational status'].str.contains('Unknown value', case=False, na=False)

                 ]) '''




data1 = data1[~(

        data1['ROT'].isnull() &
        data1['SOG'].isnull() &
        data1['Navigational status'].str.contains('Unknown value', case=False, na=False)

)]




data1 = data1.drop(['IMO', 'Callsign', 'Cargo type', 'Ship type'], axis=1)




data1['SOG'].isnull().sum()




data1['SOG'] = data1['SOG'].fillna(0)

data1['COG'] = data1['COG'].fillna(data1['COG'].mean())

data1 = data1[((data1['Latitude'] <= 90) & (data1['Latitude'] >= -90)) & (
            (data1['Longitude'] <= 180) & (data1['Longitude'] >= -180))]


data1 = data1.sort_values(by=['MMSI', 'Timestamp'])





groups = tuple(data1.groupby('MMSI'))


chunk_size = 10



chunk = [groups[i:i + chunk_size] for i in range(0, len(groups), chunk_size)]
