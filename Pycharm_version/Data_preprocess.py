import time
import pandas as pd
import glob
import os
from geopy.distance import geodesic

print('         Data pre_processing started........\n')

def load_and_prepare_data(chunk_size=10):
        start = time.time()



        path = '/Users/sazid/Documents/vinlus dataset/GPS Spoofing Detection with Parallel Computing/Data'


        print('         Data pre_processing........\n')


        file_list = glob.glob(os.path.join(path,'*csv'))

        data_on_list = [pd.read_csv(path1) for path1 in file_list]
        data = pd.concat(data_on_list, ignore_index=True)



        data1 = data.rename(columns={'# Timestamp': 'Timestamp'})
        data1['Timestamp'] = pd.to_datetime(data1['Timestamp'], format='%d/%m/%Y %H:%M:%S')
        data1['ETA'] = pd.to_datetime(data1['ETA'], format='%d/%m/%Y %H:%M:%S', errors='coerce')


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


        chunk = [groups[i:i + chunk_size] for i in range(0, len(groups), chunk_size)]

        print('         Data pre_processing ended........\n')
        print('         Original Data size: ',len(data), ' rows.')
        print('         Pre-processed Data size: ', len(data1), ' rows.')
        print('         Total Ship: ', data['MMSI'].nunique(),' \n ')

        return data1, chunk, start