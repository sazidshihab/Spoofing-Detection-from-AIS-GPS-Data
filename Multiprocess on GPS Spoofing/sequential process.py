
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



data1 = data1[((data1['Latitude'] <= 90) & (data1['Latitude'] >= -90)) & (
            (data1['Longitude'] <= 180) & (data1['Longitude'] >= -180))]


data1 = data1.sort_values(by=['MMSI', 'Timestamp'])





groups = tuple(data1.groupby('MMSI'))


chunk_size = 10



chunk = [groups[i:i + chunk_size] for i in range(0, len(groups), chunk_size)]




len(chunk)




loc_anamoly_container = pd.DataFrame()
mmsi_ship = []
total_records_ship = []
flag_count_ship = []
mega_jump_ship = []
final_flag_ship = []
final_jump_count_ship = []



container = chunk[0]
mmsi, data2 = container[0]




for i in range(len(chunk) - 1):
    for j in range(len(container)):
        container = chunk[i]
        mmsi, data2 = container[j]

        time2 = None
        lat1 = None
        lon1 = None
        count = 0
        flag_count = 0
        final_flag_count = 0
        jump_count = 0
        flag_sequence = 0
        flag_sequence_count = 0
        name = 0
        ob1 = None
        jump_sequence = None
        jump_sequence_count = None
        final_jump_count = 0

        for i2, row in data2.iterrows():
            count += 1

            lat = row['Latitude']
            lon = row['Longitude']
            point = (lat, lon)
            time1 = row['Timestamp']
            speed = row['SOG'] * 1.852

            if count >= 2:
                time_dif = (time1 - time2).total_seconds() / 60  # in minutes
                dis = geodesic(point, point1).kilometers
                max_can_go = 1 * time_dif  # as at 30 knot(.92 km/m) speed, ship can go 1km max in 1 minutes
                # print('Distance: ', dis,'KM','  Time difference: ',time_dif, 'maximum distance can cover in this time: ', max_can_go, 'Actual speed: ',
                # speed,
                # 'with actual speed should go: ', speed/60 * time_dif, time1)

                # detecting mid level anamolies
                if (dis > max_can_go * 3) and (dis < max_can_go * 50):

                    flag_count += 1
                    ob = data.index[count] - 1
                    # print(ob)

                    if (flag_sequence == ob):
                        flag_sequence_count += 1
                        # print('scon',flag_sequence_count)

                    if (flag_sequence != ob):
                        flag_sequence_count = 0

                    if flag_sequence_count >= 2:
                        final_flag_count += 1
                        flag_sequence_count = 0

                    # print(flag_sequence)
                    flag_sequence = data.index[count]

                    # print('flag  ')

                    # detecting jump (serious)

                if dis > max_can_go * 50:
                    jump_count += 1
                    ob1 = data.index[count] - 1

                    if (jump_sequence == ob1):
                        jump_sequence_count += 1

                    if (jump_sequence != ob1):
                        jump_sequence_count = 0

                    if jump_sequence_count >= 1:
                        final_jump_count += 1
                        jump_sequence_count = 0

                    jump_sequence = data.index[count]

                    # print('Mega Jump  ')

            time2 = time1
            lat1 = lat
            lon1 = lon
            point1 = (lat1, lon1)

        # print('Total flag: ', flag_count,'  Mega Jump: ',jump_count, count)
        # print('final_flag: ', final_flag_count,'\n\n\n', i ,'\n',j)

        mmsi_ship.append(mmsi)
        total_records_ship.append(count)
        flag_count_ship.append(flag_count)
        mega_jump_ship.append(jump_count)
        final_flag_ship.append(final_flag_count)
        final_jump_count_ship.append(final_jump_count)


loc_anamoly_container['mmsi_ship'] = mmsi_ship
loc_anamoly_container['total_records_ship'] = total_records_ship
loc_anamoly_container['flag_count_ship'] = flag_count_ship
loc_anamoly_container['mega_jump_ship'] = mega_jump_ship
loc_anamoly_container['final_flag_ship'] = final_flag_ship
loc_anamoly_container['final_jump_count_ship'] = final_jump_count_ship



with pd.option_context('Display.max_rows', None):
    print(loc_anamoly_container[(loc_anamoly_container['final_flag_ship'] >= 1) | (
                loc_anamoly_container['final_jump_count_ship'] >= 1)])
end = time.time()

print('Total time : ', start-end)

