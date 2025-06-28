import multiprocessing
import time
import pandas as pd

from geopy.distance import geodesic

# In[2]:

start = time.time()

from Data_preprocess import chunk



container = pd.DataFrame()


def chunk_process(chunk):
    global count2

    loc_anamoly_container = pd.DataFrame()
    mmsi_ship = []
    total_records_ship = []
    flag_count_ship = []
    mega_jump_ship = []
    final_flag_ship = []
    final_jump_count_ship = []
    for i in range(len(chunk)):
      container=chunk[i]
      mmsi, data2 = container
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
                  ob = data2.index[count-2]
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
                  flag_sequence = data2.index[count-1]

                  # print('flag  ')

                  # detecting jump (serious)

              if dis > max_can_go * 50:
                  jump_count += 1
                  ob1 = data2.index[count-2]

                  if (jump_sequence == ob1):
                      jump_sequence_count += 1

                  if (jump_sequence != ob1):
                      jump_sequence_count = 0

                  if jump_sequence_count >= 1:
                      final_jump_count += 1
                      jump_sequence_count = 0

                  jump_sequence = data2.index[count-1]

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


    return loc_anamoly_container

def main():

    with multiprocessing.Pool(processes=7) as pool:

        results = pool.map(chunk_process, chunk)
    end = time.time()
    global container
    container = pd.concat(results,ignore_index=True )
    with pd.option_context('Display.max_rows', None):
      print(container[(container['final_flag_ship']>=1) | (container['final_jump_count_ship']>=1) ])
    print(end-start)

if __name__ == "__main__":
    main()


