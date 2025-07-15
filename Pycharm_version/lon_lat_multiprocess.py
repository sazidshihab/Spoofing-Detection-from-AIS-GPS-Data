import pandas as pd
from geopy.distance import geodesic
import time
import sys
start_time = time.time()

container = pd.DataFrame()
loc_spoof = pd.DataFrame()


def chunk_process(chunk):

    elapsed = time.time() - start_time
    sys.stdout.write(f'\r⏳ Detecting... Time elapsed: {int(elapsed)}s')
    sys.stdout.flush()
    time.sleep(1)



    loc_anamoly_container = pd.DataFrame()
    loc_spoof1 = pd.DataFrame()
    mmsi_ship = []
    total_records_ship = []
    flag_count_ship = []
    mega_jump_ship = []
    final_flag_ship = []
    final_jump_count_ship = []
    index = []
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
      time_10 = 0
      dis_10, dis, max_can_go = 0, 0, 0
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
              dis = abs(geodesic(point, point1).kilometers)
              max_can_go = 1 * time_dif
              # as at 30 knot(.92 km/m) speed, ship can go 1km max in 1 minutes
              time_10 += time_dif
              dis_10 += dis

          if count % 10 == 0:
              time_10, dis_10 = 0, 0

          if (dis > max_can_go * 3):
              flag_count += 1
              # detecting mid level anamolies
              if ((dis_10 / 10) > ((time_10 / 10) * 50)) and ((time_10 / 10) >= .5):

                  ob = count-1
                  # print(ob)

                  if (flag_sequence == ob):
                      flag_sequence_count += 1
                      # print('scon',flag_sequence_count)

                  if (flag_sequence != ob):
                      flag_sequence_count = 0

                  if flag_sequence_count >= 1:
                      final_flag_count += 1
                      flag_sequence_count = 0
                      index.append(data2.index[count - 1])
                      index.append(data2.index[count - 2])

                  flag_sequence = count

                  # detecting jump (serious)

              if (dis > max_can_go * 70) and (time_dif >= 0.25) and (max_can_go > 0):
                  jump_count += 1
                  ob1 = count - 1

                  if (jump_sequence == ob1):
                      jump_sequence_count += 1

                  if (jump_sequence != ob1):
                      jump_sequence_count = 0

                  if jump_sequence_count >= 1:
                      final_jump_count += 1
                      jump_sequence_count = 0
                      index.append(data2.index[count - 1])
                      index.append(data2.index[count - 2])

                  jump_sequence = count

          time2 = time1
          lat1 = lat
          lon1 = lon
          point1 = (lat1, lon1)

      mmsi_ship.append(mmsi)
      flag_count_ship.append(flag_count)
      mega_jump_ship.append(jump_count)
      final_flag_ship.append(final_flag_count)
      final_jump_count_ship.append(final_jump_count)

    loc_anamoly_container['MMSI'] = mmsi_ship
    loc_anamoly_container['Total Flag entered'] = flag_count_ship
    loc_anamoly_container['final_flag_10_avg, 1/row'] = final_flag_ship
    loc_anamoly_container['final_flag>*50, 1/row'] = final_jump_count_ship
    loc_spoof1['index']=index

    return loc_anamoly_container,loc_spoof1



def main():
    multiprocessing, tabulate, time, load_and_prepare_data = call_pack()  # loading all packages
    data1, chunk, start = load_and_prepare_data()


    with multiprocessing.Pool(processes=7) as pool:

        results = pool.map(chunk_process, chunk)

    anamoly_list, spoof_list = zip(*results)

    # Merge both
    global container, loc_spoof
    container = pd.concat(anamoly_list, ignore_index=True)
    loc_spoof = pd.concat(spoof_list, ignore_index=True)
    '''
    with pd.option_context('Display.max_rows', None):
      print(tabulate(container[(container['final_flag_10_avg, 1/row']>=1) |
                                (container['final_flag>*50, 1/row']>=1)], headers='keys',floatfmt=".0f" ))
    '''
    container = container[(container['final_flag_10_avg, 1/row']>=1) |
                                (container['final_flag>*50, 1/row']>=1)]
    len1=len(container)
    end = time.time()
    return loc_spoof,data1,len1, end-start


def call_pack():
    import multiprocessing as multiprocessing
    from tabulate import tabulate as tabulate
    import time as time
    from Data_preprocess import load_and_prepare_data as load_and_prepare_data


    return multiprocessing,tabulate,time,load_and_prepare_data





if __name__ == "__main__":
    loc_spoof,data1,len1,time=main()



