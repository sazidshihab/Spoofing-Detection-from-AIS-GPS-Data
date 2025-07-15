def main_course_seq():
    from  Data_preprocess import load_and_prepare_data
    import pandas as pd
    import time
    import sys


    from tabulate import tabulate
    data1, chunk, start = load_and_prepare_data()

    count_course_individual_flag = []
    count_course_sequence_flag = []
    count_course_sequence_3_flag = []
    count_course_sequence_sog_2_10_flag=[]
    MMSI = []
    index1=[]

    course_anomaly_container = pd.DataFrame()
    print('         Searching for COURSE anomalies....\n')
    start_time = time.time()
    for i in range(len(chunk)):
        container1 = chunk[i]
        for j in range(len(container1)):


            mmsi, data2 = container1[j]
            count = 0
            course = 0
            course1 = 0
            course2 = 0
            time1 = 0
            time2 = 0
            timem = 0
            sog = 0
            sog1 = 0
            sog2 = 0
            sog_add = 0
            count_course_flag = 0
            count_course_flag_for_final = 0
            count_course_flag_final = 0
            count_course_flag_for_final_1 = 0
            count_course_flag_final_1 = 0
            index = 0
            prev_index = 0
            count_flag_add = 0
            course_add, time_add = 0, 0
            count_course_flag_4t = 0

            elapsed = time.time() - start_time
            sys.stdout.write(f'\r⏳ Detecting... Time elapsed: {int(elapsed)}s')
            sys.stdout.flush()
            time.sleep(1)


            for i2, row in data2.iterrows():
                count += 1
                course1 = row['COG']
                time1 = row['Timestamp']
                sog1 = row['SOG']

                if count >= 2:
                    course = abs((course1 - course2 + 180) % 360 - 180)
                    sog = abs(sog2 - sog1) / 2
                    timem = (time1 - time2).total_seconds() / 60
                    timem = abs(timem)

                    # calculating 5 consecutive data and averaging them:
                    course_add += course
                    time_add += timem
                    sog_add += sog

                if (course_add > time_add * 35) and (sog_add / 5 > 10) and (timem >= 0.5) and (
                        count % 5 == 0):  # avg val of 5 concurrent
                    count_flag_add += 1
                    index1.append((data2.index[count - 1], 1))
                    index1.append((data2.index[count - 6], 1))

                if (course > timem * 35) and (timem >= 0.2):

                    index = data2.index[count - 2]

                    count_course_flag += 1

                    if (prev_index == index) and sog < 2:

                        count_course_flag_for_final += 1
                        if count_course_flag_for_final == 15:
                            count_course_flag_final += 1
                            count_course_flag_for_final = 0
                            for z in range(1, 16, 3):
                                index1.append((data2.index[count - z], 2))

                    if (prev_index == index) and (sog <= 10) and (sog >= 2):

                        count_course_flag_for_final_1 += 1
                        if count_course_flag_for_final_1 == 3:
                            count_course_flag_final_1 += 1
                            count_course_flag_for_final_1 = 0
                            for z in range(1, 4):
                                index1.append((data2.index[count - z], 3))

                    if prev_index != index and count_course_flag_for_final != 1:
                        count_course_flag_for_final = 0

                    if prev_index != index and count_course_flag_for_final_1 != 1:
                        count_course_flag_for_final_1 = 0

                    prev_index = data2.index[count - 1]

                if count % 5 == 0:
                    course_add, time_add, sog_add = 0, 0, 0

                time2 = time1
                course2 = course1
                sog2 = sog1
            # print('count_course_flag :', count_course_flag,  'count_course_flag_final:   ', count_course_flag_final, 'count_flag_add: ', count_flag_add)
            count_course_individual_flag.append(count_course_flag)
            count_course_sequence_flag.append(count_course_flag_final)
            count_course_sequence_3_flag.append(count_flag_add)
            count_course_sequence_sog_2_10_flag.append(count_course_flag_final_1)
            MMSI.append(row['MMSI'])


    course_anomaly_container['MMSI']=MMSI
    course_anomaly_container['Total flag, 1/row']= count_course_individual_flag
    course_anomaly_container['While speed<2, 15/row(2)'] = count_course_sequence_flag
    course_anomaly_container['While sog>10,5 avg/row(1)'] = count_course_sequence_3_flag
    course_anomaly_container['While 10>speed>2, 3/row(3)'] = count_course_sequence_sog_2_10_flag

    print('\n')
    with pd.option_context('Display.max_rows', None, 'Display.max_columns', None):
      print(tabulate(course_anomaly_container[(course_anomaly_container['While speed<2, 15/row(2)']>=1) |
              ((course_anomaly_container['While sog>10,5 avg/row(1)']>=1) |
              (course_anomaly_container['While 10>speed>2, 3/row(3)']>=1))],headers='keys',floatfmt=".0f"))
    

    container = course_anomaly_container[(course_anomaly_container['While speed<2, 15/row(2)']>=1) |
              ((course_anomaly_container['While sog>10,5 avg/row(1)']>=1) |
              (course_anomaly_container['While 10>speed>2, 3/row(3)']>=1))]
    len1 = len(container)


    #storing indexes and categorical val
    loc_spoof1 = pd.DataFrame(index1, columns=['index', 'div'])
    loc_spoof1 = loc_spoof1.drop_duplicates(subset='index').set_index('index', drop=False)
    data0001 = data1.loc[data1.index.isin(loc_spoof1['index'])].copy()
    data0001['div'] = data0001.index.map(loc_spoof1['div'])


    return len1,time.time()-start



if __name__ =='__main__':
    main_course_seq()