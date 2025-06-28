from Data_preprocess import chunk
import pandas as pd

from tabulate import tabulate




count_course_individual_flag = []
count_course_sequence_flag = []
count_course_sequence_3_flag = []
MMSI = []

course_anomaly_container = pd.DataFrame()

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
        time = 0
        count_course_flag = 0
        count_course_flag_for_final = 0
        count_course_flag_final = 0
        index = 0
        prev_index = 0
        count_flag_add = 0
        course_add, time_add = 0, 0
        count_course_flag_4t = 0

        for i2, row in data2.iterrows():
            count += 1
            course1 = row['COG']
            time1 = row['Timestamp']


            if count >= 2:
                course = abs((course1 - course2 + 180) % 360 - 180)

                time = (time1 - time2).total_seconds() / 60
                time = abs(time)


                # calculating 5 consecutive data and averaging them:
                course_add += course
                time_add += time
                if count % 8 == 0:

                    course_add, time_add = 0, 0

            if course_add > time_add * 35:
                count_flag_add += 1

            if course > time * 30:


                index = data2.index[count - 2]

                count_course_flag += 1

                if prev_index == index:

                    count_course_flag_for_final += 1
                    if count_course_flag_for_final == 2:
                        count_course_flag_final += 1
                        count_course_flag_for_final = 0


                if prev_index != index and count_course_flag_for_final != 1:
                    count_course_flag_for_final = 0

                prev_index = data2.index[count - 1]

            time2 = time1
            course2 = course1

        count_course_individual_flag.append(count_course_flag)
        count_course_sequence_flag.append(count_course_flag_final)
        count_course_sequence_3_flag.append(count_flag_add)
        MMSI.append(row['MMSI'])


course_anomaly_container['MMSI']=MMSI
course_anomaly_container['count_course_individual_flag']= count_course_individual_flag
course_anomaly_container['count_course_sequence_3in_a_row_flag'] = count_course_sequence_flag
course_anomaly_container['count_course_sequence_8_time_average_flag'] = count_course_sequence_3_flag

with pd.option_context('Display.max_rows', None, 'Display.max_columns', None):
  print(tabulate(course_anomaly_container[(course_anomaly_container['count_course_sequence_3in_a_row_flag']>=3) & (course_anomaly_container['count_course_sequence_8_time_average_flag']>=10)],headers='keys', tablefmt='psql',floatfmt=".0f"))
