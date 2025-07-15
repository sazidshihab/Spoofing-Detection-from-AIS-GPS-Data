def main_course_mul():
    import folium
    import pandas as pd
    from course_multiprocess import main
    loc_spoof, data1,time = main()

    loc_spoof = pd.DataFrame(loc_spoof, columns=['index', 'div'])
    loc_spoof = loc_spoof.drop_duplicates(subset='index').set_index('index', drop=False)
    data0001 = data1.loc[data1.index.isin(loc_spoof['index'])].copy()
    data0001['div'] = data0001.index.map(loc_spoof['div'])
    len1 = data0001['MMSI'].nunique()

    spoof_loc_data_tup = tuple(data0001.groupby(data0001['MMSI']))

    for i in range(len(spoof_loc_data_tup)):

        data00 = spoof_loc_data_tup[i][1]
        data001 = data00[data00['div'] == 1]
        data002 = data00[data00['div'] == 2]
        data003 = data00[data00['div'] == 3]



        try:
             if not data001.empty:
                m1 = folium.Map(location=[data001['Latitude'].mean(), data001['Longitude'].mean()], zoom_start=2)

                # Add line connecting spoofed points
                coords = list(zip(data001['Latitude'], data001['Longitude']))
                folium.PolyLine(locations=coords, color='red', weight=4, opacity=0.7).add_to(m1)

                # Add markers with timestamp
                for _, row in data001.iterrows():
                    popup_text = f"""
                                            Time: {row['Timestamp'].strftime('%Y-%m-%d %H:%M')}<br>
                                            COG: {row['COG']} 
                                            """

                    folium.Marker(
                        location=[row['Latitude'], row['Longitude']],
                        popup=popup_text,
                        icon=folium.Icon(color='blue', icon='info-sign')
                    ).add_to(m1)

                f_name = f"/Users/sazid/Documents/vinlus dataset/GPS Spoofing Detection with Parallel Computing/Python Code/Spoofing-Detection-from-AIS-GPS-Data/Multiprocess on GPS Spoofing/course_spoof/avg_val_spoof_{data001.iloc[0]['MMSI']}.html"
                m1.save(f_name)

             if not data002.empty:
                m2 = folium.Map(location=[data002['Latitude'].mean(), data002['Longitude'].mean()], zoom_start=2)

                # Add line connecting spoofed points
                coords = list(zip(data002['Latitude'], data002['Longitude']))
                folium.PolyLine(locations=coords, color='red', weight=4, opacity=0.7).add_to(m2)

                # Add markers with timestamp
                for _, row in data002.iterrows():
                    popup_text = f"""
                         Time: {row['Timestamp'].strftime('%Y-%m-%d %H:%M')}<br>
                       COG: {row['COG']} 
                             """

                    folium.Marker(
                        location=[row['Latitude'], row['Longitude']],
                        popup=popup_text,
                        icon=folium.Icon(color='blue', icon='info-sign')
                    ).add_to(m2)

                f_name = f"/Users/sazid/Documents/vinlus dataset/GPS Spoofing Detection with Parallel Computing/Python Code/Spoofing-Detection-from-AIS-GPS-Data/Multiprocess on GPS Spoofing/course_spoof/gps_spoof_{data002.iloc[0]['MMSI']}.html"
                m2.save(f_name)

             if not data003.empty:
                m3 = folium.Map(location=[data003['Latitude'].mean(), data003['Longitude'].mean()], zoom_start=2)

                # Add line connecting spoofed points
                coords = list(zip(data003['Latitude'], data003['Longitude']))
                folium.PolyLine(locations=coords, color='red', weight=4, opacity=0.7).add_to(m3)

                # Add markers with timestamp
                for _, row in data003.iterrows():
                    popup_text = f"""
                             Time: {row['Timestamp'].strftime('%Y-%m-%d %H:%M')}<br>
                             COG: {row['COG']} 
                             """

                    folium.Marker(
                        location=[row['Latitude'], row['Longitude']],
                        popup=popup_text,
                        icon=folium.Icon(color='blue', icon='info-sign')
                    ).add_to(m3)

                f_name = f"/Users/sazid/Documents/vinlus dataset/GPS Spoofing Detection with Parallel Computing/Python Code/Spoofing-Detection-from-AIS-GPS-Data/Multiprocess on GPS Spoofing/course_spoof/mid_val_spoof_{data003.iloc[0]['MMSI']}.html"
                m3.save(f_name)


             data001 = pd.DataFrame()
             data002 = pd.DataFrame()
             data003 = pd.DataFrame()


        except Exception:
            continue


    return len1,time


if __name__ == '__main__':
            main_course_mul()