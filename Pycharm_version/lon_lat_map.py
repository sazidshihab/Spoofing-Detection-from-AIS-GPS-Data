
def main_loc_mul():
    from lon_lat_multiprocess import main

    loc_spoof, data1, len1, time = main()
    import folium
    import pandas as pd

    loc_spoof = loc_spoof.drop_duplicates()
    data000 = data1.loc[data1.index.isin(loc_spoof['index'])]

    spoof_loc_data_tup = tuple(data000.groupby(data000['MMSI']))

    for i in range(len(spoof_loc_data_tup)):
        data00 = spoof_loc_data_tup[i][1]

        # Center the map
        m = folium.Map(location=[data00['Latitude'].mean(), data00['Longitude'].mean()], zoom_start=2)

        coords = list(zip(data00['Latitude'], data00['Longitude']))
        folium.PolyLine(locations=coords, color='red', weight=3).add_to(m)
        for _, row in data00.iterrows():
            popup_text = f"""
                                        Time: {row['Timestamp'].strftime('%Y-%m-%d %H:%M')}<br>
                                        Lat: {row['Latitude']}<br>
                                        Lon: {row['Longitude']}
                                        """
            folium.Marker(
                location=[row['Latitude'], row['Longitude']],
                popup=popup_text,
                icon=folium.Icon(color='blue', icon='info-sign')
            ).add_to(m)

        f_name = f"/Users/sazid/Documents/vinlus dataset/GPS Spoofing Detection with Parallel Computing/Python Code/Spoofing-Detection-from-AIS-GPS-Data/Multiprocess on GPS Spoofing/Lat_lon spoof/spoofing_map_{data00.iloc[0]['MMSI']}.html"
        m.save(f_name)

    return len1,time



if __name__ == '__main__':
    len1, time = main_loc_mul()

