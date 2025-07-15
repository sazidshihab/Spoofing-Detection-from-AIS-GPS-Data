if __name__ == '__main__':
    from lon_lat_map import main_loc_mul
    from lon_lat_sequential import main_loc_seq
    from course_sequential import main_course_seq
    from course_map import main_course_mul



    while True:
            print('To START Sequential Location anomaly detection--> type "sequential loc" \n')
            print('To START Multiprocess Location anomaly detection and create map data--> type "multi loc" \n')
            print('To START Sequential Course anomaly detection--> type "sequential course" \n')
            print('To START Multiprocess Course anomaly detection and create map data--> type "multi course" \n\n')

            inpt = input('TYPE HERE...\n')

            if inpt.lower()=='sequential loc':
                print('      ***Location anomaly detection has started(Sequential),...*** \n')
                loc_seq_count, loc_seq_time = main_loc_seq()
                print('      \n->Time needed to complete : ', round(loc_seq_time, 2), ' Sec', ' and found: ', loc_seq_count,
                      ' anomalies.\n')
        
            if inpt.lower()=='multi loc':
                print('      ***Location anomaly detection has started(Multiprocess+map),...*** \n')
                loc_mul_count, loc_mul_time = main_loc_mul()
                print('      \n->Time needed to complete : ', round(loc_mul_time,2), ' Sec',' and found: ', loc_mul_count, ' anomalies.\nAnd location file imported as html\n' )

            if inpt.lower()=='sequential course':
                print('      ***Course anomaly detection has started(Sequential),...*** \n')
                course_seq_count, course_seq_time = main_course_seq()
                print('      \n->Time needed to complete : ', round(course_seq_time,2), ' Sec',' and found: ', course_seq_count, ' anomalies.\n' )


            if inpt.lower()=='multi course':
                print('      ***Course anomaly detection has started(Multiprocess+map),...*** \n')
                course_mul_count, course_mul_time = main_course_mul()
                print('      \n->Time needed to complete : ', round(course_mul_time, 2), ' Sec', ' and found: ', course_mul_count,
                      ' anomalies.\nAnd location file imported as html\n')

            if inpt.lower()=='exit':
                break

            print("\n" * 100)
