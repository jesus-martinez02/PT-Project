from data_acquisition import *

## You can pass a list with multiple days
day_list = ["2025-03-19","2025-03-20"]

### REPLACE WITH YOUR API KEY
api_key = "n6pVu1SOs8uP1U7pfDRf3GLl7uPwQr_h-RwQecqNTWk"

#for day in day_list:
#    get_day(day,api_key, in_relevant_lines = ['53','61','74'])


file_list = ['2025-03-10.csv','2025-03-11.csv','2025-03-12.csv','2025-03-13.csv',
             '2025-03-17.csv','2025-03-18.csv','2025-03-19.csv','2025-03-20.csv',]

merge_day_files(file_list,'full_dataset.csv')

#df = my_read_df('first_week.csv')
#print(df.head())