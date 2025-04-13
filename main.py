from data_acquisition import *

## You can pass a list with multiple days
day_list = ["2025-03-10"]

### REPLACE WITH YOUR API KEY
api_key = ""

for day in day_list:
    get_day(day,api_key, in_relevant_lines = ['53','61','74'])