from aot_client import AotClient
import pandas as pd
import schedule
import time
from datetime import datetime
import os
fom . import config


def check_current_timestamp():
    chicago_dataset = AotClient().list_observations()
    for observation in chicago_dataset:
        timestamp = set()
        for item in range(len(observation.data)):
            #create a set
            current_timestamp = observation.data[item]["timestamp"]
            timestamp.add(current_timestamp)
        return timestamp, observation
        

        
def create_dataframe():
    current_time, observation = check_current_timestamp()
    assert observation.data != {}
    observ_list =[]
    for item in current_time:
        for index in range(len(observation.data)):
            if item == observation.data[index]["timestamp"]:
                data = {
                    "time_stamp": item,
                    "value":observation.data[index]['value'],
                    "uom": observation.data[index]['uom'],
                    "sensor_path": observation.data[index]['sensor_path'],
                    "node_vsn": observation.data[index]["node_vsn"],
                    "latitude": observation.data[index]["location"]["geometry"]["coordinates"][1],
                    "longitude": observation.data[index]["location"]["geometry"]["coordinates"][0]
                }
                observ_list.append(data)
                  
    return pd.DataFrame(observ_list) 
        

def save_df():
    df = create_dataframe()
    
    df.to_csv(config.FILE_WRITE_PATH)
    print(f"saved data as CSV to: {config.FILE_WRITE_PATH} ")


def create_cumulative_df():
    """
    remove previous cumulative file and replace 
    with the updated cumulative file
    """
    file_path = config.FILE_READ_PATH
    
    if not os.path.exists(file_path):
        df = create_dataframe()
       
        df.to_csv(config.FILE_WRITE_PATH)
        print(f"saved data as CSV to: {config.FILE_WRITE_PATH} ")
        
    existing_df = pd.read_csv(file_path)
    new_df = create_dataframe()
    cumulative_df = (pd.concat([pd.read_csv(file_path), create_dataframe()], axis=0, join = "inner", ignore_index=True, copy = False).drop_duplicates())
    
    if os.path.exists(file_path):
        os.remove(file_path)
        
    cumulative_df.to_csv(file_path)
    
    print("sucessfully updated the file")
    print(f"time_stmps in latest batch of data:{check_current_timestamp()}")
    
if __name__ =="__main__":
    schedule.every(config.SCHEDULE_IN_MIN).minutes.do(create_cumulative_df)

while True:
    schedule.run_pending()
    time.sleep(1)

