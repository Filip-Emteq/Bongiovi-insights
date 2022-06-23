import json
import re
import pandas as pd
from io import StringIO
import datetime
import time
import numpy as np
import os

def add_meta_to_head(data_file_path, meta_file_path, output_file_path):
    filenames = [meta_file_path, data_file_path]
    with open(output_file_path, 'w') as outfile:
        for fname in filenames:
            with open(fname) as infile:
                outfile.write(infile.read())

def save_data(experience_data, calibration_data, metadata, path_to_single_csv):
    '''
    Parameters
    ----------
    experience_data : pd.DataFrame
    calibration_data : pd.DataFrame
    metadata : list
    '''
    
    calibration_data.to_csv('calibration_tmp.csv',index=False)
    experience_data.to_csv('experience_tmp.csv',index=False)
    
    meta_tmp = os.sep.join(path_to_single_csv.split(os.sep)[:-1])+os.sep+'metadata.csv'
    meta = open(meta_tmp, 'w')
    for line in metadata:
        if line.find('#') != -1 and line.find('Frame#') == -1:
            meta.write(line+"\n")
    meta.close()
    
    add_meta_to_head('calibration_tmp.csv', meta_tmp,os.sep.join(path_to_single_csv.split(os.sep)[:-1])+os.sep+'calibration_file.csv')
    add_meta_to_head('experience_tmp.csv', meta_tmp,os.sep.join(path_to_single_csv.split(os.sep)[:-1])+os.sep+'experience_file.csv')
    
    os.remove('calibration_tmp.csv')
    os.remove('experience_tmp.csv')
    
def adjust_calibration_duration(start):
    new_end = start + 30.005
    return new_end
    
def get_calibration_timestamps(path_to_json):
    '''
    Parameters
    ----------
    path_to_json : string
        Path to where the events .json file is stored.
    
    Returns
    -------
    calibration_start_timestamp : float
        Starting timestamp of calibration.
    calibration_end_timestamp : float
        Ending timestamp of calibration.
    
    '''
    with open(path_to_json) as f:    
        events=json.load(f)
    f.close()
    
    calibration_start_timestamp = [element['TimestampUnix'] for element in events if 'expression' in element['Label']][0]
    calibration_end_timestamp = [element['TimestampUnix'] for element in events if 'expression' in element['Label']][-1]
    
    return float(calibration_start_timestamp), float(calibration_end_timestamp)
    
    
def get_baseline_timestamps(path_to_json):
    '''
    Parameters
    ----------
    path_to_json : string
        Path to where the events .json file is stored.
    
    Returns
    -------
    calibration_start_timestamp : float
        Starting timestamp of calibration.
    calibration_end_timestamp : float
        Ending timestamp of calibration.
    
    '''
    with open(path_to_json) as f:    
        events=json.load(f)
    f.close()
    
    baseline_start_timestamp = [element['TimestampUnix'] for element in events if 'baselineHR' in element['Label']][0]
    baseline_end_timestamp = [element['TimestampUnix'] for element in events if 'baselineHR' in element['Label']][-1]
    
    return float(baseline_start_timestamp), float(baseline_end_timestamp)

def get_unix_from_name(name):
    filename = name.split(os.sep)[-1]
    year = int(filename.split("-")[0])
    month = int(filename.split("-")[1])
    day = int((filename.split("-")[2]).split("T")[0])
    hour = int((filename.split("-")[2]).split("T")[1])
    minute = int(filename.split("-")[3])
    second = int(filename.split("-")[4][:-4])
    
    date_time = datetime.datetime(year=year, month=month, day=day,
                                  hour=hour+1, minute=minute, second=second)
    #I am adding 1 hour to the values because I am converting from their UK time to UTC
    unix = time.mktime(date_time.timetuple())
    return unix
    
    
def get_time_column(unix, rows, frequency=1000):
    step_array = np.arange(rows/frequency, step=(1/frequency))
    res = step_array + unix
    timedf = pd.DataFrame(res)
    return timedf

def split_data(path_to_single_csv, path_to_json):
    
    calibration_start_timestamp, calibration_end_timestamp = get_calibration_timestamps(path_to_json)
    baseline_start_timestamp, baseline_end_timestamp = get_baseline_timestamps(path_to_json)
    
    print(calibration_end_timestamp, type(calibration_end_timestamp))
    
    
    _file = open(path_to_single_csv, 'r')
    data = _file.read()
    _file.close()
    
    metadata = [line for line in data.split('\n') if '#' in line]
    unix_reference_offset = [float(line.split(',')[1]) for line in metadata if line.find('#Time/Seconds.unixOffset') != -1][0]
    
    for line in metadata:
        if line.find('Frame#') == -1:
            data=data.replace("{}".format(line),'', 1)
    data = re.sub(r'\n\s*\n', '\n', data, re.MULTILINE)
    data = re.sub(r'\s*\n\s*Frame#','Frame#', data, re.MULTILINE)
    
    data = pd.read_csv(StringIO(data), skip_blank_lines=True, delimiter = ',', na_filter=False)
        
    #data['UnixTime'] = get_time_column(get_unix_from_name(path_to_single_csv), data.shape[0])    #this was used for the old bongiovi data to fix the timestamp issue
    #data['Time_tmp'] = data['UnixTime'] - 946684800
    #data['Time'] = data['UnixTime'] - unix_reference_offset
    
    data['Time_tmp'] = data['Time']
    
    experience_data = data.loc[(data['Time_tmp']) > calibration_end_timestamp]
    
    calib_duration = calibration_end_timestamp - calibration_start_timestamp
    #if not enough data in the calibration
    if (calib_duration < 30):
        calibration_end_timestamp = adjust_calibration_duration(calibration_start_timestamp)
    
    calibration_data = data.loc[(data['Time_tmp'] >= calibration_start_timestamp) & (data['Time_tmp'] <= calibration_end_timestamp)]
    baseline_data = data.loc[(data['Time_tmp'] >= baseline_start_timestamp) & (data['Time_tmp'] <= baseline_end_timestamp)]
    experience_data = baseline_data.append(experience_data)
    
    calibration_data=calibration_data.drop('Time_tmp',axis=1)
    experience_data=experience_data.drop('Time_tmp',axis=1)
    
    save_data(experience_data, calibration_data, metadata, path_to_single_csv)


if __name__ == "__main__":
    
   path_to_single_csv = r"C:\Users\filip\Documents\Praksa\EmTeQ Ifi\Bug fixing\old stuff idk\2022-04-29T14-25-41.csv"
   path_to_json = r"C:\Users\filip\Documents\Praksa\EmTeQ Ifi\Bug fixing\old stuff idk\2022-04-29T14-25-41.json"

   split_data(path_to_single_csv, path_to_json)
   
   
   