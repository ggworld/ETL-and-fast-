# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

# importing helper
from handler import datetime_converter
import boto3
import pandas as pd
import json


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
def read_region_file():
    REGION_FILE_NAME = 'regions.txt'
    TEXT_PLITER = ','
    with open(REGION_FILE_NAME) as reg_file:
        regions = reg_file.read()
        regions=[x.rstrip() for x in regions.split(TEXT_PLITER)]
    return regions

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # get congf file
    all_regions = read_region_file()
    for curr_region in all_regions:
        # handle connectinons and ivalid regions
        try :
            ec2_client = boto3.client('ec2', region_name=curr_region)
        except :
            print('connections error error or region is not valid contining ')
            continue
        my_j = json.dumps(ec2_client.describe_instances(), default=datetime_converter)
        my_jjs = json.loads(my_j)
        # df = pd.read_json(my_jjs['Reservations'][0] )
        df = pd.DataFrame.from_dict(my_jjs['Reservations'])
        # extract instance data
        for n, loop_instances in df.iterrows():
            if n == 0:
                ins_df = pd.DataFrame.from_dict(loop_instances['Instances'])
            else:
                ins_df = ins_df.append(pd.DataFrame.from_dict(loop_instances['Instances']))
        ins_df.reset_index(inplace=True)
        ins_df.sort_values('LaunchTime').to_json('{}.json'.format(curr_region),orient='records')

