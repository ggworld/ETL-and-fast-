import luigi
import boto3
import pandas as pd
import json
import logging
import botocore.exceptions
import datetime
import shutil
from bson import json_util


# funcrion to change nested json values acording to requierment to readable date format
def traverse(obj, path=None, callback=None):
    if path is None:
        path = []

    if isinstance(obj, dict):
        value = {k: traverse(v, path + [k], callback)
                 for k, v in obj.items()}
    elif isinstance(obj, list):
        value = [traverse(elem, path + [[]], callback)
                 for elem in obj]
    elif type(obj) == datetime.datetime :
        value = obj.isoformat()
    else:
        value = obj

    if callback is None:  # if a callback is provided, call it to get the new value
        return value
    else:
        return callback(path, value)




# class GlobalParams(luigi.Config):
#     region = luigi.IntParameter(default='us-east-1')


# extract class takes data from aws
class Extract(luigi.Task):
    region=luigi.Parameter(default='us-east-1')


    def run(self):
        try:
            ec2_client = boto3.client('ec2', region_name=self.region)
        except:
            logging.error('*** Connections error error or region is not valid contining ')
            return False
        try:
            try:
                ec2_data = ec2_client.describe_instances()
            except botocore.exceptions.ClientError:
                logging.error('*** Connections error error or region is not valid contining {}'.format(mydef().region))
                return False
            #change date format to readable one - can be moved to transform in later dev
            ec2_data_t=traverse(ec2_data)
            with self.output().open('w') as f:
                json.dump(ec2_data_t, f, default=json_util.default)

        except:
            raise
            logging.error('*** Issue writing tmp file to  region={}'.format(mydef().region))

    # output name for tmp file extract region
    def output(self):
        return luigi.LocalTarget("zzxtract.json")
# tarfsform class to handle json and sorting
class Transform(luigi.Task):
    region = luigi.Parameter(default='us-east-1')
    def requires(self):
        return Extract(self.region)
    def output(self):
        return luigi.LocalTarget('zztransorm.json')
    def run(self):
        try:
            with self.input().open('r') as infile:
                istance_data = json.load(infile, object_hook=json_util.object_hook)
        except:
            logging.error('*** Error oppening extract file {}'.format(mydef().region))


        # check resources in the region
        if istance_data['Reservations']:
            df = pd.DataFrame.from_dict(istance_data['Reservations'])
            # extract instance data

            for n, loop_instances in df.iterrows():
                if n == 0:
                    ins_df = pd.DataFrame.from_dict(loop_instances['Instances'])
                else:
                    ins_df = ins_df.append(pd.DataFrame.from_dict(loop_instances['Instances']))
            ins_df.reset_index(inplace=True)
            sort_df = ins_df.sort_values('LaunchTime')
            dict_of_df = sort_df.to_dict('records')
        else:
            dict_of_df ={}
        with self.output().open('w') as f:
            json.dump(dict_of_df, f)

# Load class curently to load per region - can be changeged to load to taget database ...
class Load(luigi.Task):
    region = luigi.Parameter(default='us-east-1')
    def requires(self):
        return Transform(self.region)

    def run(self):
        with self.output().open('w') as f:
            f.write('Done region')
        shutil.copyfile("zztransorm.json", '{}.json'.format(str(self.region)))
    def output(self):
        return luigi.LocalTarget('zzLoad.json')

# cleaning temp files of all stages in order to luigi to operate
def clean_files():
    import os
    try:
        file_path = "zzxtract.json"
        os.remove(file_path)
    except:
        pass
    try:
        file_path = "zztransorm.json"
        os.remove(file_path)
    except:
        pass
    try:
        file_path = "zzLoad.json"
        os.remove(file_path)
    except:
        pass


if __name__ == '__main__':
    # looping on all needed regions
    for region in [x.strip() for x in open('regions.txt').read().split(',')]:
        #cleaning tmp files so Luigi will run the process
        try:
            clean_files()
        except:
            pass
        try:
            luigi.build([Load(region)], workers=5, local_scheduler=True)
        except:
            logging.error("{} region had problems".format(region))