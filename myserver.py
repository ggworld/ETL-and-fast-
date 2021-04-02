from typing import Optional

from fastapi import FastAPI
import json
import pandas as pd

# function to read the data and handle errors
def read_region(region):
    try:
        # df = pd.read_json('{}.json'.format(region))
        with open('{}.json'.format(region)) as region_file:
            region_data = json.loads(region_file.read())
    except:
        region_data={'error':'no data on region {}'.format(region)}
        # df=pd.DataFrame({'Error':['No Data on region']})
    return region_data
app = FastAPI()


@app.get("/")
def read_root():
    return 'Wellcom to Region data Retriver to use send /region/<your region data> or use docs'


# The route to retrive the data
@app.get("/region/{region_id}")
def read_item(region_id: str, q: Optional[str] = None):
    region_data=read_region(region_id)
    return {"regiondata": region_data}