""" Extract Transform Load (ETL) module """
#import necessary libraries 
import requests #used to pull data from APIs basically for extraction data
import pandas as pd #transform data using pandas dataframes
from sqlalchemy import create_engine #load data into databases

def extract()->dict:
    """
    http://universities.hipolabs.com/search?name=India

    """
    API_URL = "http://universities.hipolabs.com/search?country=india"
    data = requests.get(API_URL).json() #to get response in json format
    return data

def transform(data:dict)->pd.DataFrame: #condition we only want universities from Delhi
    """ Transform the dataset into desired structure and filters """
    df = pd.DataFrame(data) #convert json data to pandas dataframe
    print(f"Total Number of universities from API {len(df)}")
    df = df[df["name"].str.contains("Delhi")] #filter out the universities from Delhi
    print(f"Number of Universities in Delhi: {len(df)}")
    df['domains'] = [','.join(map(str, 1)) for 1 in df['domains']] # convert list to string using comma separator
    df['web_pages'] = [','.join(map(str, 1)) for 1 in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[["domains","country","web_pages","name","alpha_two_code","state-province"]] # select only required columns

def load(df:pd.DataFrame) -> None:
    """ Load data into a database table """
    disk_engine = create_engine('sqlite:///universities.db')     # create a sqlite database
    df.to_sql('Delhi_Universities', disk_engine, if_exists='replace', index=False) #load data into database table

data = extract() #extract data from API
df = transform(data) #transform the extracted data
load(df) #load the transformed data into database


