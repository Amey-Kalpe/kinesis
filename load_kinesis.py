import boto3
import pandas as pd
import datetime as dt
import time


# function to create a client with specific service and region
def create_client(service, region):
    return boto3.client(service, region=region)


# function to load data from csv
def load_data(filename):
    df = pd.read_csv(filename)
    return df


# functions to correctly display numbers in two value format (i.e 06 instead of 6)
def lenghthen(value):
    if len(value) == 1:
        value = "0" + value
    return value


# function to be used for generating new runtime to be used for ES
def get_date():
    today = dt.datetime.now()

    year = today.year
    month = today.month
    day = today.day

    hour = today.hour
    minute = today.minute
    second = today.second

    return "%d/%d/%d %d:%d:%d" % (year, month, day, hour, minute, second)


# function to modify the date and time to be correctly formatted
def modify_date(data):
    new_dates = [date.split("+")[0] for date in data["Occurence_Date"]]
    data["Occurence_Date"] = new_dates

    return data


def send_kinesis(client, stream_name, shard_count, data):
    pass
