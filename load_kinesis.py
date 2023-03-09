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
    return df.iloc[0:100]


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


def send_kinesis(client, kinesis_stream_name, kinesis_shard_count, data: pd.DataFrame):
    kinesis_records = []

    rows, _ = data.shape

    current_bytes = 0

    row_count = 0

    total_row_count = rows

    send_kinesis = False

    shard_count = 1

    for _, row in data.iterrows():
        values = "|".join([str(value) for value in row])

        encoded_values = bytes(values, "utf-8")

        kinesis_record = {"Data": encoded_values, "PartitionKey": str(shard_count)}

        kinesis_records.append(kinesis_record)

        row_bytes = len(values.encode("utf-8"))
        current_bytes += row_bytes

        if (
            len(kinesis_records) == 500
            or current_bytes > 50000
            or row_count == total_row_count - 1
        ):
            send_kinesis = True

        if send_kinesis:
            client.put_records(Records=kinesis_records, StreamName=kinesis_stream_name)

            kinesis_records = []
            current_bytes = 0
            send_kinesis = False

            shard_count += 1

            if shard_count > kinesis_shard_count:
                shard_count = 1

        row_count += 1

    # log out how many records were pushed
    print("Total Records sent to Kinesis: {0}".format(total_row_count))


# main function
def main():
    # start timer
    start = time.time()

    # create a client with kinesis
    kinesis = create_client("kinesis", "us-east-2")

    # load in data from the csv
    data = load_data("Sacramento_Crime_Data_From_Two_Years_Ago.csv")

    # modify the date and add loadtime field
    data = modify_date(data)

    # send it to kinesis data stream
    stream_name = "kinesis-test"
    stream_shard_count = 1

    send_kinesis(kinesis, stream_name, stream_shard_count, data)  # send it!

    # end timer
    end = time.time()

    # log time
    print("Runtime: " + str(end - start))


if __name__ == "__main__":
    # run main
    main()
