import os
import json
import pandas as pd

# data folders
raw_folder = "../data/raw/envirosensor/"
interim_folder = "../data/interim/envirosensor/"
processed_folder = "../data/processed/envirosensor/"

# json keys
platform_keys = [
    "json_featuretype",
    "deviceType",
    "deviceId",
    "eventType",
    "format",
    "timestamp",
    "data",
]
device_keys = [
    "DeviceID",
    "DeviceType",
    "Envirosensor",
    "Event",
    "Time",
    "Data",
]
data_keys = ["TMP", "OPT", "BAT", "HDT", "BAR", "HDH"]


def validate_data(obj):
    # check platform metadata
    if not all(key in obj for key in platform_keys):
        return "Platform metadata"

    # check device metadata in nested "data" field
    if not all(key in obj.get("data", {}) for key in device_keys):
        return "Device metadata"

    # parse the device information in the nested "data" field as JSON
    try:
        device = json.loads(obj["data"])
    except json.JSONDecodeError:
        return "Invalid JSON format"

    # check sensor readings in the nested "Data" field
    if not all(key in device.get("Data", {}) for key in data_keys):
        return "Sensor readings"

    # validation successful
    else:
        return None


def log_errors(data):
    error_list = []

    # check if the data contains required keys
    for obj in data:
        error_reason = validate_data(obj)
        if error_reason:
            obj["error"] = error_reason
            error_list.append(obj)

    # create a DataFrame from the error list
    df = pd.DataFrame(error_list)

    return df


def extract_values(json_str, keys):
    json_data = json.loads(json_str)
    return [json_data.get(key) for key in keys]


def clean_data(data):
    # convert json data to a Pandas DataFrame
    df = pd.json_normalize(data)

    # extract the device metadata
    device_df = pd.DataFrame(
        df["data"].apply(lambda x: extract_values(x, device_keys)).tolist(),
        columns=device_keys,
    )

    # extract the device's data payload
    df["Data"] = df["data"].apply(lambda x: extract_values(x, ["Data"])[0])
    payload_df = pd.DataFrame(df["Data"].tolist(), columns=data_keys)

    # merge the required data
    combined_df = pd.concat(
        [
            df[["json_featuretype", "timestamp"]],
            device_df[
                [
                    "DeviceID",
                    "DeviceType",
                    "Event",
                    "Time",
                ]
            ],
            payload_df[["TMP", "OPT", "BAT", "HDT", "BAR", "HDH"]],
        ],
        axis=1,
    )

    # convert data fields to numeric
    combined_df[data_keys] = combined_df[data_keys].apply(
        pd.to_numeric, errors="coerce"
    )

    # convert timestamps to datetime format
    combined_df["timestamp"] = pd.to_datetime(combined_df["timestamp"], errors="coerce")
    combined_df["Time"] = pd.to_datetime(combined_df["Time"], errors="coerce")

    # rename columns
    combined_df = combined_df.rename(
        columns={
            "json_featuretype": "File",
            "timestamp": "PlatformTime",
            "Time": "DeviceTime",
        }
    )

    return combined_df


def clean_envirosensor_data(input_folder=raw_folder, output_folder=interim_folder):
    # initialize an empty list to store DataFrames
    cleaned_dfs = []
    logged_dfs = []

    # loop through each file in the folder
    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(input_folder, filename)

            # read JSON data from file
            try:
                with open(file_path, "r") as file:
                    data = json.load(file)

                    # log errors and clean the data
                    logged_df = log_errors(data)
                    cleaned_df = clean_data(data)

                # append the current DataFrames to lists
                logged_dfs.append(logged_df)
                cleaned_dfs.append(cleaned_df)

            except Exception as e:
                print(f"Error processing {file_path}: {e}")

    # concatenate DataFrames in each list
    error_df = pd.concat(logged_dfs, ignore_index=True)
    data_df = pd.concat(cleaned_dfs, ignore_index=True)

    # save DataFrames to CSV
    os.makedirs(output_folder, exist_ok=True)
    error_df.to_csv(f"{output_folder}error_log.csv", index=False)
    data_df.to_csv(f"{output_folder}cleaned_data.csv", index=False)

    # display counts
    print(f"Json objects with missing keys/values: {len(error_df)}")
    print(f"Cleaned sensor readings: {len(data_df)}")

    # return data as DataFrame
    return data_df


if __name__ == "__main__":
    cleaned_data = clean_envirosensor_data(raw_folder, interim_folder)
