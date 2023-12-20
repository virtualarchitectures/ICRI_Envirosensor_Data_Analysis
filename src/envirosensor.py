import os
import json
import pandas as pd

# data folders
raw_folder = "../data/raw/envirosensor/"
interim_folder = "../data/interim/envirosensor/"
processed_folder = "../data/processed/envirosensor/"

data_keys = ["TMP", "OPT", "BAT", "HDT", "BAR", "HDH"]


def validate_json(obj):
    # define the expected keys
    platform_keys = [
        "json_featuretype",
        "deviceType",
        "deviceId",
        "eventType",
        "format",
        "timestamp",
        "data",
    ]

    # check if all platform metadata are present in the object
    if not all(key in obj for key in platform_keys):
        return False

    # parse the device information in the nested "data" field as JSON
    try:
        device = json.loads(obj["data"])
    except json.JSONDecodeError:
        return False

    # check if all the expected data are present in the nested "Data" object
    return all(key in device.get("Data", {}) for key in data_keys)


def log_errors(data):
    list = []

    # check each object in the JSON data
    for obj in data:
        # check if the object contains required keys
        if not validate_json(obj):
            list.append(obj)

    # create a DataFrame from the error list
    df = pd.DataFrame(list)

    return df


def clean_data(data):
    # convert json data to a Pandas DataFrame
    df = pd.json_normalize(data)

    # convert the 'timestamp' column to datetime format
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # extract the 'Data' column as a nested dictionary
    df["Data"] = df["data"].apply(lambda x: json.loads(x).get("Data", {}))

    # filter out unexpected key-value pairs
    df_data = pd.DataFrame(df["Data"].tolist(), columns=data_keys)

    # merge the filtered 'Data' into the DataFrame
    df = pd.concat(
        [
            df.drop(["json_featuretype", "format", "data", "Data"], axis=1),
            df_data,
        ],
        axis=1,
    )

    # convert desired fields to numeric
    df[data_keys] = df[data_keys].apply(pd.to_numeric, errors="coerce")

    return df


def clean_envirosensor_data(input_folder=raw_folder, output_folder=interim_folder):
    # initialize an empty list to store DataFrames
    cleaned_dfs = []
    logged_dfs = []

    # loop through each file in the folder
    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(input_folder, filename)

            # read JSON data from file
            with open(file_path, "r") as file:
                data = json.load(file)

                # log errors
                logged_df = log_errors(data)

                # clean the data
                cleaned_df = clean_data(data)

            # append the current DataFrames to lists
            logged_dfs.append(logged_df)
            cleaned_dfs.append(cleaned_df)

    # concatenate DataFrames in each list
    error_df = pd.concat(logged_dfs, ignore_index=True)
    data_df = pd.concat(cleaned_dfs, ignore_index=True)

    # save DataFrames to CSV
    os.makedirs(output_folder, exist_ok=True)
    error_df.to_csv(os.path.join(output_folder, "error_data.csv"), index=False)
    data_df.to_csv(f"{output_folder}combined_data.csv", index=False)

    # display counts
    print(f"Json objects with missing keys: {len(error_df)}")
    print(f"Cleaned sensor readings: {len(data_df)}")

    # return DataFrames
    return data_df, error_df


if __name__ == "__main__":
    cleaned_data = clean_envirosensor_data(raw_folder, interim_folder)
