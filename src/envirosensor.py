import os
import json
import pandas as pd

# data folders
raw_folder = "../data/raw/envirosensor/"
interim_folder = "../data/interim/envirosensor/"
processed_folder = "../data/processed/envirosensor/"


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
    data_keys = ["TMP", "OPT", "BAT", "HDT", "BAR", "HDH"]

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


def log_errors(input_folder=raw_folder, output_folder=interim_folder):
    total_objects = 0
    invalid_objects = 0
    error_list = []

    # loop through all JSON files in the specified folder
    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(input_folder, filename)

            # read JSON data from file
            with open(file_path, "r") as file:
                json_data = json.load(file)

                # check each object in the JSON data
                for obj in json_data:
                    total_objects += 1

                    # check if the object matches the specified schema and desired keys
                    if not validate_json(obj):
                        invalid_objects += 1
                        error_list.append({"file_path": file_path, "error_object": obj})

    # create a DataFrame from the error list
    error_df = pd.DataFrame(error_list)

    # Save the errors to a CSV file
    os.makedirs(output_folder, exist_ok=True)
    error_df.to_csv(os.path.join(output_folder, "error_data.csv"), index=False)

    print(f"Objects with missing keys: {invalid_objects} / {total_objects}")


def clean_envirosensor_data(input_folder=raw_folder, output_folder=interim_folder):
    # initialize an empty list to store DataFrames
    dfs = []

    # specify the keys you want to include in the 'Data' section
    desired_keys = ["TMP", "OPT", "BAT", "HDT", "BAR", "HDH"]

    # loop through each file in the folder
    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(input_folder, filename)

            # read JSON data from file
            with open(file_path, "r") as file:
                data = json.load(file)

            # convert JSON data to a Pandas DataFrame
            df = pd.json_normalize(data)

            # Convert the 'timestamp' column to datetime format
            df["timestamp"] = pd.to_datetime(df["timestamp"])

            # Extract the 'Data' column as a nested dictionary
            df["Data"] = df["data"].apply(lambda x: json.loads(x).get("Data", {}))

            # Filter out unexpected key-value pairs
            df_data = pd.DataFrame(df["Data"].tolist(), columns=desired_keys)

            # Merge the filtered 'Data' into the DataFrame
            df = pd.concat(
                [
                    df.drop(["json_featuretype", "format", "data", "Data"], axis=1),
                    df_data,
                ],
                axis=1,
            )

            # Convert desired fields to numeric
            df[desired_keys] = df[desired_keys].apply(pd.to_numeric, errors="coerce")

            # Append the current DataFrame to the list
            dfs.append(df)

    # Concatenate all DataFrames in the list into a single DataFrame
    combined_df = pd.concat(dfs, ignore_index=True)

    # Save the combined DataFrame to a CSV file
    os.makedirs(output_folder, exist_ok=True)
    combined_df.to_csv(f"{output_folder}combined_data.csv", index=False)

    # count sensor readings
    print(f"Cleaned sensor readings: {len(combined_df)}")

    return combined_df
