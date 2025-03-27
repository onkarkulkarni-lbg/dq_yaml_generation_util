import os
import pandas as pd
import yaml
from collections import OrderedDict
from google.cloud import storage


def validate_directory(bucket_path):
    client = storage.Client()
    bucket_name, prefix = bucket_path.replace("gs://", "").split("/", 1)
    bucket = client.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))

    if not blobs:
        return False, "Invalid path/location, please check"

    if not any(blob.name.endswith('/') for blob in blobs):
        return False, "Passed path/location is not a directory/folder"

    xlsx_files = [blob.name for blob in blobs if blob.name.endswith('.xlsx')]
    if not xlsx_files:
        return False, "Passed directory/folder does not contain any excel sheet"

    return True, ""


def validate_excel(bucket_path, file_name):
    validation_message = ""
    excel_file = ""

    bucket_name, prefix = bucket_path.replace("gs://", "").split("/", 1)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_bytes()

    # Load the Excel file
    try:
        excel_file = pd.ExcelFile(content)
    except Exception as e:
        validation_message = f"Error loading Excel file: {e}"
        return False, validation_message

    # Check if the required sheets are present
    required_sheets = ['ColumnLevel', 'TableLevel']
    if not all(sheet in excel_file.sheet_names for sheet in required_sheets):
        validation_message = "Required sheets are missing."
        return False, validation_message

    # Read the sheets as strings
    column_level_df = pd.read_excel(content, sheet_name='ColumnLevel', dtype=str)
    table_level_df = pd.read_excel(content, sheet_name='TableLevel', dtype=str)

    # Validate ColumnLevel sheet
    column_level_columns = [
        'GCP Project ID', 'Bigquery Dataset ID', 'Bigquery Table Name', 'Column Name', 'Rule Name',
        'Rule Description', 'DQ Rule', 'Ignore Null Values', 'Threshhold',
        'Range Min Value', 'Strict Range Min Value', 'Range Max Value', 'Strict Range Max Value',
        'Set Values', 'Regular Expression', 'SQL Expression'
    ]
    if not all(col in column_level_df.columns for col in column_level_columns):
        validation_message = "ColumnLevel sheet is missing required columns."
        return False, validation_message

    # Check if there is at least one record in ColumnLevel sheet
    if column_level_df.empty:
        validation_message = "ColumnLevel sheet does not contain any records."
        return False, validation_message

    # Validate TableLevel sheet
    table_level_columns = [
        'GCP Project ID', 'Bigquery Dataset ID', 'Bigquery Table Name',
        'Partition Filter Condition', 'Data Sampling %', 'schedule_interval',
        'DQ Export GCP Project ID', 'DQ Export Bigquery Dataset ID', 'DQ Export Bigquery Table Name'
    ]
    if not all(col in table_level_df.columns for col in table_level_columns):
        validation_message = "TableLevel sheet is missing required columns."
        return False, validation_message

    # Record level validations
    required_columns = ['GCP Project ID', 'Bigquery Dataset ID', 'Bigquery Table Name']
    for df, sheet_name in zip([column_level_df, table_level_df], ['ColumnLevel', 'TableLevel']):
        for index, row in df.iterrows():
            if not all(pd.notna(row[col]) for col in required_columns):
                validation_message = f"Missing required values in {sheet_name} sheet at row {index + 1}."
                return False, validation_message

    # Cross-sheet validation
    for index, row in table_level_df.iterrows():
        if not any(
                (column_level_df['GCP Project ID'] == row['GCP Project ID']) &
                (column_level_df['Bigquery Dataset ID'] == row['Bigquery Dataset ID']) &
                (column_level_df['Bigquery Table Name'] == row['Bigquery Table Name'])
        ):
            validation_message = f"No corresponding record in ColumnLevel sheet for TableLevel row {index + 1}."
            return False, validation_message

    # Additional validation for DQ Rule and Ignore Null Values
    valid_dq_rules = ['NOT_NULL', 'RANGE', 'UNIQUE', 'SQL_ROW', 'REGEX', 'SET', 'SQL_TABLE', 'SQL_ASSERT']
    for index, row in column_level_df.iterrows():
        dq_rule = str(row['DQ Rule']).strip().upper()
        if dq_rule not in valid_dq_rules:
            validation_message = f"'DQ Rule' should be one of {valid_dq_rules} in ColumnLevel sheet at row {index + 1}."
            return False, validation_message

        if dq_rule == 'NOT_NULL' and str(row['Ignore Null Values']).strip().upper() == 'TRUE':
            validation_message = f"'Ignore Null Values' should not be TRUE when 'DQ Rule' is NOT_NULL in ColumnLevel sheet at row {index + 1}."
            return False, validation_message

        # Validate Ignore Null Values column
        ignore_null_values = str(row['Ignore Null Values']).strip().upper()
        if ignore_null_values not in ['', 'NAN', 'TRUE', 'FALSE']:
            validation_message = f"'Ignore Null Values' should be empty, TRUE, or FALSE in ColumnLevel sheet at row {index + 1}."
            return False, validation_message

        # Validate Threshhold column
        threshhold = row['Threshhold']
        if pd.notna(threshhold):
            try:
                threshhold_value = float(threshhold)
                if not (0 <= threshhold_value <= 1):
                    validation_message = f"'Threshhold' should be within the range of 0-1 in ColumnLevel sheet at row {index + 1}."
                    return False, validation_message
            except ValueError:
                validation_message = f"'Threshhold' should be a number in ColumnLevel sheet at row {index + 1}."
                return False, validation_message

        # Validate Strict Range Min Value column
        strict_range_min_value = str(row['Strict Range Min Value']).strip().upper()
        if strict_range_min_value not in ['', 'NAN', 'TRUE', 'FALSE']:
            validation_message = f"'Strict Range Min Value' should be empty, TRUE, or FALSE in ColumnLevel sheet at row {index + 1}."
            return False, validation_message

        # Validate Strict Range Max Value column
        strict_range_max_value = str(row['Strict Range Max Value']).strip().upper()
        if strict_range_max_value not in ['', 'NAN', 'TRUE', 'FALSE']:
            validation_message = f"'Strict Range Max Value' should be empty, TRUE, or FALSE in ColumnLevel sheet at row {index + 1}."
            return False, validation_message

        # Additional validation for Range Min Value and Range Max Value
        if strict_range_min_value == 'TRUE' and pd.isna(row['Range Min Value']):
            validation_message = f"'Range Min Value' should not be empty when 'Strict Range Min Value' is TRUE in ColumnLevel sheet at row {index + 1}."
            return False, validation_message

        if strict_range_max_value == 'TRUE' and pd.isna(row['Range Max Value']):
            validation_message = f"'Range Max Value' should not be empty when 'Strict Range Max Value' is TRUE in ColumnLevel sheet at row {index + 1}."
            return False, validation_message

        # Additional validation for DQ Rule 'RANGE'
        if dq_rule == 'RANGE' and (pd.isna(row['Range Min Value']) or pd.isna(row['Range Max Value'])):
            validation_message = f"'Range Min Value' and 'Range Max Value' should not be empty when 'DQ Rule' is RANGE in ColumnLevel sheet at row {index + 1}."
            return False, validation_message

        # Additional validation for DQ Rule 'SET'
        if dq_rule == 'SET' and pd.isna(row['Set Values']):
            validation_message = f"'Set Values' should not be empty when 'DQ Rule' is SET in ColumnLevel sheet at row {index + 1}."
            return False, validation_message

        # Additional validation for DQ Rule 'REGEX'
        if dq_rule == 'REGEX' and pd.isna(row['Regular Expression']):
            validation_message = f"'Regular Expression' should not be empty when 'DQ Rule' is REGEX in ColumnLevel sheet at row {index + 1}."
            return False, validation_message

        # Additional validation for DQ Rule 'SQL_ROW', 'SQL_TABLE', 'SQL_ASSERT'
        if dq_rule in ['SQL_ROW', 'SQL_TABLE', 'SQL_ASSERT'] and pd.isna(row['SQL Expression']):
            validation_message = f"'SQL Expression' should not be empty when 'DQ Rule' is {dq_rule} in ColumnLevel sheet at row {index + 1}."
            return False, validation_message

    # Validate Data Sampling % in TableLevel sheet
    for index, row in table_level_df.iterrows():
        data_sampling = row['Data Sampling %']
        if pd.notna(data_sampling):
            try:
                data_sampling_value = float(data_sampling)
                if not (0 <= data_sampling_value <= 100):
                    validation_message = f"'Data Sampling %' should be within the range of 0-100 in TableLevel sheet at row {index + 1}."
                    return False, validation_message
            except ValueError:
                validation_message = f"'Data Sampling %' should be a number in TableLevel sheet at row {index + 1}."
                return False, validation_message

    validation_message = "Excel sheet is valid."
    return True, validation_message


def list_all_valid_excels(bucket_path):
    """
    This function will return list of excel sheets matching with expected DQ format
    """
    valid_excel_sheet = False
    validation_message = ""
    valid_excel_list = list()

    client = storage.Client()
    bucket_name, prefix = bucket_path.replace("gs://", "").split("/", 1)
    bucket = client.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))

    file_list = [blob.name for blob in blobs]

    for file in file_list:
        if file.endswith('.xlsx'):
            print(f"Reading {bucket_path}/{file}")
            valid_excel_sheet, validation_message = validate_excel(bucket_path, file)
            if not valid_excel_sheet:
                print(validation_message)
            else:
                valid_excel_list.append(file)
    return valid_excel_list


def process_excel_file(bucket_path, excel_file_name):
    # Declare variables
    yaml_content_dict = {}
    dq_rule_expectation_dict = {
        'NOT_NULL': "nonNullExpectation",
        'RANGE': "rangeExpectation",
        'UNIQUE': "uniquenessExpectation",
        'SQL_ROW': "rowConditionExpectation",
        'REGEX': "regexExpectation",
        'SET': "setExpectation",
        'SQL_TABLE': "tableConditionExpectation",
        'SQL_ASSERT': "sqlAssertion"
    }
    dq_rule_dimession_dict = {
        'NOT_NULL': "COMPLETENESS",
        'RANGE': "VALIDITY",
        'UNIQUE': "UNIQUENESS",
        'SQL_ROW': "VALIDITY",
        'REGEX': "VALIDITY",
        'SET': "VALIDITY",
        'SQL_TABLE': "VALIDITY",
        'SQL_ASSERT': "VALIDITY"
    }

    # Read the Excel file
    file_path = f"{bucket_path}/{excel_file_name}"

    bucket_name = bucket_path.replace("gs://", "").split("/")[0]
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(excel_file_name)
    content = blob.download_as_bytes()

    column_level_df = pd.read_excel(content, sheet_name='ColumnLevel', dtype=str).fillna('')
    table_level_df = pd.read_excel(content, sheet_name='TableLevel', dtype=str).fillna('')

    # Group the ColumnLevel dataframe
    grouped = column_level_df.groupby(['GCP Project ID', 'Bigquery Dataset ID', 'Bigquery Table Name'])

    # Process each group
    for group_key, group_df in grouped:
        group_key_str = f"{group_key[0]}~{group_key[1]}~{group_key[2]}"

        # Query TableLevel dataframe for partition filter condition and data sampling percentage
        table_level_row = table_level_df[
            (table_level_df['GCP Project ID'] == group_key[0]) &
            (table_level_df['Bigquery Dataset ID'] == group_key[1]) &
            (table_level_df['Bigquery Table Name'] == group_key[2])
            ]
        partition_filter_condition = table_level_row['Partition Filter Condition'].values[
            0] if not table_level_row.empty else ''
        data_sampling_percentage = table_level_row['Data Sampling %'].values[0] if not table_level_row.empty else ''
        schedule_interval = table_level_row['schedule_interval'].values[0] if not table_level_row.empty else ''

        dq_export_gcp_project_id = table_level_row['DQ Export GCP Project ID'].values[
            0] if not table_level_row.empty else ''
        dq_export_bigquery_dataset_id = table_level_row['DQ Export Bigquery Dataset ID'].values[
            0] if not table_level_row.empty else ''
        dq_export_bigquery_table_name = table_level_row['DQ Export Bigquery Table Name'].values[
            0] if not table_level_row.empty else ''

        for _, row in group_df.iterrows():
            column_expectation = dq_rule_expectation_dict.get(row['DQ Rule'], '')
            column_dimession = dq_rule_dimession_dict.get(row['DQ Rule'], '')

            if group_key_str not in yaml_content_dict:
                yaml_content_dict[group_key_str] = {
                    "expectation": [],
                    "dimension": [],
                    "name": [],
                    "description": [],
                    "threshold": [],
                    "ignoreNull": [],
                    "column": [],
                    "minValue": [],
                    "maxValue": [],
                    "strictMinEnabled": [],
                    "strictMaxEnabled": [],
                    "sqlExpression": [],
                    "regex": [],
                    "setValues": [],
                    "partition_filter_condition": partition_filter_condition,
                    "data_sampling_percentage": data_sampling_percentage,
                    "schedule_interval": schedule_interval,
                    "dq_export_gcp_project_id": dq_export_gcp_project_id,
                    "dq_export_bigquery_dataset_id": dq_export_bigquery_dataset_id,
                    "dq_export_bigquery_table_name": dq_export_bigquery_table_name
                }

            yaml_content_dict[group_key_str]["expectation"].append(column_expectation)
            yaml_content_dict[group_key_str]["dimension"].append(column_dimession)
            yaml_content_dict[group_key_str]["name"].append(row['Rule Name'])
            yaml_content_dict[group_key_str]["description"].append(row['Rule Description'])
            yaml_content_dict[group_key_str]["threshold"].append(float(row['Threshhold']) if row['Threshhold'] else '')
            yaml_content_dict[group_key_str]["ignoreNull"].append(row['Ignore Null Values'])
            yaml_content_dict[group_key_str]["column"].append(row['Column Name'])
            yaml_content_dict[group_key_str]["minValue"].append(row['Range Min Value'])
            yaml_content_dict[group_key_str]["maxValue"].append(row['Range Max Value'])
            yaml_content_dict[group_key_str]["strictMinEnabled"].append(row['Strict Range Min Value'])
            yaml_content_dict[group_key_str]["strictMaxEnabled"].append(row['Strict Range Max Value'])
            yaml_content_dict[group_key_str]["sqlExpression"].append(row['SQL Expression'])
            yaml_content_dict[group_key_str]["regex"].append(row['Regular Expression'])
            yaml_content_dict[group_key_str]["setValues"].append(
                row['Set Values'].split(',') if row['Set Values'] else '')

    # writting data to yaml file now
    write_yaml_files(yaml_content_dict, bucket_path)


# onkar start
def ordered_dict_representer(dumper, data):
    """Represent OrderedDict as a regular dict in YAML."""
    items = []
    for key, value in data.items():
        # print(f"key={key}, value={value}")
        # Check if the value is a non-empty string enclosed with single quotes
        if isinstance(value, str) and value.startswith("'") and value.endswith("'") and len(value) > 2:
            # Preserve the enclosing single quotes
            items.append((key, value))
        else:
            items.append((key, value))
    return dumper.represent_dict(data.items())


yaml.add_representer(OrderedDict, ordered_dict_representer, Dumper=yaml.SafeDumper)


def write_yaml_files(yaml_content_dict, bucket_path):
    """
    Write YAML files based on the provided dictionary content.

    Args:
        yaml_content_dict (dict): Dictionary containing YAML content.
        bucket_name (str): Directory to save the YAML files.
    """
    for key, value in yaml_content_dict.items():
        # Derive YAML file name
        if value.get('schedule_interval') and isinstance(value['schedule_interval'], str) and value[
            'schedule_interval'].strip():
            modified_schedule = value['schedule_interval'].replace('*', 'star').replace(' ', '_')

            yaml_file_name = key.replace('~', '__') + '__' + modified_schedule + '.yaml'
        else:
            yaml_file_name = key.replace('~', '__') + '.yaml'

        # yaml_file_path = os.path.join(output_directory, yaml_file_name)

        # Initialize YAML content with OrderedDict
        yaml_content = OrderedDict()

        # Add rowFilter if partition_filter_condition is not empty
        if value.get('partition_filter_condition'):
            yaml_content['rowFilter'] = value['partition_filter_condition']

        # Add samplingPercent if data_sampling_percentage is not empty
        if value.get('data_sampling_percentage'):
            yaml_content['samplingPercent'] = value['data_sampling_percentage']

        # Add rules
        rules = []
        for i in range(len(value["expectation"])):
            rule = OrderedDict({
                value["expectation"][i]: {}
            })

            # Add non-null values to the rule
            if value["dimension"][i]:
                rule["dimension"] = value["dimension"][i]
            if value["column"][i]:
                rule["column"] = value["column"][i]
            if value["threshold"][i]:
                rule["threshold"] = value["threshold"][i]
            if value["ignoreNull"][i]:
                rule["ignoreNull"] = value["ignoreNull"][i].lower() == 'true'

            if value["minValue"][i]:
                # rule["minValue"] = value["minValue"][i]
                rule[value["expectation"][i]]["minValue"] = value["minValue"][i]
            if value["maxValue"][i]:
                # rule["maxValue"] = value["maxValue"][i]
                rule[value["expectation"][i]]["maxValue"] = value["maxValue"][i]
            if value["strictMinEnabled"][i]:
                # rule["strictMinEnabled"] = value["strictMinEnabled"][i].lower() == 'true'
                rule[value["expectation"][i]]["strictMinEnabled"] = value["strictMinEnabled"][i].lower() == 'true'
            if value["strictMaxEnabled"][i]:
                # rule["strictMaxEnabled"] = value["strictMaxEnabled"][i].lower() == 'true'
                rule[value["expectation"][i]]["strictMaxEnabled"] = value["strictMaxEnabled"][i].lower() == 'true'

            if value["sqlExpression"][i]:
                # rule["sqlExpression"] = value["sqlExpression"][i]
                if value["expectation"][i] == "sqlAssertion":
                    rule[value["expectation"][i]]["sqlStatement"] = value["sqlExpression"][i]
                else:
                    rule[value["expectation"][i]]["sqlExpression"] = value["sqlExpression"][i]

            if value["regex"][i]:
                # rule[value["expectation"][i]]["regex"] = str(value["regex"][i])
                # temp_var = value['regex'][i]
                rule[value["expectation"][i]]["regex"] = value['regex'][i]

            if value["setValues"][i]:
                # rule["values"] = value["setValues"][i]
                rule[value["expectation"][i]]["values"] = value["setValues"][i]

            rules.append(rule)

        yaml_content['rules'] = rules

        # writting export result to BQ table if mentioned in the file or with default one
        if value.get('dq_export_gcp_project_id') and isinstance(value['dq_export_gcp_project_id'], str) and value[
            'dq_export_gcp_project_id'].strip() and value.get('dq_export_bigquery_dataset_id') and isinstance(
            value['dq_export_bigquery_dataset_id'], str) and value[
            'dq_export_bigquery_dataset_id'].strip() and value.get('dq_export_bigquery_table_name') and isinstance(
            value['dq_export_bigquery_table_name'], str) and value['dq_export_bigquery_table_name'].strip():

            if 'postScanActions' not in yaml_content:
                yaml_content['postScanActions'] = {}

            if 'bigqueryExport' not in yaml_content['postScanActions']:
                yaml_content['postScanActions']['bigqueryExport'] = {}

            yaml_content['postScanActions']['bigqueryExport']['resultsTable'] = (
                f"//bigquery.googleapis.com/projects/{value.get('dq_export_gcp_project_id')}/datasets/{value.get('dq_export_bigquery_dataset_id')}/tables/{value.get('dq_export_bigquery_table_name')}"
            )

        else:
            pass

        # Write YAML file
        try:
            # Convert YAML content to string
            yaml_str = yaml.dump(yaml_content, default_flow_style=False, sort_keys=False, Dumper=yaml.SafeDumper)

            # Initialize GCS client
            bucket_name, prefix = bucket_path.replace("gs://", "").split("/", 1)
            client = storage.Client()
            bucket = client.bucket(bucket_name)
            if prefix == "":
                blob = bucket.blob(prefix + "/" + yaml_file_name)
            else:
                blob = bucket.blob(prefix +  yaml_file_name)

            # Write YAML string to the blob
            blob.upload_from_string(yaml_str)
            print(f"YAML content written to gs://{bucket_name}/{prefix}/{yaml_file_name}")
        except Exception as e:
            print(f"Error writing file gs://{bucket_name}/{file_name}: {e}")


# onkar end

def main():
    """
    This is the main function/entry point
    """
    location = "gs://composer_bucket/dq_excel/"
    valid_directory = False
    validation_message = ""
    valid_excel_list = list()

    # validating given gcs bucket location
    valid_directory, validation_message = validate_directory(location)

    if not valid_directory:
        print(validation_message)
    else:
        # passed input pth is directory and contains at least one excel sheet
        # Collecting valid excel sheets now
        valid_excel_list = list_all_valid_excels(location)
        if len(valid_excel_list) == 0:
            print(f"No valid excel sheets found in {location}")
        else:
            print(f"Processing following valid excel files -{str(valid_excel_list)}")
            for elements in valid_excel_list:
                process_excel_file(location, elements)


main()
