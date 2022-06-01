import argparse
import json
import re
import csv

import boto3
import requests

cmr_prefix = {
    'sbx': '.sit',
    'sit': '.sit',
    'uat': '.uat',
    'prod': ''
}


def get_s3_resp_iterator(host, prefix, s3_client):
    """
    Returns an s3 paginator.
    :param host: The bucket.
    :param prefix: The path for the s3 granules.
    :param s3_client: S3 client to create paginator with.
    """
    s3_paginator = s3_client.get_paginator('list_objects')
    return s3_paginator.paginate(
        Bucket=host,
        Prefix=prefix,
        PaginationConfig={
            'PageSize': 1000
        }
    )


def file_exists(s3_client, host, check_key):
    """
    Checks if the file exists in s3
    :param s3_client: boto3 s3 client
    :param host: Bucket location
    :param check_key: File key to verify
    :return: True if the file exists else False
    """
    exist = False
    try:
        s3_client.head_object(
            Bucket=host,
            Key=check_key
        )
        exist = True
    except s3_client.exceptions.ClientError:
        pass
    return exist


def write_csv(data_list, short_name, version):
    """
    Creates a csv file out of the data list
    :param version: Collection version
    :param short_name: Collection short name
    :param data_list: list of dictionaries with the following format:
    data_list = [{'filename': json_file_name, 'size': file_size}, ...)]
    """
    with open(f'{short_name}__{version}.csv', 'w+', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',')
        for elem in data_list:
            row = []
            for k, v in elem.items():
                row.append(v)
            csvwriter.writerow(row)


def update_dict(param_dict, filename, xml_exists, json_exists, json_file_size):
    """
    Helper function to properly update the dictionary as metadata files are discovered. Will preserve xml_exists and
    json_exists if either of these values have already been set to True but will update json file size if a new value
    is passed in.
    :param param_dict: The dictionary to be updated
    :param filename: The base filename ie some.file.tar
    :param xml_exists: Does the xml file exist ie some.file.tar.cmr.xml
    :param json_exists: Does the xml file exist ie some.file.tar.cmr.json
    :param json_file_size: The size of the json file
    :return: No return needed. The dictionary passed in is modified
    """
    entry = param_dict.get(filename, None)
    if entry:
        xml_check = entry.get('xml_exists')
        json_check = entry.get('json_exists')
        file_size_check = entry.get('json_file_size')
        entry.update({
            'xml_exists': xml_check if xml_check else xml_exists,
            'json_exists': json_check if json_check else json_exists,
            'json_file_size': json_file_size if json_file_size else file_size_check
        })
    else:
        param_dict[filename] = {'xml_exists': xml_exists, 'json_exists': json_exists, 'json_file_size': json_file_size}


def discover_granule_metadata(host, short_name, prefix, version, environment):
    """
    Scans the given host bucket to determine if there are any cmr.xml files.
    If there is only an xml then create a cmr.json file, upload it to the host/prefix location, and delete the cmr.xml
    If there are both, just delete the cmr.xml.
    Creates a csv file with containing the json file names and file sizes.
    :param short_name: The short name of the collection used in constructing the full prefix
    :param version: The version of the collection used in constructing the full prefix
    :param host: The bucket where the files are served.
    :param prefix: The path for the s3 metadata file.
    :param environment: Used to specify which api endpoint should be accessed
    :return: links of files matching reg_ex (if reg_ex is defined).
    """
    s3_xml_delete_request = {'Objects': []}
    metadata_file_dict = {}
    s3_client = boto3.client('s3')
    search_prefix = f'{prefix.rstrip("/")}/' if prefix else f'{short_name}__{version}/'
    print(f'Processing: {search_prefix}')
    response_iterator = get_s3_resp_iterator(host, search_prefix, s3_client)
    for page in response_iterator:
        for s3_object in page.get('Contents', {}):
            json_file_size = 0
            key = s3_object["Key"]
            print(f'Checking: {key}')
            base_path = re.search(r'[^/]*$', key).group()
            match_groups = re.search(r'(.*)(.cmr.(?:json|xml))', base_path)
            if match_groups:
                filename = match_groups.group(1)
                extension = match_groups.group(2)
                json_exists = False
                xml_exists = False
                if 'json' in extension:
                    json_exists = True
                    json_file_size = s3_object['Size']
                elif 'xml' in extension:
                    xml_exists = True
                    s3_xml_delete_request['Objects'].append({'Key': key})
                else:
                    print(f'{extension} extension encountered and not processed.')
                    pass
                update_dict(metadata_file_dict, filename, xml_exists, json_exists, json_file_size)

    res_list = create_missing_json(short_name=short_name, bucket=host, prefix=search_prefix,
                                   value_dict=metadata_file_dict, environment=environment)

    # Delete xml files
    for x in s3_xml_delete_request['Objects']:
        print(f'Deleting: {x}')

    if s3_xml_delete_request['Objects']:
        for block in chunker(s3_xml_delete_request, 1000):
            s3_client.delete_objects(
                Bucket=host,
                Delete=block
            )

    write_csv(res_list, short_name, version)


def create_missing_json(short_name, bucket, prefix, value_dict, environment):
    """
    Retrieves the umm json from cmr, extracts the relevant metadata from the response, and uploads the contents as
    a json file to s3.
    :param short_name: Collection short name
    :param bucket: Destination bucket to store json
    :param prefix: prefix location to store json
    :param value_dict: Dictionary containing metadata file information
    :param environment: Used to specify which api endpoint should be accessed
    :return: List of dictionaries with the following format:
    list = ({'filename': json_file_name, 'size': file_size}, ...)
    """
    result_list = []
    s3_client = boto3.client('s3')
    for base_name_key, value_dict in value_dict.items():
        if not value_dict.get('json_exists'):
            json_file_name = f'{base_name_key}.cmr.json'

            # Request umm_json for granule from cmr
            url = f'https://cmr{environment}.earthdata.nasa.gov/search/granules.umm_json?ShortName={short_name}' \
                  f'&GranuleUR={base_name_key}'
            res = requests.get(url)
            res_json = res.json()
            prefix = f"{prefix.rstrip('/')}/"
            if res_json.get('hits'):
                umm_json = json.dumps(res_json.get('items')[0].get('umm'))
                byte_str = str(umm_json).encode('utf-8')
                file_size = len(byte_str) / 1000

                # Upload to S3
                s3_client.put_object(
                    Body=byte_str,
                    Bucket=bucket,
                    Key=f'{prefix}{json_file_name}'
                )
                print(f'Uploaded {prefix}{json_file_name}')

                # Generate list of created json files
                result_list.append({'filename': json_file_name, 'size': file_size})
            else:
                print(f'CMR returned no hits for short_name: {short_name}, granule_ur: {base_name_key}')

    return result_list


def main():
    parser = argparse.ArgumentParser(description='Searches a provided location for cmr.xml files, creates cmr.json'
                                                 'files as needed, and deletes the cmr.xml files. Generates a csv'
                                                 'with the creates json files and file sizes.')
    required = parser.add_argument_group('required arguments')
    required.add_argument('--short-name', '-s', dest='short_name', required=True, help='Collection short name.')
    required.add_argument('--version', '-v', dest='version', required=True, help='Collection version.')
    required.add_argument('--bucket', '-b', dest='bucket', required=True, help='Bucket to check.')
    required.add_argument('--aws_profile', '-pr', dest='aws_profile', required=True, help='AWS PROFILE')
    required.add_argument('--prefix', '-p', dest='prefix', required=False, help='Prefix for collection location.')
    required.add_argument('--environment', '-e', dest='environment', required=False, default='prod',
                          choices={'sbx', 'sit', 'uat', 'prod'})

    args = parser.parse_args()
    short_name = args.short_name
    version = args.version
    prefix = args.prefix
    bucket = args.bucket
    aws_profile = args.aws_profile
    boto3.setup_default_session(profile_name=aws_profile, region_name="us-west-2")
    environment = f'{cmr_prefix.get(args.environment)}'

    discover_granule_metadata(host=bucket, short_name=short_name, version=version, prefix=prefix, 
                              environment=environment)


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


if __name__ == '__main__':
    main()
