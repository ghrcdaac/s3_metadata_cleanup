import argparse
import re
import csv

import boto3
import requests

from db import Metadata, initialize_db


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


def process_prefix(short_name, version, prefix=None):
    full_prefix = f'{short_name.strip("/")}__{version.strip("/")}/'
    if prefix:
        full_prefix = f'{prefix.strip("/")}/{full_prefix}'

    return full_prefix


def write_csv(data_dict):
    with open('output.csv', 'w+', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',')
        for elem in data_dict:
            row = []
            for k, v in elem.items():
                row.append(v)
            print(f'Row: {row}')
            csvwriter.writerow(row)


def discover_granules_s3(host: str, short_name: str, prefix: str, version: str, file_reg_ex=None, dir_reg_ex=None):
    """
    Fetch the link of the granules in the host s3 bucket.
    :param host: The bucket where the files are served.
    :param prefix: The path for the s3 granule.
    :param file_reg_ex: Regular expression used to filter files.
    :param dir_reg_ex: Regular expression used to filter directories.
    :return: links of files matching reg_ex (if reg_ex is defined).
    """
    count = 0
    s3_client = boto3.client('s3')
    full_prefix = process_prefix(short_name=short_name, version=version, prefix=prefix)
    response_iterator = get_s3_resp_iterator(host, full_prefix, s3_client)
    for page in response_iterator:
        data_source = []
        for s3_object in page.get('Contents', {}):
            count += 1
            json_file_size = 0
            print(f'object: {s3_object}')
            key = s3_object["Key"]
            base_path = re.search(r'[^/]*$', key).group()
            match_groups = re.search(r'(.*)(.cmr.(?:json|xml))', base_path)
            if match_groups:
                filename = match_groups.group(1)
                extension = match_groups.group(2)
                json_exists = False
                xml_exists = False
                if 'json' in extension:
                    json_exists = True
                    check_key = key.replace('json', 'xml')
                    json_file_size = s3_object['Size']
                    xml_exists = file_exists(s3_client, host, check_key)
                elif 'xml' in extension:
                    xml_exists = True
                    check_key = key.replace('xml', 'json')
                    json_exists = file_exists(s3_client, host, check_key)
                else:
                    print(f'{extension} extension encountered and not processed.')
                    pass

                data_source.append({
                    'base_name': filename,
                    'xml_exists': xml_exists,
                    'json_exists': json_exists,
                    'json_file_size': json_file_size
                })

        Metadata.insert_many(data_source).on_conflict_ignore().execute()
        data_source.clear()

    res_dict = create_missing_json(short_name=short_name, bucket=host, prefix=full_prefix)
    write_csv(res_dict)

    return count


def create_missing_json(short_name, bucket, prefix):
    result_list = []
    s3_client = boto3.client('s3')
    query = Metadata.select(Metadata.base_name, Metadata.json_file_size).where(Metadata.json_exists == 0)
    for metadata_obj in query:
        json_file_name = f'{metadata_obj.base_name}.cmr.json'

        # Request umm_json for granule
        url = f'https://cmr.earthdata.nasa.gov/search/granules.umm_json?ShortName={short_name}' \
              f'&GranuleUR={metadata_obj.base_name}'
        res = requests.get(url)
        res_json = res.json()

        byte_str = str(res_json).encode('utf-8')
        file_size = len(byte_str) / 1000

        # Upload to S3
        s3_client.put_object(
            Body=byte_str,
            Bucket=bucket,
            Key=f'{prefix}{json_file_name}'
        )
        print(f'Uploaded {prefix}{json_file_name}')

        # Update database after upload with file size and json
        metadata_obj.json_file_size = file_size
        metadata_obj.json_exists = True
        metadata_obj.save()

        # Generate list of created json files
        result_list.append({'filename': json_file_name, 'size': file_size})

    return result_list


def main():
    parser = argparse.ArgumentParser(description='Test cli script')
    required = parser.add_argument_group('required arguments')
    required.add_argument('--short-name', '-s', dest='short_name', required=True, help='Collection short name.')
    required.add_argument('--version', '-v', dest='version', required=True, help='Collection version.')
    required.add_argument('--prefix', '-p', dest='prefix', required=False, help='Prefix for collection location.')

    args = parser.parse_args()
    short_name = args.short_name
    version = args.version
    prefix = args.prefix

    discover_granules_s3('sharedsbx-public', short_name=short_name, version=version, prefix=prefix)


if __name__ == '__main__':
    initialize_db('/tmp/granule_metadata.db')
    main()
