import argparse
import concurrent.futures
import json
import re
import csv
from itertools import islice

import boto3
import requests

cmr_prefix = {
    'sbx': '.uat',
    'sit': '.uat',
    'uat': '.uat',
    'prod': ''
}


def dictionary_chunks(data, size=1000):
    it = iter(data)
    for i in range(0, len(data), size):
        yield {k: data[k] for k in islice(it, size)}


def sequence_chunks(seq, size=1000):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


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


class WrapperClass:
    def __init__(self, aws_profile, short_name, version, prefix, bucket, environment):
        boto3.setup_default_session(profile_name=aws_profile)
        self.bucket = bucket
        self.short_name = short_name
        self.version = version
        self.prefix = prefix

        self.path = f'{self.short_name}__{self.version}/'
        if self.prefix:
            self.path = f'{self.path}{self.prefix}/'

        self.environment = environment

    def write_csv(self, data_list):
        """
        Creates a csv file out of the data list
        :param data_list: list of dictionaries with the following format:
        data_list = [{'filename': json_file_name, 'size': file_size}, ...)]
        """
        with open(f'{self.short_name}__{self.version}.csv', 'a+', newline='') as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',')
            for elem in data_list:
                row = []
                for k, v in elem.items():
                    row.append(v)
                csv_writer.writerow(row)

    def discover_granule_metadata(self):
        """
        Scans the given host bucket to determine if there are any cmr.xml files.
        If there is only an xml then create a cmr.json file, upload it to the host/prefix location, and delete the
        cmr.xml. If there are both, just delete the cmr.xml.
        Creates a csv file with containing the json file names and file sizes.
        :return: links of files matching reg_ex (if reg_ex is defined).
        """
        # search_prefix = f'{prefix.rstrip("/")}/' if prefix else f'{short_name}__{version}/'
        full_prefix = f'{self.short_name}__{self.version}/'
        if self.prefix:
            full_prefix = f'{full_prefix}{self.prefix}'

        metadata_file_dict = {}
        xml_key_list = []
        print(f'Processing: {self.bucket}/{full_prefix}')
        s3_client = boto3.client('s3')
        response_iterator = get_s3_resp_iterator(self.bucket, full_prefix, s3_client)
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
                        xml_key_list.append({'Key': key})
                    else:
                        print(f'{extension} extension encountered and not processed.')
                        pass
                    update_dict(metadata_file_dict, filename, xml_exists, json_exists, json_file_size)

        return {'metadata_file_dict': metadata_file_dict, 'xml_key_list': xml_key_list}

    def json_wrapper(self, base_name_key):
        """
        Fetches the json for a metadata file and returns a dictionary containing neecessary information for further
         processing.
        {'filename': json_file_name, 'size': file_size, 'bytes': byte_str} where byte_str is the raw byte string of the
        json.
        :param base_name_key: The file name to fetch metadata from CMR
        :return: Dictionary of the following format:
        {'filename': json_file_name, 'size': file_size, 'bytes': byte_str}
        """
        json_file_name = f'{base_name_key}.cmr.json'
        # Request umm_json for granule from cmr
        url = f'https://cmr{self.environment}.earthdata.nasa.gov/search/granules.umm_json?ShortName={self.short_name}' \
              f'&GranuleUR={base_name_key}'
        res = requests.get(url)
        res_json = res.json()
        if res_json.get('hits'):
            umm_json = json.dumps(res_json.get('items')[0].get('umm'))
            byte_str = str(umm_json).encode('utf-8')
            file_size = len(byte_str) / 1000

            return {'filename': json_file_name, 'size': file_size, 'bytes': byte_str}
        else:
            print(f'CMR returned no hits for short_name: {self.short_name}, granule_ur: {base_name_key}')

    def upload_wrapper(self, file_dict):
        """
        Uploads the bytes of a json file to S3.
        :param file_dict: Dictionary of the following format:
        {'filename': json_file_name, 'size': file_size, 'bytes': byte_str}
        :return: The response from the boto3 put_object call
        """
        s3_client = boto3.client('s3')
        # Upload to S3
        res = s3_client.put_object(
            Body=file_dict.get('bytes'),
            Bucket=self.bucket,
            Key=f'{self.path}{file_dict.get("filename")}'
        )

        return res

    def create_missing_json(self, value_dict):
        """
        Retrieves the umm json from cmr, extracts the relevant metadata from the response, and uploads the contents as
        a json file to s3.
        :return: List of dictionaries with the following format:
        [{'filename': json_file_name, 'size': file_size}, ...]
        """
        result_list = []
        print(f'Creating {len(value_dict)} json files.')
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for base_name_key, value_dict in value_dict.items():
                if not value_dict.get('json_exists'):
                    futures.append(
                        executor.submit(
                            self.json_wrapper, base_name_key=base_name_key
                        )
                    )

            for future in concurrent.futures.as_completed(futures):
                result_list.append(future.result())

        return result_list

    def upload_json(self, result_list):
        """
        Wrapper for the threadpoolexecutor
        :param result_list: List with the following format:
        [{'filename': json_file_name, 'size': file_size}, ...]
        :return: None
        """
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for entry in result_list:
                futures.append(
                    executor.submit(
                        self.upload_wrapper, file_dict=entry
                    )
                )

            for future in concurrent.futures.as_completed(futures):
                future.result()

    def delete_xml_files(self, xml_list):
        """
        Deletes files from S3
        :param xml_list: List of dictionaries of [{'Key': key}, ...] format.
        :return: None
        """
        if xml_list:
            s3_client = boto3.client('s3')
            for block in sequence_chunks(xml_list):
                s3_client.delete_objects(
                    Bucket=self.bucket,
                    Delete={'Objects': block}
                )

                # Delete xml files
                for key in block:
                    print(f'Deleted: {key}')


def main():
    parser = argparse.ArgumentParser(description='Searches a provided location for cmr.xml files, creates cmr.json'
                                                 'files as needed, and deletes the cmr.xml files. Generates a csv'
                                                 'with the creates json files and file sizes.')
    required = parser.add_argument_group('required arguments')
    required.add_argument('--short-name', '-s', dest='short_name', required=True, help='Collection short name.')
    required.add_argument('--version', '-v', dest='version', required=True, help='Collection version.')
    required.add_argument('--bucket', '-b', dest='bucket', required=True, help='Bucket to check.')
    required.add_argument('--aws_profile', '-pr', dest='aws_profile', required=True, help='AWS PROFILE')
    required.add_argument('--prefix', '-p', dest='prefix', required=False, default='',
                          help='Prefix for collection location.')
    required.add_argument('--environment', '-e', dest='environment', required=False, default='prod',
                          choices={'sbx', 'sit', 'uat', 'prod'})

    args = parser.parse_args()
    short_name = args.short_name
    version = args.version
    prefix = args.prefix
    bucket = args.bucket
    aws_profile = args.aws_profile
    environment = f'{cmr_prefix.get(args.environment)}'
    wc = WrapperClass(aws_profile=aws_profile, bucket=bucket, short_name=short_name, prefix=prefix, version=version,
                      environment=environment)

    result_dict = wc.discover_granule_metadata()
    metadata_file_dict = result_dict.get('metadata_file_dict')

    # Batch here
    for block in dictionary_chunks(metadata_file_dict):
        result_list = wc.create_missing_json(value_dict=block)
        wc.upload_json(result_list)
        for entry in result_list:
            del entry['bytes']

        xml_key_list = result_dict.get('xml_key_list')
        wc.delete_xml_files(xml_key_list)
        wc.write_csv(result_list)


if __name__ == '__main__':
    main()
