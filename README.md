# S3 Metadata Cleanup Tool
## Purpose
Detect and replace obsolete AWS S3 `cmr.xml` (ECHO-10 XML) metadata objects with valid `cmr.json` (UMM-G JSON) metdata objects. Does not alter/write to any AWS asset (DynamoDB, RDS, OpenSearch, etc.) _other than_ modifying S3 objects.
​
## Audience
DAAC operators or developers using the [Cumulus Framework](http:/https://github.com/nasa/cumulus/ "Cumulus Framework") who want to replace obsolete AWS S3 `cmr.xml` metadata objects with valid `cmr.json` ones.  
​
In particular, this software assumes a 1:1 correspondence between Collections and S3 location (that is, the granules for collection _x_ are all in the same S3 location, and granules belonging to other collections are not present in this location), and no garauntee is made that the results will be as-expected for use cases departing from this paradigm.
​
------------
​
​
## Execution requirements
Minimum python version: 3.8.13+
The following external python3 libraries are required:
- boto3
- botocore
- requests
- urllib3
​
------------
​
### Installing required python libraries, method 1 (preferred)
The necessary libraries can be installed directly into the currently-active python3 environment by issuing the following command. For most users, this will be sufficient:
```
$ pip install -r requirements.txt
```
​
### Installing required python libraries, method 2 (alternate)
In *some* scenarios (e.g., dependency resolver incompatibilities), the above approach *may* not satisfy your needs. Although beyond the scope of this repository/guide, solutions such as [Conda](https://docs.conda.io/projects/conda/en/latest/index.html) can be used to create a brand new python3 environment with the necessary libraries. 
​
Assuming you have Conda installed, you can run the following commands to create and activate a new python3 environment called `s3_metadata` with the required libraries:
```
$ conda env create -f environment.yml 
$ conda activate s3_metadata
```
​
------------
​
Additionally, the executor must have a properly configured [AWS Profile](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html). The following characteristics of the collection whose files are being altered, and the target AWS environment, must be known:
- Collection shortname
- Collection version
- AWS S3 bucket name
- AWS Profile name
- S3 prefix, aka the internal S3 path within the bucket to the `cmr.xml` files
​
------------
​
## What it does/expected behavior
The script searches a user-specified S3 location and searches all files and builds a dictionary with the following format:
{'filename_without_json_or_xml: {'json_exists': <True/False>, 'xml_exists': <True/False>}}. The extensions are expected to be cmr.json or cmr.xml. These are always presumed to contain granule metadata.  For each such object found, a call is made to the CMR API to retrieve the equivalent UMM-G JSON granule metadata, which is streamed (written to) a new `cmr.json` object in the user-specified S3 location. Extant `cmr.xml` objects are removed from S3 only when all `cmr.json` objects have been written.
​
An error with one (or several) per-granule attempts should not cause other attempts to fail (unless the error is unrecoverable/etc). More details about errors and edge cases will be established as this tool sees real-world use.
​
###  Nominal output & results
STDOUT/STDERR will show progress/errors as usual. In addition, a `.csv` file will be created locally. This file contains a list of the created `cmr.json` object(s) with their corresponding file size(s) and can be used as a record of "what has been done".
​
`cmr.xml` objects are removed and replacement `cmr.json` objects are created in the user-specified S3 location.
​
The usual exit code is `0`, signifying no errors.
​
### Known edge cases
1. If CMR does not have UMM-G JSON metadata for the  granule corresponding to a particular `cmr.xml` object, an error like the following will be shown, and that granule's `cmr.xml` object will not be removed, nor a replacement `cmr.json` object created: 
```
CMR returned no hits for short_name: {short_name}, granule_ur: {base_name_key}'
```
​
2. If both a `cmr.xml` and `cmr.json` object with the same basename already exist in the user-specified S3 location, the `cmr.xml` object will be deleted and no new/replacement `cmr.json` object will be created.
​
------------
​
## Usage & Example
Issue the following command to view the help/usage output (as shown below):
```
$ python main.py --help
usage: main.py [-h] --short-name SHORT_NAME --version VERSION --bucket BUCKET --aws_profile AWS_PROFILE [--prefix PREFIX]
               [--environment {sbx,sit,uat,prod}]
​
Searches a provided location for cmr.xml files, creates cmr.jsonfiles as needed, and deletes the cmr.xml files. Generates a csvwith the
creates json files and file sizes.
​
optional arguments:
  -h, --help            show this help message and exit
​
required arguments:
  --short-name SHORT_NAME, -s SHORT_NAME
                        Collection short name.
  --version VERSION, -v VERSION
                        Collection version.
  --bucket BUCKET, -b BUCKET
                        Bucket to check.
  --aws_profile AWS_PROFILE, -pr AWS_PROFILE
                        AWS PROFILE
  --prefix PREFIX, -p PREFIX
                        Prefix for collection location.
  --environment {sbx,sit,uat,prod}, -e {sbx,sit,uat,prod}
```
​
### Full example: Invocation, STDOUT results
This S3 location has a mixture of `cmr.xml` and `cmr.json` objects representing granule metadata. The date ranges are non-overlapping, which is what we normally expect to see:
```
$ aws s3 ls ghrcwuat-public/rssmif17d__7/ --profile ghrcwuat | grep xml | wc -l
     275
$ aws s3 ls ghrcwuat-public/rssmif17d__7/ --profile ghrcwuat | grep json | wc -l
     117
​
$ aws s3 ls ghrcwuat-public/rssmif17d__7/ --profile ghrcwuat | grep xml | head
2020-08-31 14:31:50       2217 f17_ssmis_20200801v7.nc.cmr.xml
2020-08-31 14:32:43       2217 f17_ssmis_20200802v7.nc.cmr.xml
2020-08-31 14:32:10       2217 f17_ssmis_20200803v7.nc.cmr.xml
2020-08-31 14:32:14       2216 f17_ssmis_20200804v7.nc.cmr.xml
2020-08-31 14:32:00       2217 f17_ssmis_20200805v7.nc.cmr.xml
2020-08-31 14:32:11       2217 f17_ssmis_20200806v7.nc.cmr.xml
2020-08-31 14:32:35       2217 f17_ssmis_20200807v7.nc.cmr.xml
2020-08-31 14:31:55       2217 f17_ssmis_20200808v7.nc.cmr.xml
2020-08-31 14:32:23       2217 f17_ssmis_20200809v7.nc.cmr.xml
2020-08-31 14:31:58       2217 f17_ssmis_20200810v7.nc.cmr.xml
​
$ aws s3 ls ghrcwuat-public/rssmif17d__7/ --profile ghrcwuat | grep xml | tail
2021-05-17 15:27:42       2139 f17_ssmis_20210508v7.nc.cmr.xml
2021-05-17 15:27:58       2139 f17_ssmis_20210509v7.nc.cmr.xml
2021-05-17 15:27:47       2139 f17_ssmis_20210510v7.nc.cmr.xml
2021-05-17 15:27:42       2139 f17_ssmis_20210511v7.nc.cmr.xml
2021-05-17 15:27:51       2139 f17_ssmis_20210512v7.nc.cmr.xml
2021-05-17 15:28:03       2139 f17_ssmis_20210513v7.nc.cmr.xml
2021-05-17 19:32:41       2139 f17_ssmis_20210514v7.nc.cmr.xml
2021-05-17 19:32:43       2139 f17_ssmis_20210515v7.nc.cmr.xml
2021-05-17 19:32:46       2139 f17_ssmis_20210516v7.nc.cmr.xml
2021-05-17 19:32:46       2139 f17_ssmis_20210517v7.nc.cmr.xml
​
$ aws s3 ls ghrcwuat-public/rssmif17d__7/ --profile ghrcwuat | grep json | head
2021-07-28 15:21:06       2278 f17_ssmis_20210701v7.nc.cmr.json
2021-07-28 15:21:04       2277 f17_ssmis_20210702v7.nc.cmr.json
2021-07-28 15:20:45       2278 f17_ssmis_20210703v7.nc.cmr.json
2021-07-28 15:21:02       2278 f17_ssmis_20210704v7.nc.cmr.json
2021-07-28 15:21:01       2278 f17_ssmis_20210705v7.nc.cmr.json
2021-07-28 15:20:41       2278 f17_ssmis_20210706v7.nc.cmr.json
2021-07-28 15:20:58       2278 f17_ssmis_20210707v7.nc.cmr.json
2021-07-28 15:20:56       2278 f17_ssmis_20210708v7.nc.cmr.json
2021-07-28 15:20:37       2277 f17_ssmis_20210709v7.nc.cmr.json
2021-07-28 15:20:44       2278 f17_ssmis_20210710v7.nc.cmr.json
​
$ aws s3 ls ghrcwuat-public/rssmif17d__7/ --profile ghrcwuat | grep json | tail
2021-11-30 15:55:35       2717 f17_ssmis_20211129v7.nc.cmr.json
2021-11-30 15:55:23       2717 f17_ssmis_20211130v7.nc.cmr.json
2021-12-08 16:18:47       2716 f17_ssmis_20211201v7.nc.cmr.json
2021-12-08 16:18:33       2717 f17_ssmis_20211202v7.nc.cmr.json
2021-12-08 16:18:57       2716 f17_ssmis_20211203v7.nc.cmr.json
2021-12-08 16:18:39       2717 f17_ssmis_20211204v7.nc.cmr.json
2021-12-08 16:18:00       2716 f17_ssmis_20211205v7.nc.cmr.json
2021-12-08 16:18:43       2717 f17_ssmis_20211206v7.nc.cmr.json
2021-12-08 16:18:55       2717 f17_ssmis_20211207v7.nc.cmr.json
2021-12-08 16:18:20       2717 f17_ssmis_20211208v7.nc.cmr.json
```
​
We want to replace the 275 `cmr.xml` objects with 275 `cmr.json` objects, so in our case (the `rssmif17d` collection, version `7`, `uat` environment, `ghrcwuat-public` bucket) we run the following command:
```
$ python main.py --short-name rssmif17d --version 7 --bucket ghrcwuat-public --aws_profile ghrcwuat --prefix rssmif17d__7 --environment uat
```
​
The full invocation + results of this example are shown below:
```
$ python main.py --short-name rssmif17d --version 7 --bucket ghrcwuat-public --aws_profile ghrcwuat --prefix rssmif17d__7 --environment uat 
Processing: rssmif17d__7/
Checking: rssmif17d__7/f17_ssmis_20200801v7.nc.cmr.xml
Checking: rssmif17d__7/f17_ssmis_20200802v7.nc.cmr.xml
Checking: rssmif17d__7/f17_ssmis_20200803v7.nc.cmr.xml
​
....etc, etc, 1 "Checking" line per each cmr.xml object found....
​
Checking: rssmif17d__7/f17_ssmis_20210517v7.nc.cmr.xml
Checking: rssmif17d__7/f17_ssmis_20210701v7.nc.cmr.json
Checking: rssmif17d__7/f17_ssmis_20210702v7.nc.cmr.json
Checking: rssmif17d__7/f17_ssmis_20210703v7.nc.cmr.json
Checking: rssmif17d__7/f17_ssmis_20210704v7.nc.cmr.json
​
....etc, etc, 1 "Checking" line per each cmr.json object found....
​
Checking: rssmif17d__7/test/f17_ssmis_20210708v7.nc.cmr.json
Uploaded rssmif17d__7/f17_ssmis_20200801v7.nc.cmr.json
Uploaded rssmif17d__7/f17_ssmis_20200802v7.nc.cmr.json
Uploaded rssmif17d__7/f17_ssmis_20200803v7.nc.cmr.json
Uploaded rssmif17d__7/f17_ssmis_20200804v7.nc.cmr.json
​
....etc, etc, 1 "Uploaded blah.cmr.json" line per each new cmr.json object written....
​
Uploaded rssmif17d__7/f17_ssmis_20210517v7.nc.cmr.json
Deleting: {'Key': 'rssmif17d__7/f17_ssmis_20200801v7.nc.cmr.xml'}
Deleting: {'Key': 'rssmif17d__7/f17_ssmis_20200802v7.nc.cmr.xml'}
Deleting: {'Key': 'rssmif17d__7/f17_ssmis_20200803v7.nc.cmr.xml'}
Deleting: {'Key': 'rssmif17d__7/f17_ssmis_20200804v7.nc.cmr.xml'}
​
....etc, etc, 1 "Deleting key blah cmr.xml" line per each cmr.xml object removed....
​
$ $?
-bash: 0: command not found
```
​
### Full example: Resulting local `.csv` file
As explained above, the software created a local `csv` file when run, containing the names of each new `cmr.json` object created and its size, 1 per line.
​
**Note that we have created/written 275 `cmr.json` objects, reflected by 275 items listed in this file, and this matches the initial count (275) of `cmr.xml` objects that were present in the S3 location**`s3://ghrcwuat-public/rssmif17d__7`. This count is a useful check for determining whether the results are as-expected:
```
$ ls -lh rssmif17d__7.csv 
-rw-r--r--  1 sflynn  staff    11K May  4 12:33 rssmif17d__7.csv
​
$ wc -l rssmif17d__7.csv 
     275 rssmif17d__7.csv
​
$ head rssmif17d__7.csv 
f17_ssmis_20200801v7.nc.cmr.json,1.475
f17_ssmis_20200802v7.nc.cmr.json,1.475
f17_ssmis_20200803v7.nc.cmr.json,1.475
f17_ssmis_20200804v7.nc.cmr.json,1.474
f17_ssmis_20200805v7.nc.cmr.json,1.475
f17_ssmis_20200806v7.nc.cmr.json,1.475
f17_ssmis_20200807v7.nc.cmr.json,1.475
f17_ssmis_20200808v7.nc.cmr.json,1.475
f17_ssmis_20200809v7.nc.cmr.json,1.475
f17_ssmis_20200810v7.nc.cmr.json,1.475
​
$ tail rssmif17d__7.csv 
f17_ssmis_20210508v7.nc.cmr.json,1.504
f17_ssmis_20210509v7.nc.cmr.json,1.505
f17_ssmis_20210510v7.nc.cmr.json,1.505
f17_ssmis_20210511v7.nc.cmr.json,1.505
f17_ssmis_20210512v7.nc.cmr.json,1.504
f17_ssmis_20210513v7.nc.cmr.json,1.505
f17_ssmis_20210514v7.nc.cmr.json,1.505
f17_ssmis_20210515v7.nc.cmr.json,1.505
f17_ssmis_20210516v7.nc.cmr.json,1.505
f17_ssmis_20210517v7.nc.cmr.json,1.504
```
​
### Full example: Verifying results
Recall that we initially found 275 `cmr.xml` objects and 117 `cmr.json` objects at `s3://ghrcwuat-public/rssmif17d__7`, for a total of 392 `cmr.*` objects. 
​
We saw above how output file `rssmif17d__7.csv` contains 275 lines corresponding to 275 newly-created `cmr.json` objects.
​
Checking S3 to verify these 275 new `cmr.json` objects have been created and the 117 old `cmr.xml` objects have been removed is as easy as:
```
$ aws s3 ls ghrcwuat-public/rssmif17d__7/ --profile ghrcwuat | grep xml | wc -l
       0
$ aws s3 ls ghrcwuat-public/rssmif17d__7/ --profile ghrcwuat | grep json | wc -l
     392
```
​
You can view the last-modified dates of the new `cmr.json` files and verify that they're non-empty by listing them. Example:
```
$ aws s3 ls ghrcwuat-public/rssmif17d__7/ --profile ghrcwuat | head
2022-05-04 12:30:18       1475 f17_ssmis_20200801v7.nc.cmr.json
2022-05-04 12:30:18       1475 f17_ssmis_20200802v7.nc.cmr.json
2022-05-04 12:30:19       1475 f17_ssmis_20200803v7.nc.cmr.json
2022-05-04 12:30:19       1474 f17_ssmis_20200804v7.nc.cmr.json
2022-05-04 12:30:20       1475 f17_ssmis_20200805v7.nc.cmr.json
2022-05-04 12:30:21       1475 f17_ssmis_20200806v7.nc.cmr.json
2022-05-04 12:30:21       1475 f17_ssmis_20200807v7.nc.cmr.json
2022-05-04 12:30:22       1475 f17_ssmis_20200808v7.nc.cmr.json
2022-05-04 12:30:23       1475 f17_ssmis_20200809v7.nc.cmr.json
```
