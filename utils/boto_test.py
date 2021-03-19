"""testing to see if the AWS environment is accessible locally"""

from saml_util import samlapi_formauth
import boto3

# define boto sessions
session = boto3.session.Session(profile_name='saml')
s3 = session.resource('s3')

def bucket_name_finder():
    """basic list of all S3 bickets"""
    for bucket in s3.buckets.all():
        print(bucket.name)

# try except loop

if __name__=='__main__':
    try:
        bucket_name_finder()
    except:
        print("\nAccess Error: please run saml auth again\n")
        samlapi_formauth()
        bucket_name_finder()
