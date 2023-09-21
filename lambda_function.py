import logging
import boto3
import datetime
import json
from botocore.exceptions import ClientError

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')
s3_control = boto3.client('s3control')

# Parameter
cloudtrail = "cloudtrail"
accountID = context.invoked_function_arn.split(":")[4]

# Main
def lambda_handler(event, context):
    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s: %(asctime)s: %(message)s')
    
    
    bucket_names = s3_client.list_buckets()
    
    # List Regional Access Point - debugging ===================
    
    # access_point = client.list_access_points(AccountId=accountID)
    # print(access_point)
    #===========================================
    
    # 1. Delete Multi-Region Access Points
    delete_multiregion_access_points()
    
    
    for bucket in bucket_names['Buckets']:
        # 2 Do not delete Cloudtrail Bucket
        if cloudtrail in bucket["Name"]:
            # bucket["Name"].remove('aws-cloudtrail-logs-613589781924-fa7023cc')
            continue
        else:
            # print(bucket["Name"])
            bucket_ = s3.Bucket(bucket["Name"])
            
            try:
                # 3. Delete Bucket Policy
                delete_policy(bucket_)
                # 4. Delete all objects in S3 bucket
                s3.Bucket(bucket["Name"]).objects.all().delete()
                # 5. Permanently Delete ALL S3 Objects
                permanently_delete_object(bucket_)
                # 6. Delete Bucket
                s3_client.delete_bucket(Bucket=bucket["Name"])
            
            except Exception as e: # For Debugging easier
                print(e)
                print(bucket["Name"])
                # continue
            
    return True

# Based on AWS Function

def list_bucket_objects(bucket_name):
    # Retrieve the list of bucket objects
    
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return None
    return response['Contents']
    

def permanently_delete_object(bucket):
    """
    Permanently deletes a versioned object by deleting all of its versions.

    Usage is shown in the usage_demo_single_object function at the end of this module.

    :param bucket: The bucket that contains the object.
    :param object_key: The object to delete.
    """
    try:
        bucket.object_versions.all().delete()
        # bucket.object_versions.filter(Prefix=object_key).delete()
        # logger.info("Permanently deleted all versions of object %s.", object_key)
    except ClientError as e:
        print("delete object error:")
        logging.error(e)
        #  logger.exception("Couldn't delete all versions of %s.", object_key)
        raise
    
def delete_policy(bucket):
    """
    Delete the security policy from the bucket.
    """
    try:
        bucket.Policy().delete()
        # logger.info("Deleted policy for bucket '%s'.", self.bucket.name)
    except ClientError as e:
        # logger.exception("Couldn't delete policy for bucket '%s'.", self.bucket.name)
        logging.error(e) 
        raise


def delete_multiregion_access_points():
    try:
        access_point_multi_region = s3_control.list_multi_region_access_points(AccountId=accountID) 
        if access_point_multi_region['AccessPoints']==[]:
            print("Empty")
        else:
            for multi_region_ap in access_point_multi_region['AccessPoints']:
                detail_tuple = list(multi_region_ap.items())[0]
                detail_dict = {
                    detail_tuple[0] : detail_tuple[1]
                }
                s3_control.delete_multi_region_access_point(AccountId=accountID,Details=detail_dict)
            return "Done"
    except ClientError as e:
        logging.error(e)
        raise