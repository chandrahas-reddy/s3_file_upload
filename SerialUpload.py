"""
Author: Chandrahas Dodda

Readme:

SerialUpload
This file consists of a class which uploads files from source to S3 buckets sequentially.
"""

import boto3
import Config as config
import os


class SerialUpload:
    def serial_upload(self, source, target, bucket_name):
        print("Serial File Upload...")
        # s3 client
        s3 = boto3.client("s3",
                          aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)
        bucket_resource = s3

        # getting files from source
        source_path = os.listdir(source)
        print("all files in the source path: {}".format(source_path))
        print("Total count of files: {}".format(len(source_path)))

        # uploading to s3
        status = "passed"
        try:
            for file in source_path:
                local_path = source+file
                target_path = target + "{}".format(file)
                bucket_resource.upload_file(local_path, bucket_name, target_path)
        except:
            print("S3 file upload failed!!")
            status = "failed"

        return status
