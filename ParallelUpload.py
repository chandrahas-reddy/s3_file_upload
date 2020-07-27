"""
Author: Chandrahas Dodda

Readme:

ParallelUpload
This file consists of a class which uploads files from source to S3 buckets in Parallel.
"""

import boto3
import Config as config
import os
from multiprocessing import Pool

# s3 client
s3 = boto3.client("s3",
                  aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)
bucket_resource = s3


class ParallelUpload:
    def upload_to_s3(self, source, bucket_name, target):
        try:
            bucket_resource.upload_file(source, bucket_name, target)
        except:
            print("S3 file upload failed!!")

    def parallel_upload(self, source, target, bucket_name):
        print("parallel file upload...")
        status = "passed"

        # getting files from source
        source_path = os.listdir(source)
        print("all files in the source path: {}".format(source_path))
        no_of_src_files = len(source_path)
        print("Total count of files: {}".format(no_of_src_files))

        source_files = []
        target_files = []
        for f in source_path:
            local_path = source + f
            target_path = target + "{}".format(f)
            source_files.append(local_path)
            target_files.append(target_path)
        arg_dump = [(source_files[i], bucket_name, target_files[i]) for i in range(len(source_files))]

        # parallel processing
        try:
            with Pool(processes=config.agents) as pool:
                pool.starmap(self.upload_to_s3, arg_dump)
        except:
            print("parallel processing failed!")
            status = "failed"
        return status
