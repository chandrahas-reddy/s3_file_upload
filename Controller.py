"""
Author: Chandrahas Dodda

Readme:

This is the main controller file which will accept input file path, S3 bucket path and option to choose sequential or
parallel upload of files to S3.
"""

import argparse
from SerialUpload import SerialUpload
from ParallelUpload import ParallelUpload
import Config as config

parser = argparse.ArgumentParser(description='Passing mandatory arguments')
parser.add_argument("--source_path", action="store", dest="source", help="Your source directory path")
parser.add_argument("--s3_bucket_name", action="store", dest="s3_bucket", help="Your S3 bucket name")
parser.add_argument("--target_path", action="store", dest="target", help="Your S3 target folder path")
parser.add_argument("--process", action="store", dest="process", help="Input s for sequential and p for parallel "
                                                                      "upload of files to s3 bucket")


class Controller:
    def get_arguments(self, options):
        if options.source is None:
            raise RuntimeError("--source_path, Please specify the source files folder path.")

        if options.s3_bucket is None:
            raise RuntimeError("--s3_bucket_name, Please provide with S3 bucket name.")

        if options.target is None:
            raise RuntimeError("--target_path, Please provide with S3 target path.")

        if options.process is None:
            raise RuntimeError("--process, Please input either s or p for sequential or parallel processing")
        else:
            if options.process.lower() == 's':
                execute = "seq"
            elif options.process.lower() == 'p':
                execute = "parallel"
            else:
                raise RuntimeError("Not a valid process input. Please choose s or p")
        return execute

    def welcome(self, source, target, process, email):
        print("********************")
        print("File upload to S3")
        print("********************")
        print("Source path: {}".format(source))
        print("S3 bucket name: {}".format(target))
        print("Execution Style: {}".format(process))
        print("Email on config: {}".format(email))
        print("********************")
        print("********************")

    def main_control(self, options):
        # parsing input arguments
        execute = self.get_arguments(options)
        source = options.source
        bucket_name = options.s3_bucket
        target = options.target

        # welcome message
        self.welcome(source, target, execute, config.email_sender)

        # execution - file upload to s3
        if execute == "seq":
            status = SerialUpload().serial_upload(source, target, bucket_name)
        else:
            status = ParallelUpload().parallel_upload(source, target, bucket_name)


        # end note
        print("*********************")
        if status == 'passed':
            print("Files uploaded successfully")
        else:
            print("Files upload failed")
        print("*********************")


if __name__ == '__main__':
    options = parser.parse_args()
    Controller().main_control(options)
