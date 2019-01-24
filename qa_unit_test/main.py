import boto3
import os


class Validator(object):

    def __init__(self, file, required_fields=[]):
        self.file = file
        self.required_fields = required_fields
        self.errors = []

    def validate(self):
        self.validate_header()

        for line in file:
            _, error = self.validate_row(line)
            if error:
                self.errors.append(error)

        return bool(self.errors), self.errors

    def validate_header(self, file):
        missing_fields = set(self.required_fields) - set(self.file.fieldnames)
        if missing_fields:
            self.raise_header_error(missing_fields)
        return not bool(missing_fields)

    def validate_row(self, row):
        pass


class Uploader(object):

    def __init__(self, bucket):
        self.bucket = bucket

    def upload_file_to_s3(self, file, file_name):
        s3_client = boto3.client('s3')
        s3_client.put_object(Bucket=self.bucket, Key=file_name, Body=file)


class FileManager(object):

    def __init__(self, file_path):
        self.file_path = file_path
        self.file = self._open_file()
        self.uploader = Uploader("Test Bucket")
        self.validator = Validator(self.file)

    def _open_file(self):
        try:
            self.file = open(self.file_path)
        except OSError:
            print "Please Enter a valid path"

    def remove_file(self):
        if os.path.isfile(self.file_path):
            os.remove(self.filename)

    def upload(self):
        errors = self.validator.validate()
        if errors:
            print "Please fix the following errors: {}".format(",".join(errors))
        else:
            self.uploader.upload_file_to_s3(self.file)
            self.remove_file()
