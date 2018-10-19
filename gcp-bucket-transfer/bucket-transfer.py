#!/usr/bin/env python

import os
import pdb
import sys
import json
import pytz
import logging
import argparse
import dictdiffer

from datetime import datetime

from google.cloud import storage
from google.cloud.storage import Blob

def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
                        "-p", 
                        "--project", 
                        dest = "gcp_project", 
                        type = str, 
                        help = "GCP project where objects are stored.",
                        required = True)
    parser.add_argument(
                        "-j", 
                        "--json-input", 
                        dest = "json_file", 
                        type = str, 
                        help = (
                                "JSON file with list of dicts containing " + 
                                "metadata of objects stored in GCS."), 
                        required = True)
    parser.add_argument(
                        "-b", 
                        "--target-bucket", 
                        dest = "target_bucket", 
                        type = str, 
                        help = "Name of bucket where objects will be moved.", 
                        required = True)
    parser.add_argument(
                        "-m", 
                        "--meta-outfile", 
                        dest = "meta_outfile", 
                        type = str, 
                        help = "File that new blob metadata will be written to.", 
                        required = True)
    parser.add_argument("-d", 
                        "--delete-source", 
                        dest = "delete_source", 
                        type = bool, 
                        help = "With flag, source blob will be deleted after transfer.", 
                        default = False)
    args = parser.parse_args(argv)
    return(args)

def get_seconds_from_epoch(datetime_obj):
    """Get datetime as total seconds from epoch.

    Provides datetime in easily sortable format
    """
    return (datetime_obj - datetime(1970, 1, 1, tzinfo=pytz.UTC)).total_seconds()

def get_blob_metadata(blob):
    blob_dict = {}

    bucket = blob.bucket.name
    path = blob.name

    dirname = os.path.dirname(path)

    # Object name includes name and extension
    basename = os.path.basename(path)
    elements = basename.split('.')
    name = elements[0]
    # In case basename contains multiple periods
    extension = '.'.join(elements[1:])

    # Time values
    time_created_epoch = get_seconds_from_epoch(blob.time_created)
    time_updated_epoch = get_seconds_from_epoch(blob.updated)

    blob_dict = {
                 'bucket': bucket,
                 'path': path,
                 'dirname': dirname,
                 'basename': basename,
                 'name': name,
                 'extension': extension, 
                 'gcs-id': blob.id,
                 'size': blob.size, 
                 'md5_hash': blob.md5_hash, 
                 'crc23c': blob.crc32c, 
                 'gcs-storage-class': blob.storage_class, 
                 'time-created-epoch': time_created_epoch, 
                 'time-created-iso': blob.time_created.isoformat(), 
                 'time-updated-epoch': time_updated_epoch, 
                 'time-updated-iso': blob.updated.isoformat(),
                 'self-link': blob.self_link, 
                 'public-url': blob.public_url, 
                 'content-type': blob.content_type
    }
    return blob_dict

def main():
    # Parse command-line arguments
    logging.basicConfig(level='DEBUG')
    logging.info("Parsing command-line arguments.")
    args = parse_args(sys.argv[1:])

    logging.info("Command-line arguments: {}.".format(args))
    project = args.gcp_project
    json_file = args.json_file
    target_bucket_name = args.target_bucket
    meta_outfile = args.meta_outfile
    delete_source = args.delete_source

    # Load data from json file
    logging.info("Loading blobs to transfer from {}.".format(json_file))
    with open(json_file, 'r') as file_handle:
        data = json.load(file_handle)
    logging.info("Found {} blobs to transfer".format(len(data)))

    logging.info("Creating storage client.")
    client = storage.Client(project=project)
    target_bucket = client.get_bucket(target_bucket_name)

    for entry in data:
        source_bucket_name = entry['node']['bucket'] 
        source_path = entry['node']['path']
        db_id = entry['id']
        logging.info("Beginning transfer of {} from {} to {}".format(
                                                            source_path, 
                                                            source_bucket_name, 
                                                            target_bucket_name))

        source_bucket = client.get_bucket(source_bucket_name)

        # Trim "data/bina-deliverables" prefix for wgs-2000 data
        # data/bina-deliverables/401593083/069303ad-4a99-4554-9da0-cfb324c9060a/Recalibration/alignments.bam
        if source_bucket.name == "gbsc-gcp-project-mvp-phase-2-data": 
            elements = source_path.split('/')
            target_path = '/'.join(elements[2:])
            logging.info("Trimming first 2 elements of directory path.")
        else:
            target_path = source_path

        # Get source blob
        logging.info("Getting soure blob from {}".format(source_path))
        source_blob = source_bucket.get_blob(source_path)
        # Get source metadata
        source_metadata = get_blob_metadata(source_blob)
        logging.info("Source metadata: {}".format(source_metadata))

        # Get target blob
        logging.info("Rewriting source blob to gs://{}/{}.".format(
                                                                target_bucket, 
                                                                target_path))
        target_blob = Blob(target_path, target_bucket)
        token = None
        while True:
            result = target_blob.rewrite(source_blob, token=token)
            token = result[0]
            if token:
                logging.info("Rewrite status: {} of {} total bytes written.".format(
                                                                    result[1], 
                                                                    result[2]))
            else:
                logging.info("Rewrite complete. {} of {} total bytes written. ".format(
                                                                    result[1], 
                                                                    result[2]))
                break
        
        # Get target blob metadata
        logging.info("Getting target metadata.")
        target_metadata = get_blob_metadata(target_blob)

        # Add metadata changes to a dictionary that mirrors input 
        output_metadata = {
                           'id': db_id, 
                           'node': target_metadata
                          }
        
        # Compare source & target metadatas 
        diffs = list(dictdiffer.diff(source_metadata, target_metadata))
        for diff in diffs:
            logging.info("Changed metadata: {}".format(diff))

        for diff in diffs:
            meta_property = diff[1] 
            # Check that size, md5, crc23c, content-type are unchanged 
            if meta_property in ['md5_hash', 'crc23c', 'content-type']:
                logging.error('Content changed: {}'.format(diff))
                sys.exit()

        # Delete source blob
        #if delete_source:
        #    source_bucket.delete_blob(source_path)

        with open(meta_outfile, 'a') as file_handle:
            file_handle.write(json.dumps(target_metadata))

if __name__ == "__main__":
    main()
