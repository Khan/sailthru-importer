import argparse
import futures
import math
import os
import secrets
from sailthru import sailthru_client as sc
import shutil
import sys
import time


# The max file size (in bytes) a CSV can be for API upload to Sailthru. 
# Check Sailthru for last max size.
SAILTHRU_MAX_SIZE = 10000000

# Initialize Sailthru client
sailthru_client = sc.SailthruClient(secrets.api_key, secrets.api_secret)


def main():
    parser = argparse.ArgumentParser(
        description='Upload an input CSV file to Sailthru.')
    parser.add_argument('input_file', type=str,
                        help='the file to process and upload')
    parser.add_argument('--keep-files', dest='keep_files', 
                        action='store_const', const=True, default=False,
                        help='Keep intermediate split files after uploading')
    parser.add_argument('--split-only', dest='split_only', 
                        action='store_const', const=True, default=False,
                        help='Split into necessary chunks, but don\'t upload')
    input_filename = parser.parse_args().input_file
    keep_files = parser.parse_args().keep_files
    split_only = parser.parse_args().split_only
    
    split_and_upload_file(input_filename, keep_files, split_only)


def split_and_upload_file(filename, keep_files, split_only,
        intermediate_folder='./intermediate_files'):
    # Find the size of the input file, and use that to determine how many
    # pieces it needs to be chunked into for upload.
    file_size = os.stat(filename).st_size
    print "Input file size: %s MB" % (file_size / 1000000)
    num_pieces = int(math.ceil(float(file_size) / float(SAILTHRU_MAX_SIZE)))

    # Find the maximum number of rows per piece
    num_rows = sum(1 for line in open(filename, 'r'))
    if num_pieces == 1:
        print ("Under %sMB, can send without splitting" % 
            (SAILTHRU_MAX_SIZE / 1000000))
        max_rows = num_rows
    else:
        print ("Greater than %sMB, file requires splitting" % 
            (SAILTHRU_MAX_SIZE / 1000000))
        # Calculate rows/piece, rounded to the nearest 100
        max_rows = int(math.ceil((num_rows / num_pieces) / 100) * 100)
    
    # Split the input file
    intermediate_folder = './intermediate_files'
    split(open(filename, 'r'), num_pieces, row_limit=max_rows, 
        output_path=intermediate_folder)

    if not split_only:
        # Upload!
        files = (["%s/output_%s.csv" % (intermediate_folder, i) for 
            i in range(1, num_pieces + 1)])
        upload_multiple(files)

        if not keep_files:
            shutil.rmtree(intermediate_folder)
    else:
        # Split only
        print "Split files stored in %s" % intermediate_folder


def upload_multiple(file_list, max_workers=5):
    """Upload a bunch of CSV files to Sailthru. 

    Using Python's concurrent.futures library, asynchronously uploads
    the files to Sailthru, a few at a time.

    Arguments:
        file_list: List of filenames to be uploaded.
        max_workers: The numbers of asynchronous uploads to allow at once.

    """
    with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {executor.submit(upload, filename): filename
                             for filename in file_list}
        count = 0
        total = len(file_list)
        for future in futures.as_completed(future_to_file):
            file_name = future_to_file[future]
            if future.exception() is not None:
                print '%r generated an exception: %s' % (file_name,
                                                         future.exception())
            else:
                # Successful upload
                count += 1
                print ('%s/%s files have been successfully uploaded' % 
                    (count, total))


def upload(filepath):
    """Creates a Sailthru upload job for the given file.

    Arguments:
        filepath: String path to file to be uploaded.
            
    """
    print "Uploading %s" % filepath
    # Start the upload job
    request_data = {
            'job': 'import',
            'file': filepath,
            'list': 'Dev Test',
            'signup_dates': 1,
            'report_email': 'aatashparikh@khanacademy.org'
        }
    response = sailthru_client.api_post('job', request_data, {'file': 1})
    job_id = response.get_body().get("job_id")

    # Keeping checking status until we find out that it's done
    while True:
        time.sleep(30)
        response = sailthru_client.api_get('job', {'job_id': job_id})
        if response.get_body().get("status") == "completed":
            return


def split(filehandler, num_pieces, delimiter=',', row_limit=10000,
    output_name_template='output_%s.csv', output_path='./intermediate_files', 
    keep_headers=True):
    """Splits a CSV file into multiple pieces.

    Arguments:
        num_pieces: Number of splits that will occurs. Passed in solely
        for printing purposes.
        row_limit: The number of rows you want in each output file. 
            10,000 by default.
        output_name_template: A %s-style template for the numbered 
            output files.
        output_path: Where to stick the output files.
        keep_headers: Whether or not to print the headers 
            in each output file. 

    """
    import csv
    reader = csv.reader(filehandler, delimiter=delimiter)
    current_piece = 1
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    current_out_path = os.path.join(
         output_path,
         output_name_template % current_piece
    )
    current_out_writer = csv.writer(open(current_out_path, 'w'), 
        delimiter=delimiter)
    current_limit = row_limit
    if keep_headers:
        headers = reader.next()
        current_out_writer.writerow(headers)
    for i, row in enumerate(reader):
        if i + 1 > current_limit:
            sys.stdout.write("\rSplitting into %s chunks: %d completed" % 
                (num_pieces, current_piece))
            sys.stdout.flush()
            current_piece += 1
            current_limit = row_limit * current_piece
            current_out_path = os.path.join(
               output_path,
               output_name_template % current_piece
            )
            current_out_writer = csv.writer(open(current_out_path, 'w'), 
                delimiter=delimiter)
            if keep_headers:
                current_out_writer.writerow(headers)
        current_out_writer.writerow(row)
    print


if __name__ == "__main__":
    main()
