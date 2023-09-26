from pyspark.sql import SparkSession
import re
import os
import json
import time
import argparse

def main(split, share):
    pass

    data_dir = '/shared/hm31/xml_data/'

    parser = argparse.ArgumentParser(description="Your script description.")

    spark = SparkSession.builder.appName("XMLSubstringCount").getOrCreate()

    # Define a function to count occurrences of substrings in a file
    def count_substring_occurrences(file_path, substrings):
        lines = spark.read.text(file_path).rdd.map(lambda x: x.value)

        # Initialize a dictionary to store counts for each substring
        substring_counts = {substring: 0 for substring in substrings}

        for substring_to_count in substrings:
            pattern = r'\b{}\b'.format(re.escape(substring_to_count))
            substring_count = lines.flatMap(lambda line: re.findall(pattern, line, re.IGNORECASE)).count()
            substring_counts[
                substring_to_count] = substring_count / 2  # Assuming you want to divide by 2 as in your example

        # Extract the filename from the file path
        filename = os.path.basename(file_path)

        # Return the filename and the substring counts
        return filename, substring_counts

    strings_of_interest = ['DateCompleted', 'ISSN', 'GrantList', 'MeshHeadingList',
                           'ChemicalList', 'PubmedArticle', 'PubDate']
    # Process each XML file and print the results
    output_file = "output.txt"
    pattern = r'(\d{4})\.xml$'
    xml_files = os.listdir(data_dir + '/').sort()

    result_dict = {}

    start = time.time()
    chunck = len(xml_files)//split

    if (chunck) * (share + 1) >= len(xml_files):
        end_index = len(xml_files)

    else:
        end_index = (share+1) * chunck


    for xml_file_index in range(chunck*share, end_index):
        xml_file = os.path.join(data_dir, xml_file)
        filename, substring_counts = count_substring_occurrences(xml_file, strings_of_interest)

        file_id = re.search(pattern, filename).group(1)
        result_dict[file_id] = substring_counts

    result_dict['time'] = time.time() - start
    with open(f'dump/output{split}{share}.json', "w") as json_file:
        json.dump(result_dict, json_file)
    # Stop the SparkSession

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Your script description.")
    parser.add_argument("split", type=int, help="Split size integer")
    parser.add_argument("share", type=int, help="Index integer")
    args = parser.parse_args()
    main(args.split, args.share)
