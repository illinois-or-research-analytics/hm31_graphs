from pyspark.sql import SparkSession
import re
import os
import json
import time

def clear_files(dir):
    files = os.listdir(dir)

    for file in files:
        if '.gz' in file:
            os.system(f'cd {data_dir} && gunzip {file}')
            os.system(f'cd {data_dir} && rm {file}')


if __name__ == "__main__":
    data_dir = '/shared/hm31/xml_data/'
    #data_dir = 'test/'


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


    strings_of_interest = ['DateCompleted', 'ISSN', 'ArticleTitle', 'ISSN', 'GrantList', 'KeywordList', 'MeshHeadingList',
                           'ChemicalList', 'PubmedArticle', 'Abstract', 'PubDate', 'PubmedArticle']

    strings_of_interest = ['DateCompleted', 'ISSN', 'GrantList', 'MeshHeadingList',
                           'ChemicalList', 'PubmedArticle', 'PubDate']
    # Process each XML file and print the results
    output_file = "output.txt"
    pattern = r'(\d{4})\.xml$'
    xml_files = os.listdir(data_dir+'/')

    result_dict = {}

    start = time.time()

    for xml_file in xml_files:


            xml_file = os.path.join(data_dir, xml_file)
            filename, substring_counts = count_substring_occurrences(xml_file, strings_of_interest)

            file_id = re.search(pattern, filename).group(1)
            result_dict[file_id] = substring_counts

    result_dict['time'] = time.time() - start
    with open('output.json', "w") as json_file:
        json.dump(result_dict, json_file)
    # Stop the SparkSession