from pyspark.sql import SparkSession
import re
import os
import json

def clear_files(dir):
    files = os.listdir(dir)

    for file in files:
        if '.gz' in file:
            os.system(f'cd {data_dir} && gunzip {file}')
            os.system(f'cd {data_dir} && rm {file}')


if __name__ == "__main__":
    data_dir = 'xml_data/'


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


    # List of XML files to process

    # ]
    strings_of_interest = ['DateCompleted', 'ISSN', 'ArticleTitle', 'ISSN', 'GrantList', 'KeywordList', 'MeshHeadingList',
                           'ChemicalList', 'PubmedArticle', 'Abstract', 'PubDate', 'PubmedArticle']

    # Process each XML file and print the results
    output_file = "output.txt"
    pattern = r'(\d{4})\.xml$'
    xml_files = os.listdir(data_dir+'/')

    result_dict = {}

    # with open(output_file, "w") as f:
    #     for xml_file in xml_files:
    #         xml_file = os.path.join(data_dir, xml_file)
    #         filename, substring_counts = count_substring_occurrences(xml_file, strings_of_interest)
    #
    #         f.write(f"File: {re.search(pattern, filename).group(1)}\n")
    #         for substring, count in substring_counts.items():
    #             f.write(f"Occurrences of '{substring}': {count}\n")
    #         f.write("\n")
    # # Stop the SparkSession
    # spark.stop()

    for xml_file in xml_files:
            xml_file = os.path.join(data_dir, xml_file)
            filename, substring_counts = count_substring_occurrences(xml_file, strings_of_interest)

            file_id = re.search(pattern, filename).group(1)
            result_dict[file_id] = substring_counts

    with open('output.json', "w") as json_file:
        json.dump(result_dict, json_file)
    # Stop the SparkSession
