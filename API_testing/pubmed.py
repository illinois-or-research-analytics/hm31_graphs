"""
Building on top of https://github.com/titipata/pubmed_parser
"""
import json
import os
import xmltodict

def extract_nested_keys_from_dict(answer_string, current_dict):
    new_str = answer_string
    for key, value in current_dict.items():
        new_str = answer_string +  f' {key} '
        if isinstance(value, dict):
            new_str += extract_nested_keys_from_dict(new_str, value)

    return new_str





if __name__ == "__main__":
    current_dir = 'API_testing/'
    files = os.listdir(current_dir);
    print (files)

    for file in files:
        if file.split('.')[-1] != 'xml':
            continue

        with open(current_dir + file) as xml_file:
            data_dict = xmltodict.parse(xml_file.read())

        print(file)
        print(extract_nested_keys_from_dict('',data_dict))
        print()

        json_data = json.dumps(data_dict)
        with open(f'{current_dir}{file.split(".")[0]}.json', "w") as json_file:
            json_file.write(json_data)
