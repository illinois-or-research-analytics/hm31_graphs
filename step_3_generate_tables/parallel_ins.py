import os
import psycopg2
from psycopg2 import sql
import multiprocessing
import re
os.environ["PGDATABASE"] = "ernieplus"


def insert_values_into_table(values_list):
    try:
        conn = psycopg2.connect("")
        cur = conn.cursor()

        args = ','.join(cur.mogrify("(%s,%s,%s, %s, %s, %s, %s)", i).decode('utf-8')
                        for i in values_list)

        cur.execute("INSERT INTO hm31.pubmed_all_xmls_new VALUES " + (args))

        conn.commit()
        print("Inserted values successfully")
        return "insertion success"

    except Exception as e:
        print("Error:", e)
        return "insertion error"

    finally:
        cur.close()
        conn.close()



import xmltodict

def parse(file_path):
    xml_dict = ""
    try:
        # Open and read the XML file
        with open(file_path, 'r', encoding='utf-8') as file:
            xml_contents = file.read()

        print("red")
        # Parse the XML content using xmltodict
        xml_dict = xmltodict.parse(xml_contents)
        print("parsed")
        # Print the parsed XML as a Python dictionary
    except Exception as e:
        print("Error:", e)

    return xml_dict


from tqdm import tqdm
def parse_single_record(xml_dict):
    mesh_headings = []
    grants = []
    year = ""
    journal_ISSN = ""
    chemical_list = []
    meta_data = {}
    pub_year = ""
    PMID = ""

    try:
        xml_dict = dict(xml_dict)

        #         print(xml_dict)

        try:
            if 'PMID' in xml_dict:
                PMID = xml_dict['PMID']['#text']
        except:
            pass

        try:
            if 'DateCompleted' in xml_dict:
                new_dic = dict(xml_dict['DateCompleted'])
                if 'Year' in new_dic:
                    year = new_dic['Year']

            else:
                pass

        except:
            pass

        try:
            if 'Article' in xml_dict:
                new_dic = dict(xml_dict['Article'])
                journal_ISSN = new_dic['Journal']['ISSN']['#text']

        except:
            pass

        try:
            if 'Article' in xml_dict:
                new_dic = dict(xml_dict['Article'])
                pub_year = new_dic['Journal']['JournalIssue']['PubDate']['Year']

        except:
            pass

        try:
            if 'Article' in xml_dict:
                new_dic = dict(xml_dict['Article'])

            if 'Title' in new_dic['Journal'].keys():
                journal_title = new_dic['Journal']['Title']

        except:
            pass

        try:
            if 'Article' in xml_dict:
                new_dic = dict(xml_dict['Article'])

            if 'GrantList' in new_dic:
                if type(new_dic['GrantList']['Grant']) == list:
                    for grant in new_dic['GrantList']['Grant']:
                        if 'GrantID' in grant:
                            grants.append((grant['GrantID']))

                else:
                    grants.append(new_dic['GrantList']['Grant']['GrantID'])
                    pass
        except:
            pass

        try:
            if 'MeshHeadingList' in xml_dict:
                if type(xml_dict['MeshHeadingList']['MeshHeading']) == list:
                    for mesh in xml_dict['MeshHeadingList']['MeshHeading']:
                        if '@Type' in mesh['DescriptorName'].keys() and mesh['DescriptorName']['@Type'] == 'Geographic':
                            continue

                        mesh_headings.append((mesh['DescriptorName']['@UI']))

                else:

                    mesh = xml_dict['MeshHeadingList']['MeshHeading']['DescriptorName']
                    if (not '@Type' in mesh.keys() or mesh['@Type'] != 'Geographic'):
                        mesh_headings.append(xml_dict['MeshHeadingList']['MeshHeading']['DescriptorName']['@UI'])

        except:
            pass

        try:

            if 'ChemicalList' in xml_dict:
                if type(xml_dict['ChemicalList']['Chemical']) == list:
                    for substance in xml_dict['ChemicalList']['Chemical']:
                        if substance['NameOfSubstance']['@UI'][0].lower() == 'c':
                            chemical_list.append(substance['NameOfSubstance']['@UI'])

                else:

                    if xml_dict['ChemicalList']['Chemical']['NameOfSubstance']['@UI'][0].lower() == 'c':
                        chemical_list.append(xml_dict['ChemicalList']['Chemical']['NameOfSubstance']['@UI'])


        except Exception as e:
            print(e)
            print("ERR")
            pass

    except:
        pass

    if len(year) == 0:
        year = pub_year

    meta_data = {'PMID': PMID, 'mesh': mesh_headings, 'grants': grants, 'year': year,
                 'journal_ISSN': journal_ISSN, 'journal_title': journal_title,
                 'chemical': chemical_list, 'pub_year': pub_year}

    return meta_data

def convert_dict_to_query(dic):
    if dic['year'] == '':
        year = 0

    else:
        year = dic['year']

    if dic['pub_year'] == '':
        pub_year = 0

    else:
        pub_year = dic['pub_year']

    query = (int(dic['PMID']), dic['journal_ISSN'], ','.join(dic['grants']), ','.join(dic['chemical']),
             pub_year, year, ','.join(dic['mesh']))

    return query


def parallelize(file_path):
    start = time.time()
    stats = ""
    xml_dict = parse(file_path)

    if len(xml_dict) == 0:
        stats = "Failed parse or empty xml_dict"

    else:
        meta_data_array = []

        for i in range(len(xml_dict['PubmedArticleSet']['PubmedArticle'])):
            rec = xml_dict['PubmedArticleSet']['PubmedArticle'][i]['MedlineCitation']
            x = parse_single_record(rec)
            meta_data_array.append(x)

        query_array = []

        for i in meta_data_array:
            query_array.append(convert_dict_to_query(i))

        insert_stats = insert_values_into_table(query_array)

        if "error" in insert_stats:
            stats = "failed in insertion"

        else:
            stats = "success"

    finish = time.time()
    pattern = r'(\d{4})\.xml$'

    file_id = re.search(pattern, file_path).group(1)

    with open(f'stats/{file_id}.txt', 'w') as file:
        file.write(f"{stats}\n")
        file.write(f"{str(finish-start)}\n")



if __name__ == "__main__":
    import multiprocessing
    import time

    start = time.time()

    nest_dir = '/shared/hm31/xml_data/'
    files = os.listdir(nest_dir)

    lst = []

    for file in files:
        lst.append((nest_dir + file,))

    with multiprocessing.Pool(processes=60) as pool:
        results = pool.starmap(parallelize, lst)

    finish = time.time()


    with open('elapsed.txt', 'w') as file:
        # Write a number to the file (e.g., 42)
        number = finish - start
        file.write(str(number))
