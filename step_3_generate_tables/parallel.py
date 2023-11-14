import xmltodict
import pandas as pd


def parse(file_path):
    try:
        # Open and read the XML file
        with open(file_path, 'r', encoding='utf-8') as file:
            xml_contents = file.read()
        print("read")
        # Parse the XML content using xmltodict
        xml_dict = xmltodict.parse(xml_contents)
        print("parsed")
        # Print the parsed XML as a Python dictionary
    except Exception as e:
        print("Error:", e)

    return xml_dict


def clean_abstract(abstract):
    overhauled = ''

    if type(abstract) != str:
        for a in abstract:
            if type(a) != str:
                if a != None and '#text' in a:
                    overhauled += a['#text']

            else:
                overhauled += a

        return overhauled

    return abstract


def parse_single_record(xml_dict, originial_dict, i=0):
    mesh_headings = []
    grants = []
    year = ""
    journal_ISSN = ""
    chemical_list = []
    meta_data = {}
    pub_year = ""
    PMID = ""
    doi = ""
    title = ""
    abstract = ""

    year_revised = ""
    month_revised = ""
    day_revised = ""
    date_revised = ""

    try:
        xml_dict = dict(xml_dict)

        #         print(xml_dict)

        try:
            if 'PMID' in xml_dict:
                PMID = xml_dict['PMID']['#text']
        except:
            pass

        try:
            if 'DateRevised' in xml_dict:
                new_dic = dict(xml_dict['DateCompleted'])

                if 'Year' in new_dic:
                    year_revised = new_dic['Year']

                if 'Month' in new_dic:
                    month_revised = new_dic['Month']

                if 'Day' in new_dic:
                    day_revised = new_dic['Day']

        except:
            pass

        #         try:
        #             if 'DateCompleted' in xml_dict:
        #                 new_dic = dict(xml_dict['DateCompleted'])
        #                 if 'Year' in new_dic:
        #                     year = new_dic['Year']

        #                 if 'Month' in new_dic:
        #                     month = new_dic['Month']

        #                 if 'Day' in new_dic:
        #                     day = new_dic['Day']

        #             else:
        #                   pass

        #         except:
        #             pass

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

            article_id_list = originial_dict['PubmedData']['ArticleIdList']['ArticleId']

            for element in article_id_list:
                if 'doi' in str(element).lower():
                    doi = element['#text']
                    break
        #                 print('doi' in str(article_id_list[1]))
        #                 # print(article_id_list[1])
        #                 print(article_id_list[1].keys())
        #                 print(article_id_list[1]['#text'])
        #                 #print('doi' in str(article_id_list[1]))

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

                title = new_dic['ArticleTitle']

                #                 if i == 10844:
                #                     print(title,'AAA \n')

                if '#text' in title:
                    title = title['#text']

                #                     if i == 10844:
                #                         print(title,'BBB \n')

                elif 'b' in title:
                    title = title['b']
                    if '#text' in title:
                        title = title['#text']


                elif 'sup' in title:
                    title = title['sup']

                if 'i' in title:
                    title = title['i']

                if type(title) == list:
                    title = ' '.join(title)




        except:
            pass

        try:

            if 'Article' in xml_dict:
                new_dic = dict(xml_dict['Article'])
                abstract = new_dic['Abstract']['AbstractText']

                while '#text' in abstract:
                    abstract = abstract['#text']


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

    if title == None:
        title = ''

    if abstract == None:
        abstract = ''

    abstract = clean_abstract(abstract)

    if year_revised == '':
        date_revised = '0000-00-00'

    else:
        date_revised = f'{year_revised}-{month_revised}-{day_revised}'
        # print(date_revised)

    meta_data = {'PMID': int(PMID), 'mesh': str(mesh_headings), 'grants': str(grants), 'year': str(year),
                 'journal_ISSN': str(journal_ISSN), 'journal_title': str(journal_title),
                 'chemical': str(chemical_list), 'pub_year': str(pub_year), 'doi': doi.lower(),
                 'title': str(title), 'abstract': str(abstract),
                 'date_revised': date_revised}

    return meta_data


def parallelize(file_path, mode='parquet', dump_address='/shared/hossein_hm31/pubmed_parquet/'):
    xml_dict = parse(file_path)

    meta_data_array = []

    for i in range(len(xml_dict['PubmedArticleSet']['PubmedArticle'])):
        rec = xml_dict['PubmedArticleSet']['PubmedArticle'][i]['MedlineCitation']
        total = xml_dict['PubmedArticleSet']['PubmedArticle'][i]
        x = parse_single_record(rec, total)
        meta_data_array.append(x)

    if mode == 'postgres':
        query_array = []

        for i in meta_data_array:
            query_array.append(convert_dict_to_query(i))

        insert_values_into_table(query_array)

    elif mode == 'parquet':
        xml_name = file_path.split('/')[-1].split('.')[0]  # Example '/shared/hossein_hm31/xml_data/pubmed23n0933.xml' -> pubmed23n0933.xml
        df = pd.DataFrame(meta_data_array)
        df.set_index(['PMID', 'doi'], inplace=True)
        df.to_parquet(f'{dump_address + xml_name}.parquet')
        print('saved')


def insert_values_into_table(values_list):
    import psycopg2
    from psycopg2 import sql
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect("")
        cur = conn.cursor()

        args = ','.join(cur.mogrify("(%s,%s,%s, %s, %s, %s, %s, %s, %s, %s)", i).decode('utf-8')
                        for i in values_list)

        cur.execute("INSERT INTO hm31.pubmed_all_xmls_abstracts VALUES " + (args))

        conn.commit()
        print("Inserted values successfully")

    except Exception as e:
        print("Error:", e)

    finally:
        cur.close()
        conn.close()


def convert_dict_to_query(dic):
    if dic['year'] == '':
        year = 0

    else:
        year = dic['year']

    if dic['pub_year'] == '':
        pub_year = 0

    else:
        pub_year = dic['pub_year']

    query = (int(dic['PMID']), dic['journal_ISSN'], ' '.join(dic['grants']), ' '.join(dic['chemical']),
             pub_year, year, ' '.join(dic['mesh']), dic['doi'], dic['abstract'], dic['title'])

    return query


def add_remaining():
    import multiprocessing
    import time
    import os

    obtained_files = os.listdir('/shared/hossein_hm31/pubmed_parquet/')
    all_files = os.listdir('/shared/hossein_hm31/xml_data/')

    all_xml_files = [file.split('.')[0] for file in all_files if '.xml' in file]
    all_parquet_files = [file.split('.')[0] for file in obtained_files]

    files_left = [file for file in all_xml_files if not file in all_parquet_files]

    lst = []
    start = time.time()

    for file in files_left:
        lst.append((f'/shared/hossein_hm31/xml_data/{file}.xml',))

    with multiprocessing.Pool(processes=60) as pool:
        results = pool.starmap(parallelize, lst)

    end = time.time()
    print(f'elapsed {end - start}')

def main():
    import multiprocessing
    import time
    import os

    start = time.time()

    nest_dir = '/shared/hossein_hm31/xml_data/'
    files = os.listdir(nest_dir)

    lst = []

    for file in files:
        lst.append((nest_dir + file,))

    with multiprocessing.Pool(processes=60) as pool:
        results = pool.starmap(parallelize, lst)

    end = time.time()
    print(f'elapsed {end - start}')
    file_name = "elapsed_time.txt"

    # Open the file for writing
    with open(file_name, "w") as file:
        # Write the elapsed time to the file
        file.write(f'elapsed {end - start}')


if __name__ == '__main__':
    main()
    add_remaining()
    add_remaining()
    add_remaining()