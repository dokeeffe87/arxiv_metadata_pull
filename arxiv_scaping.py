# import modules
from __future__ import division

import pandas as pd
import csv
import feedparser
import re
import time
import urllib
import dateutil
import xml.etree.ElementTree as ET


# There are two namespaces to parse from what is returned by the arxiv api.
name_spaces = { 'atom': 'http://www.w3.org/2005/Atom',
                'arxiv': 'http://arxiv.org/schemas/atom'}


def generate_query(start, results_per_iteration, base_url=base_url, category=None, id_list=None):
    """
    Generate a few simple queries. You can make your own if you'd like to do something more custom, this is just to get started.
    :param start: Start index for the arxiv articles.  Start at zero if you want to get from the start
    :param results_per_iteration: The number of results to return per query
    :param base_url: The base url for the arxiv query
    :param category: The article category you are interested in
    :param id_list: If youy want a list of specific articles, put their ids in as a list
    :return: The query to be used to pull data
    """
    if category:
        query = base_url + 'search_query={0}&start={1}&max_results={2}&sortBy=submittedDate&sortOrder=descending'.format(category, start, results_per_iteration)
    elif id_list:
        query = 'http://export.arxiv.org/api/query?id_list='
        for id_ in id_list:
            query += id_ + ','
        query = query[:-1]
    else:
        print('Please supply at least a category or list of arxiv ids for the search')
        query = None

    return query


def run_query(query):
    """
    This query will grab article metadata starting from the newest
    :param query: query to pass to arxiv api
    :return: A dataframe with the query results
    """

    # perform a GET request using the query
    url = urllib.urlopen(query)
    response = url.read()

    # The response is an xml string with some custom tags, so using a generic parser like feedparser won't work.
    # Instead, directly parse the xml
    root = ET.fromstring(response)

    df_list = []

    for entry in root.findall("atom:entry", name_spaces):
        # If there are no results, arxiv sometimes just a blank entry
        if entry.find("atom:id", name_spaces) is None:
            continue
        # Get the paper metadata
        # Store the data in a dict
        metadata_dict = {}

        metadata_dict['title'] = entry.find('atom:title', name_spaces).text.replace('\n', '').replace('  ', '')

        metadata_dict['published'] = dateutil.parser.parse(entry.find('atom:published', name_spaces).text)

        metadata_dict['updated'] = dateutil.parser.parse(entry.find('atom:updated', name_spaces).text)

        metadata_dict['summary'] = entry.find('atom:summary', name_spaces).text.replace('\n', ' ')

        metadata_dict['authors'] = [{'name': author.find('atom:name', name_spaces).text,
                                     'affiliations': [x.text for x in author.findall('arxiv:affiliation', name_spaces)]} for author in entry.findall('atom:author', name_spaces)]

        metadata_dict['arxiv_link_with_arxiv_version'] = entry.find("./atom:link[@type='text/html']", name_spaces).attrib['href']

        # Does this work?
        metadata_dict['pdf_link_with_arxiv_version'] = entry.find("./atom:link[@type='application/pdf']", name_spaces).attrib['href']

        metadata_dict['primary_category'] = entry.find('arxiv:primary_category', name_spaces).attrib['term']

        metadata_dict['categories'] = [x.attrib['term'] for x in entry.findall('atom:category', name_spaces)]

        # These fields are not necessarily filled out.  I want to filled it if the info exists, otherwise return none

        try:
            metadata_dict['comment'] = entry.find('arxiv:comment', name_spaces).text
        except AttributeError:
            metadata_dict['comment'] = None

        try:
            metadata_dict['doi'] = entry.find('arxiv:doi', name_spaces).text
        except AttributeError:
            metadata_dict['doi'] = None

        try:
            metadata_dict['journal_reference'] = entry.find('arxiv:journal_ref').text
        except AttributeError:
            metadata_dict['journal_reference'] = None

        metadata_dict['arxiv_id'] = metadata_dict['arxiv_link_with_arxiv_version'].split('/abs/')[-1].split('v')[0]

        metadata_dict['arxiv_version'] = metadata_dict['arxiv_link_with_arxiv_version'].split('/abs/')[-1][metadata_dict['arxiv_link_with_arxiv_version'].split('/abs/')[-1].index('v')::]

        metadata_dict['arxiv_link'] = 'http://arxiv.org/abs/' + metadata_dict['arxiv_id']

        metadata_dict['pdf_link'] = 'http://arxiv.org/pdf/' + metadata_dict['arxiv_id']

        metadata_df = pd.DataFrame.from_dict(metadata_dict, orient='index').transpose()

        df_list.append(metadata_df)

    # return pd.DataFrame.from_dict(metadata_dict, orient='index').transpose()

    df = pd.concat(df_list)

    return df


if __name__ == '__main__':
    # TODO: Add more documentation

    # Define variables for the search query
    # Base api query url
    base_url = 'http://export.arxiv.org/api/query?'
    # Search parameters

    # TODO: Turn this into a list, or a parameter
    category = 'cat:hep-th'
    start = 0
    total_results = 50000
    results_per_iteration = 100
    wait_time = 5
    # Initial file labeling variable
    starting = start
    ending = start + results_per_iteration
    id_list = ['1312.2261', '0710.5765v1']
    # TODO: Add checks to make sure the specified directory exists
    sav_dir = ''
    file_name = 'example_file_2019'
    filename = sav_dir + file_name

    # Example query. You can write your own if you'd prefer to do something more customized
    query = generate_query(start=start,
                           results_per_iteration=results_per_iteration,
                           base_url=base_url,
                           category=None,
                           id_list=id_list)

    print('Executing query: \n {0}'.format(query))

    df = run_query(query=query)

    print('Query ran and returned {0} results'.format(df.shape[0]))

    # Save file
    print('Saving file to {0}'.format(filename + '.csv.gz'))
    df.to_csv(filename, index=False, compression='gzip')
