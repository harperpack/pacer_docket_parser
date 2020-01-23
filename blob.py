#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 17 07:49:20 2020

@author: harper
"""

from bs4 import BeautifulSoup
import os
import time
from datetime import date
from pathlib import Path
from multiprocessing import Pool
import pandas as pd

class CorpusParser:
    
    def __init__(self, docket_dir, o_format="text", o_dir=None, threads=5):
        self.docket_directory = docket_dir
        self.output_format = o_format
        self.output_directory = o_dir if o_dir else "/parsed_dockets"
        self.process_count = threads
        self.docket_queue = []
        self.parsed_dockets = {}
        self.missed_parses = {}
        self.docket_count = 0
        self.parse_count = 0
        self.main()
    
    def build_docket_queue(self):
        if os.path.isfile(self.docket_directory):
            self.docket_queue.append(self.docket_directory)
            self.docket_count += 1
        elif os.path.isdir(self.docket_directory):
            for (dirpath, dirnames, filenames) in os.walk(self.docket_directory):
                for filename in filenames:
                    if filename.endswith('.html'):
                        self.docket_queue.append(os.path.join(dirpath, filename))
                        self.docket_count += 1
    
    def parse_docket(self, docket):
        new_parse = DocketParser(docket)
        self.parsed_documents[new_parse.case_id] = new_parse.parsed_docket
        if new_parse.miss_list:
            self.miss_parses[new_parse.case_id] = new_parse.miss_list
        self.parse_count += 1
    
    def output_to_csv(self):
        Path(self.output_directory).mkdir(parents=True, exist_ok=True)
        self.filename = self.output_directory + str(self.parse_count) + \
                        "_dockets_" + str(date.today().strftime("%Y%m%d"))
        docket_df = pd.DataFrame.from_dict(self.parsed_dockets, orient='index')
        with open(self.filename, "w") as f:
            docket_df.to_csv(f)
    
    def main(self):
        start_queue = time.time()
        self.build_docket_queue()
        if not self.docket_queue:
            print("Could not find .html files to parse. :(")
        else:
            queue_time = time.time() - start_queue
            print("Successfully queued {c} dockets in {t} seconds.".format(c=self.docket_count,t=queue_time))
            p = Pool(processes=self.process_count)
            start_parse = time.time()
            p.map(self.parse_docket, self.docket_queue)
            parse_time = time.time() - start_parse
            print("Successfully parsed {c} dockets in {t} seconds.".format(c=self.parse_count,t=parse_time))
            self.output_to_csv()
            print("Saved to {p}.".format(p=self.filename))
            print("\nAnd when the parser saw the breadth of its outputs, it wept for there were no more files to parse.")
                
    
class DocketParser:
    
    def __init__(self, docket):
        self.docket = docket
        self.parsed_docket = {"docket_flags":'',
                              "docket_text":'',
                              "docket_title":'',
                              "docket_header":'',
                              "pacer_receipt":'',
                              }
        self.case_id = os.path.splittext(os.path.basename(docket))[0]
        self.miss_list = []
        self.roles = ['Defendant','Plaintiff','Petitioner','Respondent','Appellee','Appellant','Trustee','Creditor']
        self.main()
    
    def open_docket(self):
        with open(self.docket, "r") as f:
            docket_html = f.read()
        self.soup = BeautifulSoup(docket_html, 'html.parser')
    
    def log_miss(self, unrecognized_html):
        self.miss_detail.append(unrecognized_html)
    
    def parse_docket_header(self):
        for header in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            docket_title = [header_element.get_text() for header_element in self.soup.find_all(header)]
            if docket_title:
                break
        self.parsed_docket["docket_title"] = docket_title
    
    def parse_docket_footers(self, table):
        table_text = table.get_text()
        if table_text.isupper() or ' ' not in table_text:
            self.parsed_docket["docket_flags"] = table_text
        elif "Docket Text" in table_text:
            self.parsed_docket["docket_text"] = table_text
        elif "PACER" in table_text:
            self.parsed_docket["pacer_receipt"] = table_text
        elif "Date Filed" in table_text:
            self.parsed_docket["docket_header"] = table_text
        else:
            self.log_miss(table)
    
    def parse_roles(self, table, split, roles):
        if len(roles) == 1:
            table_as_str = str(table)
        else:
            pass
    
    def parse_docket(self):
        for table in self.soup.find_all('table'):
            table_topic = table.find('u')
            if not table_topic:
                self.parse_docket_footers(table)
            else:
                table_text = table.get_text()
                roles = [role for role in self.roles if role in table_text]
                if not roles:
                    self.log_miss(table)
                else:
                    self.parse_roles(table, table_topic, roles)
                    
                    
    
    

#path = '/Users/harper/Documents/nu_work/noacri/data/1-06-cr-00887.html'
path = '/Users/harper/Documents/petitioner.html'
with open(path, "r") as f:
    docket_html = f.read()
soup = BeautifulSoup(docket_html, 'html.parser')
print(str(soup))


def parse_case_overview(docket):
    pass

def parse_dockets(location):
    parsed_dockets = {}
    dockets_to_be_parsed = build_docket_queue(location)
    if not dockets_to_be_parsed:
        print("Could not find .html files to parse. :(")
    else:
        for docket in dockets_to_be_parsed:
            parsed_docket = {}
            with open(docket, "r") as f:
                docket_html = f.read()
            soup = BeautifulSoup(docket_html, 'html.parser')
            parsed_docket["docket_header"] = [h3.get_text() for h3 in soup.find_all('h3')]
            for table in soup.find_all("table"):
                table_text = table.get_text()
                table_topic = table.find('u')
                if not table_topic:
                    if table_text.isupper():
                        parsed_docket["docket_flags"] = table_text
                        continue
                    elif "Docket Text" in table_text:
                        parsed_docket["docket_text"] = table_text
                    elif "PACER" in table_text:
                        pass
                else:
                    topic_text = table_topic.get_text()
                    if "Defendant" in topic_text:
                        #parse_role(table, "Defendant")
                        partitions = table.get_text().partition(table_topic)
                        
                        
                        
                    elif "Plaintiff" in topic_text:
                        pass
                        #parse_role(table, "Plaintiff")
                    elif "Petitioner" in topic_text:
                        pass
                        #parse_role(table, "Petitioner")
                    elif "Respondent" in topic_text:
                        pass
                        #parse_role(table, "Respondent")
                    else:
                        print("===== UNRECOGNIZED COMPONENT =====")
                        print(table_text)
                        print("----------------------------------")
                        #self.log_miss(table)
            
            
#        
#    case = {}
#    soup = BeautifulSoup(docket_html, 'html.parser')
#    tables = soup.find_all("table")
#    count = 0
#    for table in tables:
#        table_text = table.get_text()
#        table_label = table.find('b')
#        if table_label:
#            for element in table_label.descendants:
#                print(element)

#parse_dockets(path)

#if __name__ == "__main__":
#    parse_dockets(sys.argv[1])