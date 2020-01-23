#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 17 07:49:20 2020

@author: harper
"""

from bs4 import BeautifulSoup
import os.path
import time
from datetime import date
from pathlib import Path
#from multiprocessing import Pool
import pandas as pd
import json

class CorpusParser:
    
    def __init__(self, docket_dir, o_format="text", o_dir=None, threads=5):
        self.docket_directory = docket_dir
        self.output_format = o_format
        self.output_directory = o_dir if o_dir else "./parsed_dockets/"
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
        self.parsed_dockets[new_parse.case_id] = new_parse.parsed_docket
        if new_parse.miss_list:
            self.missed_parses[new_parse.case_id] = new_parse.miss_list
        self.parse_count += 1
    
    def output_to_csv(self):
        Path(self.output_directory).mkdir(parents=True, exist_ok=True)
#        if not os.path.isdir(self.output_directory):
#            os.mkdir(self.output_directory)
        self.filename = self.output_directory + str(self.parse_count) + \
                        "_dockets_" + str(date.today().strftime("%Y%m%d")) + ".json"
        with open(self.filename, 'w', encoding='utf-8') as f:
            json.dump(self.parsed_dockets, f, ensure_ascii=False, indent=4)
#        docket_df = pd.DataFrame.from_dict(self.parsed_dockets, orient='index')
#        with open(self.filename, "w") as f:
#            docket_df.to_csv(f)
    
    def main(self):
        start_queue = time.time()
        self.build_docket_queue()
        if not self.docket_queue:
            print("Could not find .html files to parse. :(")
        else:
            queue_time = time.time() - start_queue
            print("Successfully queued {c} dockets in {t} seconds.".format(c=self.docket_count,t=queue_time))
#            p = Pool(processes=self.process_count)
            start_parse = time.time()
            for docket in self.docket_queue:
                self.parse_docket(docket)
#            p.map(self.parse_docket, self.docket_queue)
            parse_time = time.time() - start_parse
            print("Successfully parsed {c} dockets in {t} seconds.".format(c=self.parse_count,t=parse_time))
            self.output_to_csv()
            print("Saved to {p}".format(p=os.getcwd()+self.filename))
            print("\nAnd when the parser saw the breadth of its outputs, it wept for there were no more files to parse.")
                
    
class DocketParser:
    
    def __init__(self, docket):
        self.docket = docket
        self.parsed_docket = {"docket_flags":'',
                              "docket_text":'',
                              "docket_title":'',
                              "docket_header":'',
                              "pacer_receipt":'',
                              "defendant":{},
                              "plaintiff":{},
                              "petitioner":{},
                              "respondent":{},
                              "appellee":{},
                              "appellant":{},
                              "trustee":{},
                              "creditor":{},
                              }
        self.case_id = str(os.path).split(os.path.basename(docket))[0]
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
        self.parsed_docket["docket_title"] = {"blob":docket_title}
    
    def parse_header(self, text, pertains_to_entire_docket=False, key=None):
        header_dict = {"blob":text}
        ## DO MORE?
        if not pertains_to_entire_docket:
            self.parsed_docket["docket_header"] = header_dict
        else:
#            key = len(self.parsed_docket[pertains_to_entire_docket].keys())
            self.parsed_docket[pertains_to_entire_docket][key]["role_header"] = header_dict
            
    def parse_docket_footers(self, table):
        table_text = table.get_text()
        if table_text.isupper() or ' ' not in table_text:
            self.parsed_docket["docket_flags"] = {"blob":table_text}
        elif "Docket Text" in table_text:
            self.parsed_docket["docket_text"] = {"blob":table_text}
        elif "PACER" in table_text:
            self.parsed_docket["pacer_receipt"] = {"blob":table_text}
        elif "Date Filed" in table_text:
            self.parse_header(table_text)
        else:
            self.log_miss(table)
    
    def parse_reps(self, role, key, rep_blob, rep_key=0):
        estimated_reps = rep_blob.find_all('b')
        if estimated_reps:
            name_attempt = ' '.join(estimated_reps[0].get_text().split())
            self.parsed_docket[role][key]["reps"][rep_key] = {"name_attempt":name_attempt}
            if len(estimated_reps) == 1:
                self.parsed_docket[role][key]["reps"][rep_key]["blob"] = rep_blob.get_text()
            else:
                current_blob, next_rep, remaining_blob = str(rep_blob).partition(str(estimated_reps[1]))
                current_rep_blob = BeautifulSoup(current_blob, 'html.parser')
                self.parsed_docket[role][key]["reps"][rep_key]["blob"] = current_rep_blob.get_text()
                leftover_rep_blob = BeautifulSoup(next_rep + remaining_blob, 'html.parser')
                self.parse_reps(role, key, leftover_rep_blob, rep_key+1)
    
    def parse_role(self, table, split, role):
        table_as_str = str(table)
        role_dict = {"blob":'',
                     "reps":{"total_blob":''},
                     "charges_blob":''}
        pre_role, throwaway, post_role = table_as_str.partition(str(split))
        soup = BeautifulSoup(post_role, 'html.parser')
        if "represented" in post_role:
            role_blob = BeautifulSoup(post_role.partition("represented")[0], 'html.parser')
            role_dict["blob"] = role_blob.get_text()
            end_reps = soup.find('u')
            if end_reps:
                rep_section, next_underline, charges_section = post_role.partition("represented")[2].partition(end_reps.get_text())
                charges_blob = BeautifulSoup(next_underline + charges_section, 'html.parser')
                role_dict["charges_blob"] = charges_blob.get_text()
            else:
                rep_section = post_role.partition("represented")[2]
            rep_blob = BeautifulSoup(rep_section, 'html.parser')
            role_dict["reps"]["total_blob"] = rep_blob.get_text()
        else:
            role_dict["blob"] = soup.get_text()
        key = len(self.parsed_docket[role].keys())
        self.parsed_docket[role][key] = role_dict
        if self.parsed_docket[role][key]["reps"]["total_blob"]:
            self.parse_reps(role, key, rep_blob)
        if "Assigned to" in pre_role or "Date Filed" in pre_role:
            soup = BeautifulSoup(pre_role, 'html.parser')
            self.parse_header(soup.get_text(),role,key)
        
    def parse_roles(self, table, split, roles):
        if len(roles) == 1:
            self.parse_role(table, split, roles[0].lower())
        else:
            for role in roles:
                underlines = table.find_all('u')
                if len(underlines) == 1:
                    self.parse_role(table, underlines[0], role.lower())
                    break
                current_role_portion, next_role, next_role_portion = str(table).partition(str(underlines[1]))
                role_blob = BeautifulSoup(current_role_portion, 'html.parser')
                self.parse_role(role_blob, underlines[0], role.lower())
                table = BeautifulSoup(next_role + next_role_portion, 'html.parser')
            
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
                    
    def main(self):
        self.open_docket()
        self.parse_docket_header()
        self.parse_docket()
    
    
#path = '/Users/harper/Documents/nu_work/noacri/data/1-06-cr-00887.html'
path = '/Users/harper/Documents/petitioner.html'

CorpusParser(path)

#with open(path, "r") as f:
#    docket_html = f.read()
#soup = BeautifulSoup(docket_html, 'html.parser')
#print(str(soup))

#
#def parse_case_overview(docket):
#    pass
#
#def parse_dockets(location):
#    parsed_dockets = {}
#    dockets_to_be_parsed = build_docket_queue(location)
#    if not dockets_to_be_parsed:
#        print("Could not find .html files to parse. :(")
#    else:
#        for docket in dockets_to_be_parsed:
#            parsed_docket = {}
#            with open(docket, "r") as f:
#                docket_html = f.read()
#            soup = BeautifulSoup(docket_html, 'html.parser')
#            parsed_docket["docket_header"] = [h3.get_text() for h3 in soup.find_all('h3')]
#            for table in soup.find_all("table"):
#                table_text = table.get_text()
#                table_topic = table.find('u')
#                if not table_topic:
#                    if table_text.isupper():
#                        parsed_docket["docket_flags"] = table_text
#                        continue
#                    elif "Docket Text" in table_text:
#                        parsed_docket["docket_text"] = table_text
#                    elif "PACER" in table_text:
#                        pass
#                else:
#                    topic_text = table_topic.get_text()
#                    if "Defendant" in topic_text:
#                        #parse_role(table, "Defendant")
#                        partitions = table.get_text().partition(table_topic)
#                        
#                        
#                        
#                    elif "Plaintiff" in topic_text:
#                        pass
#                        #parse_role(table, "Plaintiff")
#                    elif "Petitioner" in topic_text:
#                        pass
#                        #parse_role(table, "Petitioner")
#                    elif "Respondent" in topic_text:
#                        pass
#                        #parse_role(table, "Respondent")
#                    else:
#                        print("===== UNRECOGNIZED COMPONENT =====")
#                        print(table_text)
#                        print("----------------------------------")
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