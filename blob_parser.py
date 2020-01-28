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
import json
import sys

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
        self.duplicates = 0
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
        case_id = new_parse.case_id + "_0"
        while case_id in self.parsed_dockets:
            self.duplicates += 1
            case_id += str(self.duplicates)
        self.parsed_dockets[case_id] = new_parse.parsed_docket
        if new_parse.miss_list:
            self.missed_parses[case_id] = new_parse.miss_list
        self.parse_count += 1
    
    def output_to_local_dir(self):
        Path(self.output_directory).mkdir(parents=True, exist_ok=True)
        self.filename = self.output_directory + str(self.parse_count) + \
                        "_dockets_" + str(date.today().strftime("%Y%m%d")) + ".json"
        with open(self.filename, 'w', encoding='utf-8') as f:
            json.dump(self.parsed_dockets, f, ensure_ascii=False, indent=4)
    
    def main(self):
        start_queue = time.time()
        self.build_docket_queue()
        if not self.docket_queue:
            print("Could not find .html files to parse. :(")
        else:
            queue_time = round(time.time() - start_queue,2)
            print("Successfully queued {c} dockets in {t} seconds.".format(c=self.docket_count,t=queue_time))
#            p = Pool(processes=self.process_count)
            start_parse = time.time()
            for docket in self.docket_queue:
                self.parse_docket(docket)
#            p.map(self.parse_docket, self.docket_queue)
            parse_time = round(time.time() - start_parse,2)
            print("Successfully parsed {c} dockets in {t} seconds.".format(c=self.parse_count,t=parse_time))
            if self.duplicates > 0:
                print("While parsing, we found {d} redundant case IDs. Don't worry, we took care of them.")
            self.output_to_local_dir()
            print("Saved to {p}".format(p=os.getcwd()+self.filename.replace("./","/")))
            print("\nAnd when the parser saw the breadth of its outputs, it wept for there were no more files to parse.")
                
    
class DocketParser:
    
    def __init__(self, docket):
        self.docket = docket
        self.parsed_docket = {"docket_flags":'',
                              "case_id":'',
                              "docket_text":[],
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
#        self.case_id = str(os.path).split(os.path.basename(docket))[0]
        self.case_id = docket[docket.rfind(str(os.path.sep))+1:].partition('.html')[0]
        self.miss_list = []
        self.roles = ['Defendant','Plaintiff','Petitioner','Respondent','Appellee','Appellant','Trustee','Creditor']
        self.main()
    
    def open_docket(self):
        with open(self.docket, "rb") as f:
            docket_html = f.read()
        self.soup = BeautifulSoup(docket_html, 'html.parser')
        self.parsed_docket["case_id"] = self.case_id
    
    def log_miss(self, unrecognized_html):
        self.miss_list.append(unrecognized_html)
    
    def parse_docket_header(self):
        for header in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            docket_title = ' '.join([header_element.get_text() for header_element in self.soup.find_all(header)])
            if docket_title:
                break
#        self.parsed_docket["docket_title"] = {"blob":docket_title}
        self.parsed_docket["docket_title"] += docket_title
    
    def parse_header(self, text, pertains_to_entire_docket=False, key=None):
#        header_dict = {"blob":text}
        ## DO MORE?
        if not pertains_to_entire_docket:
            self.parsed_docket["docket_header"] += text
        else:
            if "role_header" not in self.parsed_docket[pertains_to_entire_docket][key]:
                self.parsed_docket[pertains_to_entire_docket][key]["role_header"] = text
            else:
                self.parsed_docket[pertains_to_entire_docket][key]["role_header"] += text
            
    def parse_docket_footers(self, table):
        table_text = table.get_text()
        if "agistrate" not in table_text and ' ' not in table_text: #table_text.isupper() or
            self.parsed_docket["docket_flags"] += table_text
        elif "Docket Text" in table_text:
#            self.parsed_docket["docket_text"] = {"blob":table_text,"rows":[]}
            self.parsed_docket["docket_text"] = {"rows":[]}
            first = True
            for row in table.find_all('tr'):
                full_row = {"date":'',"number":'',"links":'',"text":''}
                for column in row.find_all('td'):
                    if first:
                        full_row["text"] = row.get_text()
                        first = False
                        break
                    else:
                        if "/" in column.get_text() and column.get_text().replace("/","").isnumeric():
                            full_row["date"] = column.get_text()
                        elif len(column.get_text()) < 6:
                            full_row["number"] = column.get_text().strip()
                        else:
                            full_row["text"] = column.get_text()
                full_row["links"] = [str(a['href']) for a in row.find_all('a') if a.has_attr('href')]
                self.parsed_docket["docket_text"]["rows"].append(full_row)
        elif "PACER" in table_text:
            self.parsed_docket["pacer_receipt"] += table_text
        elif "Date Filed" in table_text:
            self.parse_header(table_text)
        else:
            self.log_miss(table)
    
    def parse_sub_role(self, blob, tags, role, key, subkey, sub2key=None, sub3key=None):
        if blob.get_text():
            tags_sought = []
            tag_text = []
            for tag in tags:
                tags_sought.append(blob.find_all(tag))
            for tag in tags_sought:
                for subtag in tag:
                    tag_text.append(subtag.get_text().strip())
            print(tag_text)
            for count, text in enumerate(tag_text):
                tag_text[count] = ' '.join(text.strip().split())
            if any(element == "name_attempt" for element in [role, key, subkey, sub2key, sub3key]):
                sub_blob = tag_text[0] if tag_text else ''
            else:
                sub_blob = tag_text
#            sub_blob = ' '.join(tag_text)
#            sub_blob = ' '.join(sub_blob.strip().split())
            if sub3key:
                self.parsed_docket[role][key][subkey][sub2key][sub3key] = sub_blob
            elif sub2key:
                self.parsed_docket[role][key][subkey][sub2key] = sub_blob
            else:
                self.parsed_docket[role][key][subkey] = sub_blob
    
#    def parse_reps(self, role, key, rep_blob, rep_key=0):
#        estimated_reps = rep_blob.find_all('b')
#        if estimated_reps:
#            name_attempt = ' '.join(estimated_reps[0].get_text().split())
#            if len(estimated_reps) == 1:
#                if estimated_reps[0].get_text():
#                    self.parsed_docket[role][key]["reps"][rep_key] = {"name_attempt":name_attempt}
#                    self.parsed_docket[role][key]["reps"][rep_key]["blob"] = rep_blob.get_text()
#                    self.parse_sub_role(rep_blob, ['i','em'], role, key, "reps", rep_key, "italics")
#                    self.parse_sub_role(rep_blob, ['b','strong'], role, key, "reps", rep_key, "name_attempt")
#            else:
#                self.parsed_docket[role][key]["reps"][rep_key] = {"name_attempt":name_attempt}
#                current_blob, next_rep, remaining_blob = str(rep_blob).partition(str(estimated_reps[1]))
#                current_rep_blob = BeautifulSoup(current_blob, 'html.parser')
#                self.parsed_docket[role][key]["reps"][rep_key]["blob"] = current_rep_blob.get_text()
#                self.parse_sub_role(current_rep_blob, ['i','em'], role, key, "reps", rep_key, "italics")
#                self.parse_sub_role(current_rep_blob, ['b','strong'], role, key, "reps", rep_key, "name_attempt")
#                leftover_rep_blob = BeautifulSoup(next_rep + remaining_blob, 'html.parser')
#                self.parse_reps(role, key, leftover_rep_blob, rep_key+1)
    
    def parse_reps(self, role, key, rep_blob, rep_key=0):
        estimated_reps = rep_blob.find_all('b')
        if estimated_reps:
            for i in range(len(estimated_reps)):
                if not estimated_reps[i]:
                    continue
                elif not rep_blob.get_text():
                    continue
                name_attempt = ' '.join(estimated_reps[i].get_text().split())
                if i == len(estimated_reps) - 1:
                    self.parsed_docket[role][key]["reps"][rep_key] = {"name_attempt":name_attempt}
                    self.parsed_docket[role][key]["reps"][rep_key]["blob"] = rep_blob.get_text()
                    self.parse_sub_role(rep_blob, ['i','em'], role, key, "reps", rep_key, "italics")
                    self.parse_sub_role(rep_blob, ['b','strong'], role, key, "reps", rep_key, "name_attempt")
                else:
                    self.parsed_docket[role][key]["reps"][rep_key] = {"name_attempt":name_attempt}
                    current_blob, next_rep, remaining_blob = str(rep_blob).partition(str(estimated_reps[i+1]))
                    current_rep_blob = BeautifulSoup(current_blob, 'html.parser')
                    self.parsed_docket[role][key]["reps"][rep_key]["blob"] = current_rep_blob.get_text()
                    self.parse_sub_role(current_rep_blob, ['i','em'], role, key, "reps", rep_key, "italics")
                    self.parse_sub_role(current_rep_blob, ['b','strong'], role, key, "reps", rep_key, "name_attempt")
                    rep_blob = BeautifulSoup(next_rep + remaining_blob, 'html.parser')
                    rep_key += 1
    
    def parse_role(self, table, split, role):
        table_as_str = str(table)
        role_dict = {"blob":'',
                     "name_attempt":'',
                     "italics":'',
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
            soup = role_blob
        self.parse_sub_role(soup, ['i','em'], role, key, "italics")
        self.parse_sub_role(soup, ['b','strong'], role, key, "name_attempt")
        if "Assigned to" in pre_role or "Date Filed" in pre_role:
            soup = BeautifulSoup(pre_role, 'html.parser')
            self.parse_header(soup.get_text(),role,key)
        
    def parse_roles(self, table, split, roles):
        if len(roles) == 1:
            self.parse_role(table, split, roles[0].lower())
        else:
            #print(roles)
            underlines = table.find_all('u')
            length = len(underlines) - 1
            for count, underline in enumerate(underlines):
                if underline.get_text().strip() in roles:
                    if not table.get_text():
                        if str(table):
                            self.log_miss(table)
                        break
                    if count == length:
                        self.parse_role(table, underline, underline.get_text().strip().lower())
                    else:
#                        print("-----u and u+1-----")
#                        print(underlines[count])
#                        print(underlines[count+1])
                        pre_role, current_underline, leftover = str(table).partition(str(underlines[count]))
                        role_section, next_underline, remainder = leftover.partition(str(underlines[count + 1]))
                        role_section = pre_role + current_underline + role_section
#                        role_section, next_underline, remainder = str(table).partition(str(underlines[count + 1]))
                        role_blob = BeautifulSoup(role_section, 'html.parser')
#                        print('-----Start Role-----')
#                        print(' '.join(role_blob.get_text().strip().split()))
                        self.parse_role(role_blob, underline, underline.get_text().strip().lower())
                        table = BeautifulSoup(next_underline + remainder, 'html.parser')
#                        print('-----Next Role-----')
#                        print(' '.join(table.get_text().strip().split()))
                    
#                role = [x for x in roles if x == underline.get_text().strip()]
#                if role:
#                    
#                    
#            for role in roles:
#                underlines = table.find_all('u')
#                #print(underlines)
#                if len(underlines) == 1:
#                    self.parse_role(table, underlines[0], role.lower())
#                    break
#                current_role_portion, next_role, next_role_portion = str(table).partition(str(underlines[1]))
#                role_blob = BeautifulSoup(current_role_portion, 'html.parser')
#                print('-----Start Role-----')
#                print(' '.join(role_blob.get_text().strip().split()))
#                self.parse_role(role_blob, underlines[0], role.lower())
#                table = BeautifulSoup(next_role + next_role_portion, 'html.parser')
#                print('-----Next Role-----')
#                print(' '.join(table.get_text().strip().split()))
            
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
#path = '/Users/harper/Documents/petitioner.html'
#path = '/Users/harper/Documents/nu_work/nsf/noacri/code/test_dockets'
#path = '/Users/harper/Documents/nu_work/nsf/noacri/code/test_dockets/4-16-cv-00376-WEJ.html'
path = '/Users/harper/Documents/nu_work/nsf/noacri/code/test_dockets/1-16-cv-00863-TWT.html'

CorpusParser(path)

#if __name__ == "__main__":
#    CorpusParser(sys.argv[1])