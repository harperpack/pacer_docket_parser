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
from multiprocessing import Pool
import json
import sys
import random

class CorpusParser:
    
    def __init__(self, docket_dir, o_dir=None, threads=8):
        self.docket_directory = docket_dir
        self.output_directory = o_dir if o_dir else "./parsed_dockets/"
        self.process_count = threads
        self.docket_queue = []
        self.parsed_dockets = {}
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
        case_id = new_parse.case_id + "_0" + str(random.randint(1,99999))
        parsed_docket = {case_id: new_parse.parsed_docket}
        return parsed_docket
    
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
            p = Pool(processes=self.process_count)
            start_parse = time.time()
            results_list = p.map(self.parse_docket, self.docket_queue)
            self.parse_count = len(results_list)
            for result_dict in results_list:
                self.parsed_dockets.update(result_dict)
            parse_time = round(time.time() - start_parse,2)
            print("Successfully parsed {c} dockets in {t} seconds.".format(c=self.parse_count,t=parse_time))
            self.output_to_local_dir()
            print("Saved to {p}".format(p=os.getcwd()+self.filename.replace("./","/")))
            print("\nAnd when the parser saw the breadth of its outputs, it wept for there were no more files to parse.")
                
    
class DocketParser:
    
    def __init__(self, docket):
        self.docket = docket
        self.parsed_docket = {"docket_flags":'',
                              "case_id":'',
                              "addl_docket_fields":[],
                              "docket_text":[],
                              "docket_title":'',
                              "docket_header":'',
                              "role_header":'',
                              "pacer_receipt":'',
                              "defendant":{},
                              "plaintiff":{},
                              "petitioner":{},
                              "respondent":{},
                              "appellee":{},
                              "appellant":{},
                              "trustee":{},
                              "creditor":{},
                              "material witness":{},
                              "interested party":{},
                              "counter claimant":{},
                              "miscellaneous party":{},
                              "counter defendant":{},
                              "cross claimant":{},
                              "cross defendant":{},
                              "missed_parses":[],
                              }
        self.case_id = docket[docket.rfind(str(os.path.sep))+1:].partition('.html')[0]
        self.miss_list = []
        self.roles = ['Defendant','Plaintiff','Petitioner','Respondent',
                      'Appellee','Appellant','Trustee','Creditor', 
                      'Material Witness', 'Interested Party', 'Counter Claimant',
                      'Miscellaneous Party','Counter Defendant','Cross Claimant',
                      'Cross Defendant',
                      ]
        self.other_known_underlines = ['Pending Counts','Disposition','Q','R',
                                       'Highest Offense Level (Opening)','U',
                                       'Terminated Counts','Complaints','L',
                                       'Highest Offense Level (Terminated)',
                                       'Highest Offense Level']
        self.main()
    
    def open_docket(self):
        with open(self.docket, "rb") as f:
            docket_html = f.read()
        self.soup = BeautifulSoup(docket_html, 'html.parser')
        self.parsed_docket["case_id"] = self.case_id
    
    def parse_initial_charge_headers(self, html_str, role, key):
        first_header_val = ''
        second_header = ''
        leftover_text, bracket, remaining_html = html_str.partition("<")
        remaining = bracket+remaining_html
        if leftover_text:
            first_header = leftover_text
        else:
            first_header = "unknown_label"
        remaining_initial_row, start_next, finish_next = remaining.partition("<tr")
        remaining = start_next + finish_next
        soup = BeautifulSoup(remaining_initial_row, 'html.parser')
        for td in soup.find_all('td'):
            if not td.get_text() or td.get_text() == ' ':
                continue
            underline = td.find('u')
            if underline:
                second_header = underline.get_text()
                first_header_val = ''
            else:
                second_header = ''
                first_header_val = td.get_text()
        if first_header_val:
            self.update_addl_field_list([first_header, soup.get_text()], first_header, [first_header_val], '', role, key)
            first_header = ''
        return first_header, second_header, remaining
    
    def parse_charges(self, html_str, role, key):
        one_h, two_h, remaining = self.parse_initial_charge_headers(html_str, role, key)
        one_h_lines = ['']
        one_h_val = ['']
        two_h_val = ['']
        two_h_lines = ['']
        soup = BeautifulSoup(remaining, 'html.parser')
        for row in soup.find_all('tr'):
            for row_num, td in enumerate(row.find_all('td')):
                underline = td.find('u')
                if row_num == 0:
                    if underline:
                        if one_h:
                            self.update_addl_field_list(one_h_lines, one_h, one_h_val, '', role, key)
                        if two_h:
                            self.update_addl_field_list(two_h_lines, two_h, two_h_val, '', role, key)
                        one_h = underline.get_text()
                        one_h_lines = [underline.get_text()]
                        one_h_val = ['']
                        two_h = ''
                        two_h_val = ['']
                        two_h_lines = ['']
                    else:
                        if len(one_h_lines) == 1 and one_h_lines[0] == '':
                            one_h_lines = [td.get_text()]
                        else:
                            one_h_lines.append(td.get_text())
                        if len(one_h_val) == 1 and one_h_val[0] == '':
                            one_h_val = [td.get_text()]
                        else:
                            one_h_val.append(td.get_text())
                elif row_num == 1:
                    continue
                elif row_num == 2:
                    if underline:
                        if two_h:
                            self.update_addl_field_list(two_h_lines, two_h, two_h_val, '', role, key)
                        two_h = underline.get_text()
                        two_h_lines = [underline.get_text()]
                        two_h_val = ['']
                    else:
                        if two_h:
                            if len(two_h_lines) == 1 and two_h_lines[0] == '':
                                two_h_lines = [td.get_text()]
                            else:
                                two_h_lines.append(td.get_text())
                            if len(two_h_val) == 1 and two_h_val[0] == '':
                                two_h_val = [td.get_text()]
                            else:
                                two_h_val.append(td.get_text())
                        else:
                            if len(one_h_lines) == 1 and one_h_lines[0] == '':
                                one_h_lines = [td.get_text()]
                            else:
                                one_h_lines.append(td.get_text())
                            if len(one_h_val) == 1 and one_h_val[0] == '':
                                one_h_val = [td.get_text()]
                            else:
                                one_h_val.append(td.get_text())
                else:
                    print("~~~~~MORE THAN 3 TDs~~~~~")
                    if underline:
                        if one_h:
                            self.update_addl_field_list(one_h_lines, one_h, one_h_val, '', role, key)
                        if two_h:
                            self.update_addl_field_list(two_h_lines, two_h, two_h_val, '', role, key)
                        one_h = underline.get_text()
                        one_h_lines = [underline.get_text()]
                        one_h_val = ['']
                        two_h = ''
                        two_h_val = ['']
                        two_h_lines = ['']
                    else:
                        if len(one_h_lines) == 1 and one_h_lines[0] == '':
                            one_h_lines = [td.get_text()]
                        else:
                            one_h_lines.append(td.get_text())
                        if len(one_h_val) == 1 and one_h_val[0] == '':
                            one_h_val = [td.get_text()]
                        else:
                            one_h_val.append(td.get_text())
        if one_h:
            self.update_addl_field_list(one_h_lines, one_h, one_h_val, '', role, key)
        if two_h:
            self.update_addl_field_list(two_h_lines, two_h, two_h_val, '', role, key)
#        self.update_addl_field_list(line[], name, value[], key=role, rolekey=key)
    
    def parse_docket_header(self):
        for header in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
            docket_title = ' '.join([header_element.get_text() for header_element in self.soup.find_all(header)])
            if docket_title:
                break
        self.parsed_docket["docket_title"] += docket_title
    
    def parse_header(self, text, pertains_to_entire_docket=False, key=None):
        if not pertains_to_entire_docket:
            self.parsed_docket["docket_header"] += text
        else:
            texts = text.split('\n')
            for t in texts:
                if not t:
                    continue
                t = pertains_to_entire_docket + str(key) + " |*| " + t + '\n'
                self.parsed_docket["role_header"] += t
                
    def parse_docket_text(self, table, table_text):
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
            
    def parse_docket_footers(self, table):
        table_text = table.get_text()
        if "agistrate" not in table_text and ' ' not in table_text: 
            self.parsed_docket["docket_flags"] += table_text.replace("\n","")
        elif "Docket Text" in table_text:
            self.parse_docket_text(table, table_text)
#            self.parsed_docket["docket_text"] = {"rows":[]}
#            first = True
#            for row in table.find_all('tr'):
#                full_row = {"date":'',"number":'',"links":'',"text":''}
#                for column in row.find_all('td'):
#                    if first:
#                        full_row["text"] = row.get_text()
#                        first = False
#                        break
#                    else:
#                        if "/" in column.get_text() and column.get_text().replace("/","").isnumeric():
#                            full_row["date"] = column.get_text()
#                        elif len(column.get_text()) < 6:
#                            full_row["number"] = column.get_text().strip()
#                        else:
#                            full_row["text"] = column.get_text()
#                full_row["links"] = [str(a['href']) for a in row.find_all('a') if a.has_attr('href')]
#                self.parsed_docket["docket_text"]["rows"].append(full_row)
        elif "PACER" in table_text:
            self.parsed_docket["pacer_receipt"] += table_text
        elif "Date Filed" in table_text:
            self.parse_header(table_text)
        else:
            self.parsed_docket["missed_parses"].append(table.get_text())
    
    def parse_sub_role(self, blob, tags, role, key, subkey, sub2key=None, sub3key=None):
        if blob.get_text():
            tags_sought = []
            tag_text = []
            for tag in tags:
                tags_sought.append(blob.find_all(tag))
            for tag in tags_sought:
                for subtag in tag:
                    tag_text.append(subtag.get_text().strip())
            for count, text in enumerate(tag_text):
                tag_text[count] = ' '.join(text.strip().split())
            if any(element == "name_attempt" for element in [role, key, subkey, sub2key, sub3key]):
                sub_blob = tag_text[0] if tag_text else ''
            else:
                sub_blob = tag_text
            if sub3key:
                self.parsed_docket[role][key][subkey][sub2key][sub3key] = sub_blob
            elif sub2key:
                self.parsed_docket[role][key][subkey][sub2key] = sub_blob
            else:
                self.parsed_docket[role][key][subkey] = sub_blob
    
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
                    self.parsed_docket[role][key]["reps"][rep_key]["blob"] = rep_blob.get_text().strip().split('\n')
                    self.parse_sub_role(rep_blob, ['i','em'], role, key, "reps", rep_key, "fields")
                    self.parse_sub_role(rep_blob, ['b','strong'], role, key, "reps", rep_key, "name_attempt")
                else:
                    self.parsed_docket[role][key]["reps"][rep_key] = {"name_attempt":name_attempt}
                    current_blob, next_rep, remaining_blob = str(rep_blob).partition(str(estimated_reps[i+1]))
                    current_rep_blob = BeautifulSoup(current_blob, 'html.parser')
                    self.parsed_docket[role][key]["reps"][rep_key]["blob"] = current_rep_blob.get_text().strip().split('\n')
                    self.parse_sub_role(current_rep_blob, ['i','em'], role, key, "reps", rep_key, "fields")
                    self.parse_sub_role(current_rep_blob, ['b','strong'], role, key, "reps", rep_key, "name_attempt")
                    rep_blob = BeautifulSoup(next_rep + remaining_blob, 'html.parser')
                    rep_key += 1
    
    def parse_role(self, table, split, role):
        table_as_str = str(table)
        role_dict = {"blob":'',
                     "name_attempt":'',
                     "italics":'',
                     "reps":{"total_blob":''},
                     "charges":[]}
        pre_role, throwaway, post_role = table_as_str.partition(str(split))
        soup = BeautifulSoup(post_role, 'html.parser')
        if "represented" in post_role:
            role_blob = BeautifulSoup(post_role.partition("represented")[0], 'html.parser')
            role_dict["blob"] = role_blob.get_text().strip().split('\n')
            end_reps = soup.find('u')
            if end_reps:
                rep_section, next_underline, charges_section = post_role.partition("represented")[2].partition(end_reps.get_text())
#                charges_blob = BeautifulSoup(next_underline + charges_section, 'html.parser')
                charges_blob = next_underline + charges_section
            else:
                rep_section = post_role.partition("represented")[2]
                charges_blob = ''
            rep_blob = BeautifulSoup(rep_section, 'html.parser')
            role_dict["reps"]["total_blob"] = rep_blob.get_text().strip().split('\n')
        else:
            end_reps = soup.find('u')
            if end_reps:
                role_blob, next_underline, charges_section = post_role.partition(end_reps.get_text())
                role_section = BeautifulSoup(role_blob, 'html.parser').get_text()
                charges_blob = next_underline + charges_section
            else:
                role_section = soup.get_text()
                charges_blob = ''
            role_dict["blob"] = role_section.strip().split('\n')
        key = len(self.parsed_docket[role].keys())
        self.parsed_docket[role][key] = role_dict
        if self.parsed_docket[role][key]["reps"]["total_blob"]:
            self.parse_reps(role, key, rep_blob)
            soup = role_blob
        if charges_blob:
            self.parse_charges(charges_blob, role, key)
        self.parse_sub_role(soup, ['i','em'], role, key, "italics")
        self.parse_sub_role(soup, ['b','strong'], role, key, "name_attempt")
        if "Assigned to" in pre_role or "Date Filed" in pre_role:
            soup = BeautifulSoup(pre_role, 'html.parser')
            self.parse_header(soup.get_text(),role,key)

    def parse_roles(self, table, split, roles):
        if len(roles) == 1:
            self.parse_role(table, split, roles[0].lower())
        else:
            underlines = table.find_all('u')
            length = len(underlines) - 1
            for count, underline in enumerate(underlines):
                if underline.get_text().strip() in roles:
                    if not table.get_text():
                        if str(table):
                            self.parsed_docket["missed_parses"].append(table.get_text())
                        break
                    if count == length:
                        self.parse_role(table, underline, underline.get_text().strip().lower())
                    else:
                        pre_role, current_underline, leftover = str(table).partition(str(underlines[count]))
                        role_section, next_underline, remainder = leftover.partition(str(underlines[count + 1]))
                        role_section = pre_role + current_underline + role_section
                        role_blob = BeautifulSoup(role_section, 'html.parser')
                        self.parse_role(role_blob, underline, underline.get_text().strip().lower())
                        table = BeautifulSoup(next_underline + remainder, 'html.parser')
    
    def clean_underline(self, underline):
        return ' '.join([line for line in underline.get_text().strip().split() if "(" not in line])
    
    def update_role_list(self):
        searchable_area = self.soup.get_text().partition("Docket Text")[0]
        searchable_soup = BeautifulSoup(searchable_area, 'html.parser')
        for underline in searchable_soup.find_all('u'):
            underline = self.clean_underline(underline)
#            underline = ' '.join(underline.get_text().strip().split())
            if underline in self.other_known_underlines:
                continue
            elif underline in self.roles:
                continue
            else:
                print("#######======> NEW ROLE: {r}|{c}".format(r=underline, c=self.case_id))
                self.roles.append(underline)
                self.parsed_docket[underline.lower()] = {}

            
    def parse_docket(self):
        docket_tables = self.soup.find_all('table')
        if not docket_tables:
            self.parsed_docket["missed_parses"].append(self.soup.get_text())
        for table in docket_tables:
            table_topic = table.find('u')
            if not table_topic:
                self.parse_docket_footers(table)
            else:
                table_text = table.get_text()
                roles = [role for role in self.roles if role in table_text]
                if not roles:
                    self.parse_docket_footers(table)
#                    self.parsed_docket["missed_parses"].append(table.get_text())
                else:
                    if "Docket Text" in table_text:
                        self.parse_docket_text(table, table_text)
                    else:
                        self.parse_roles(table, table_topic, roles)
    
    def update_addl_field_list(self, line, name, value, loc="Header", key="addl_docket_fields", rolekey=None):
        if key == "addl_docket_fields":
            self.parsed_docket[key].append({#"line":line,
                                            "field_name_attempt":name,
                                            "field_value_attempt":value,
                                            "found_in_section":loc})
        elif key.title() in self.roles:
            field_name = '_'.join(name.strip().lower().split())
            field_value = [val.strip() for val in value]
            self.parsed_docket[key][rolekey]["charges"].append({#"line":line,
                                            "field_name_attempt":field_name,
                                            "field_value_attempt":field_value})
        else:
            print("WRONG ADDL FIELD ENTRY")
            self.parsed_docket["missed_parses"].append(line)
    
    def refine_header_parsing(self):
        title = self.parsed_docket.pop("docket_title").split('\n')
        title += self.parsed_docket.pop("docket_header").split('\n')
        for line in title:
            if not line:
                continue
            elif "court" in line.lower() and "u.s." in line.lower():
                self.update_addl_field_list(line, "court_type", line)
            elif "court" in line.lower() and "united states" in line.lower():
                self.update_addl_field_list(line, "court_type", line)
            elif "district of " in line.lower() and "(" in line:
                court_name, paren, city = line.partition("(")
                court_state = court_name.partition(" of ")[2]
                self.update_addl_field_list(line, "district", court_name.strip())
                self.update_addl_field_list(line, "court_city", city.replace(")",''))
                self.update_addl_field_list(line, "court_state", court_state.strip())
            elif 'USDC' in line:
#                print(line.lower())
                if "other" in line.lower() and "court" in line.lower() and "case" in line.lower() and "number" in line.lower():
                    field_name, colon, field_value = line.partition(":")
                    field_name = '_'.join(field_name.strip().lower().split())
                    self.update_addl_field_list(line, field_name, field_value.strip())
                elif "(" in line:
                    court_name, paren, city = line.partition("(")
                    self.update_addl_field_list(line, "court_city", city.replace(")",''))
                    if " of " in line.lower():
                        court_state = court_name.partition(" of ")[2]
                        self.update_addl_field_list(line, "district", court_name.strip())
                        self.update_addl_field_list(line, "court_state", court_state.strip())
                    else:
                        tokens = court_name.split()
                        if tokens[0] == 'USDC':
                            if tokens[1].lower() in ['southern','eastern','western','northern','middle','central']:
                                self.update_addl_field_list(line, "district", court_name.partition('USDC')[2].strip())
                                court_state = tokens[2]
                        else:
                            court_state = tokens[-2]
                        self.update_addl_field_list(line, "court_state", court_state.strip())
                else:
                    self.update_addl_field_list(line, "district", line.strip())
            elif " circuit" in line.lower():
                self.update_addl_field_list(line, "circuit", line.strip())
            elif self.case_id.replace("_",'').replace("-",'').replace(":",'') in line.replace("_",'').replace("-",'').replace(":",''):
                case_type = line.partition(":")[0]
                if "civil" in case_type.lower():
                    self.update_addl_field_list(line, "case_type", "Civil")
                elif "criminal" in case_type.lower():
                    self.update_addl_field_list(line, "case_type", "Criminal")
                else:
                    self.update_addl_field_list(line, "case_type", case_type)
            elif " v. " in line.lower():
                if "case title" not in line.lower():
                    self.update_addl_field_list(line, "case_title", line)
                else:
                    case_title = line.partition("itle:")[2].strip()
                    self.update_addl_field_list(line, "case_title", case_title)
            elif ":" in line:
                field_name, colon, field_value = line.partition(":")
                field_name = '_'.join(field_name.strip().lower().split())
                self.update_addl_field_list(line, field_name, field_value.strip())
            else:
                if line and line.strip() != '' and line.strip() != ' ':
                    self.update_addl_field_list(line, "unsure", line)
        role_header = self.parsed_docket.pop("role_header").split("\n")
        for line in role_header:
            if not line:
                continue
            elif ":" in line:
                location, separator, line_value = line.partition(" |*| ")
                field_name, colon, field_value = line_value.partition(":")
                field_name = '_'.join(field_name.strip().lower().split())
                self.update_addl_field_list(line_value, field_name, field_value.strip(), location)
            else:
                location, separator, line_value = line.partition(" |*| ")
                self.update_addl_field_list(line_value, "unsure", '', location)
    
    def clean_number(self, number, merge=''):
        to_remove = ['(',')','-',',','_','.','#']
        for element in to_remove:
            number = number.replace(element, ' ')
        return merge.join(number.strip().split())
    
    def parse_contact_blob(self, role, key, num, non_reps=False, formatted=None):
        other_fields = []
        fields = {'org':'',
                  'building':'',
                  'postal_address':['',''],
                  'city':'',
                  'state':'',
                  'zip':'',
                  'phone':'',
                  'fax':'',
                  'email':'',
                  'designation':[],
                  'date_terminated':'',
                  'pro_se':'',
                  'lead':'',
                  'to_be_noticed':'',
                  'district':'',
                  'id_number':'',
                  'title':'',
                  'other_italics':[]
                  }
        org_words = ['Office','Program','P.C.','Inc.','LP',' inc.','Corp','LLC',
                     'LLP','LLP-A','PC','OFFICE','PROGRAM','INC.','CORP','Group',
                     'GROUP','Associates','ASSOCIATES','&','Team','TEAM','Squad',
                     'SQUAD','UNIT','Unit','L.P.','L.L.C.','GMBH','Gmbh','LLc',
                     'Llc','Company','Corporation','Co.','co.','L.p.','L.l.c',
                     'l.l.c','Firm']
        postal_words = ['P.O.','PO ','Box','BOX','St.','Street','Blvd','Hwy','Jnc',
                        'Boulevard','Dr.','Drive','Ave','Highway','Junction',
                        'Rd','Road','Circle','Place','Ridge','Terrace','Sq.',
                        'Square','Lane','Ln','Crescent','Alley','Bvd','Court',
                        'Expressway','Freeway','Jct','Parkway', 'North', 'South',
                        'East', 'West', 'NW', 'SW','Pier']
        second_address = ['Suite','Ste','Floor','Room',]
        building_words = ['Tower','Center','Building','Courthouse','Complex',
                          'Prison','Jail','Penitentiary','Facility','USP','College',
                          'University','Institute','Institution','Supermax']
        states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", 
                           "FL", "GA","HI", "ID", "IL", "IN", "IA", "KS", "KY", 
                           "LA", "ME", "MD","MA", "MI", "MN", "MS", "MO", "MT",
                           "NE", "NV", "NH", "NJ","NM", "NY", "NC", "ND", "OH", 
                           "OK", "OR", "PA", "RI", "SC","SD", "TN", "TX", "UT",
                           "VT", "VA", "WA", "WV", "WI", "WY"]
        title_words = ["Chief","Warden","Principal","Attorney","Deputy","Judge",
                       "Detective","Officer","Lead","Senior","Assistant","Junior",
                       "Acting","Advocate","Court","Commissioner","President",
                       "Liaison","Bondsman","Defender","Counsel","Sheriff"]
        if not non_reps:
            lines = self.parsed_docket[role][key]["reps"][num].pop("blob")
            try:
                italics = self.parsed_docket[role][key]["reps"][num].pop("fields")
            except(KeyError):
                italics = []
        else:
            lines = [non_reps]
            italics = formatted
        for line in lines:
            #print(line)
            tokens = line.split()
            if not line:
                continue
            elif line == '.' or line.lower() == "v.":
                continue
            elif line.strip() == '' or line.strip == ' ':
                continue
            elif not non_reps and ' '.join(line.split()) == self.parsed_docket[role][key]["reps"][num]["name_attempt"]:
                continue
            elif 'by' in line.strip()[0:2]:
                continue
            elif "@" in line and "." in line:
                premail, colon, email = line.partition(":")
                fields["email"] = email
            elif len(tokens) > 1 and tokens[-2] in states:
                fields["state"] = tokens[-2]
                if len(tokens) > 3:
                    fields["city"] = ' '.join(tokens[0:-2]).replace(",","")
                else:
                    fields["city"] = tokens[0].replace(",","")
                fields["zip"] = tokens[-1]
            elif len(tokens) > 2 and tokens[-1].isnumeric() and tokens[-2].upper() in states:
                fields["state"] = tokens[-2]
                if len(tokens) > 3:
                    fields["city"] = ' '.join(tokens[0:-2]).replace(",","")
                else:
                    fields["city"] = tokens[0].replace(",","")
                fields["zip"] = tokens[-1]
            elif any(word in line for word in postal_words):
                fields["postal_address"][0] = line
            elif any(word in line.title() for word in postal_words):
                fields["postal_address"][0] = line
            elif any(word in line.title() for word in second_address):
                fields["postal_address"][1] = line
            elif any(word in line for word in org_words):
                fields["org"] = line
            elif any(word in line.title() for word in building_words):
                fields["building"] = line
            elif len(tokens) > 1 and tokens[0].isnumeric() and tokens[1].isalpha():
                fields["postal_address"][0] = line
            elif len(tokens) > 2 and tokens[0].isnumeric() and tokens[2].isalpha():
                fields["postal_address"][0] = line
            elif self.clean_number(line).isnumeric():
                if "(" in line and ")" in line:
                    fields["phone"] = self.clean_number(line)
                elif len(self.clean_number(line,' ').split()) == 1:
                    fields["id_number"] = line
                elif len(self.clean_number(line,' ').split()[-1]) == 4 and len(self.clean_number(line,' ').split()[-2]) == 3:
                    fields["phone"] = self.clean_number(line)
                elif len(self.clean_number(line)) == 10 or len(self.clean_number(line)) == 7:
                    fields["phone"] = self.clean_number(line)
                else:
                    fields["id_number"] = line
            elif "Fax" in line.title():
                fax = line.partition(":")[2]
                fields["fax"] = self.clean_number(fax)
            elif line in italics:
                if "lead" in line.lower():
                    fields["lead"] = line
                elif "designation" in line.lower():
                    designation = line.partition(":")[2]
                    fields["designation"].append(designation)
                elif "terminated" in line.lower():
                    date = line.partition(":")[2]
                    fields["date_terminated"] = date
                elif "notice" in line.lower():
                    fields["to_be_noticed"] = line
                else:
                    fields["other_italics"].append(line)
            elif "pro se" in line.lower():
                fields["pro_se"] = line
            elif any(word in line.title() for word in self.roles):
                fields["title"] = line
            elif any(word in line.title() for word in title_words):
                fields["title"] = line
            elif "district" in line.lower():
                fields["district"] = line
            elif line[-6:].isnumeric():
                fields["id_number"] = line
            else:
                if "(" in line and ")" in line and "-" in line:
                    fields["phone"] = line
                else:
                    fields["unsure"] = line
        for field in fields.keys():
            if fields[field]:
                field_val = fields[field]
                if isinstance(field_val, list):
                    if field == 'postal_address':
                        field_val = ' '.join(field_val)
                        if field_val.strip() != "" and field_val.strip() != ' ' and field_val != "\n":
                            other_fields.append({"field_name_attempt":field,"field_value_attempt":' '.join(field_val.strip().split())})
                    else:
                        for val in field_val:
                            other_fields.append({"field_name_attempt":field,"field_value_attempt":' '.join(val.strip().split())})
                else:
                    other_fields.append({"field_name_attempt":field,"field_value_attempt":' '.join(field_val.strip().split())})
        if not non_reps:
            self.parsed_docket[role][key]["reps"][num]["other_fields"] = other_fields
        else:
            if "other_fields" in self.parsed_docket[role][key]:
                self.parsed_docket[role][key]["other_fields"] += other_fields
            else:
                self.parsed_docket[role][key]["other_fields"] = other_fields
#            if not self.parsed_docket[role][key]["other_fields"]:
#                self.parsed_docket[role][key]["other_fields"] = other_fields
#            else:
#                for field in other_fields:
#                    self.parsed_docket[role][key]["other_fields"].append(field)
#        other_fields.append({"field_name_attempt":field_name,"field_value_attempt":field_val})
    
    def refine_role_blobs(self, role, key, name):
        addl_fields = []
        lines = self.parsed_docket[role][key].pop("blob")
        italics = self.parsed_docket[role][key].pop("italics")
        building_words = ['Tower','Center','Building','Courthouse','Complex',
                          'Prison','Jail','Penitentiary','Facility','USP',]
        title_words = ["Chief","Warden","Principal","Attorney","Deputy","Judge",
                       "Detective","Officer","Lead","Senior","Assistant","Junior",
                       "Acting","Advocate","Court","Commissioner","President",
                       "Liaison","Esq.","ESQ"]
        for line in lines:
            parsed_elsewhere = False
            if not line:
                continue
            elif line == self.parsed_docket[role][key]["name_attempt"]:
                continue
            elif "also known as" in line:
                field_name = "alias"
                field_val = line.partition("also known as")[2]
            elif ":" in line:
                field_name, colon, field_val = line.partition(":")
                if "terminated" in field_name.lower():
                    field_name = "date_terminated"
                else:
                    field_name = '_'.join(field_name.strip().lower().split())
            elif any(word in line.title() for word in building_words):
                if any(word in line.title() for word in title_words):
                    if name in line:
                        line = line.replace(name, ' ')
                    field_name = "role_or_position"
                    field_val = line
                else:
                    field_name = "building"
                    field_val = line
            elif line in italics:
                if name in line:
                    line = line.replace(name, ' ')
                field_name = "role_or_position"
                field_val = line
            elif any(word in line.title() for word in title_words):
                if name in line:
                    line = line.replace(name, ' ')
                field_name = "role_or_position"
                field_val = line
            else:
                self.parse_contact_blob(role, key, '', line, italics)
                parsed_elsewhere = True
            if not parsed_elsewhere:
                field_val = ' '.join(field_val.strip().split())
                addl_fields.append({"field_name_attempt":field_name,"field_value_attempt":field_val})
        if addl_fields:
            if "other_fields" in self.parsed_docket[role][key]:
                self.parsed_docket[role][key]["other_fields"] += addl_fields
            else:
                self.parsed_docket[role][key]["other_fields"] = addl_fields
        self.parsed_docket[role][key]["reps"].pop("total_blob")
        for num in self.parsed_docket[role][key]["reps"].keys():
            self.parse_contact_blob(role, key, num)
    
    def refine_blob_parsing(self):
        for role in self.roles:
            if self.parsed_docket[role.lower()]:
                for num in self.parsed_docket[role.lower()].keys():
                    self.refine_role_blobs(role.lower(), num, self.parsed_docket[role.lower()][num]["name_attempt"])
                    
    def main(self):
        self.open_docket()
        self.update_role_list()
        self.parse_docket_header()
        self.parse_docket()
        self.refine_header_parsing()
        self.refine_blob_parsing()


if __name__ == "__main__":
    CorpusParser(sys.argv[1])