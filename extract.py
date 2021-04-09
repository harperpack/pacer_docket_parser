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

    def __init__(self, docket_dir, o_dir="./parsed_dockets/pipeline/", threads=8, qmax=1):
        self.docket_directory = docket_dir
        self.output_directory = o_dir
        self.process_count = threads
        self.docket_queue = []
        self.queue_of_queues = []
        self.queue_max = qmax
        self.parsed_dockets = {}
        self.docket_count = 0
        self.parse_count = 0
        self.main()

    def track_completed(self):
        self.completed_queue = {"complete":[],"quarantine":[]}

    def build_docket_queue(self):
        if os.path.isfile(self.docket_directory):
            self.docket_queue.append(self.docket_directory)
            self.docket_count += 1
        elif os.path.isdir(self.docket_directory):
            for (dirpath, dirnames, filenames) in os.walk(self.docket_directory):
                for filename in filenames:
                    if filename.endswith('.html'):
                        full_filename = str(os.path.join(dirpath, filename))
                        if full_filename not in self.completed_queue["complete"]:
                            self.docket_queue.append(os.path.join(dirpath, filename))
                            self.docket_count += 1
        if self.docket_count > self.queue_max:
            target = 0
            for i in range(self.docket_count):
                if target + self.queue_max >= self.docket_count - 1:
                    self.queue_of_queues.append(self.docket_queue[target:])
                    break
                else:
                    self.queue_of_queues.append(self.docket_queue[target:target+self.queue_max])
                    target += self.queue_max
        else:
            self.queue_of_queues.append(self.docket_queue)

    def progress(self, count, length):
        # drawn from https://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console/27871113
        length -= 1
        width = 60
        if length > 0:
            completion = float(count / length)
        else:
            completion = float(1)
        filled = int(width * completion)
        prog = round(100 * completion,1)
        prog = str(prog) + "%" + ' ' * (5 - len(str(prog)))
        progress_bar = '█' * filled + '-' * (width - filled)
        big = width * ' '
        sys.stdout.write('\r|{b}| {p} \t{f}\r'.format(b=progress_bar,p=prog, f=big))
        sys.stdout.flush()

    def parse_docket(self, docket):
        new_parse = DocketParser(docket)
        if "FAILURE" not in new_parse.parsed_docket:
            case_id = new_parse.case_id + "_0" + str(random.randint(1,99999))
            parsed_docket = {case_id: new_parse.parsed_docket}
            filename = ''.join([self.output_directory,case_id,".json"])
            with open(filename, "w") as f:
                json.dump(parsed_docket, f, ensure_ascii=False, indent=4)
            return '1'

    def output_to_local_dir(self, count, total):
        Path(self.output_directory).mkdir(parents=True, exist_ok=True)
        self.filename = self.output_directory + str(self.parse_count) + \
                        "_dockets_" + str(date.today().strftime("%Y%m%d")) + \
                        "_" + str(count) + "_of_" + str(total) + ".json"
        with open(self.filename, 'w', encoding='utf-8') as f:
            json.dump(self.parsed_dockets, f, ensure_ascii=False, indent=4)

    def ensure_output_dirs(self):
        if not os.path.exists(self.output_directory):
            os.makedirs(self.output_directory)

    def main(self):
        try:
            start_queue = time.time()
            self.track_completed()
            self.build_docket_queue()
            self.ensure_output_dirs()
            if not self.docket_queue:
                print("Could not find .html files to parse. :(")
            else:
                queue_time = round(time.time() - start_queue,2)
                print("Successfully queued {c} dockets in {t} seconds.".format(c=self.docket_count,t=queue_time))
                length = len(self.queue_of_queues)
                for count, queue in enumerate(self.queue_of_queues, start=1):
                    self.docket_queue = queue
                    self.parsed_dockets = {}
                    p = Pool(processes=self.process_count)
                    start_parse = time.time()
                    results_list = []
                    results_list = p.map(self.parse_docket, self.docket_queue)
                    p.close()
                    p.join()
                    self.parse_count = len(results_list)
                    parse_time = round(time.time() - start_parse,2)
                    self.completed_queue["complete"] += queue
                    self.progress(count, length)
                print("\nCompleted in {s}s.\n".format(s=round(time.time() - start_queue,2)))
                print("\nAnd when the parser saw the breadth of its outputs, it wept for there were no more files to parse.")
        except Exception as e:
            print('Now is the winter of our discontent: parsing failed with exception "{f}"'.format(f=e))
        if self.completed_queue:
            Path(self.output_directory).mkdir(parents=True, exist_ok=True)
            self.filename = self.output_directory + str(date.today().strftime("%Y%m%d")) + "_parselist.json"
            with open(self.filename, 'w', encoding='utf-8') as f:
                json.dump(self.completed_queue, f, ensure_ascii=False, indent=4)

class DocketParser:

    def __init__(self, docket):
        self.docket = docket
        self.parsed_docket = {"docket_flags":'',
                              "case_id":'',
                              "docket_case_id":'',
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
                              "thirdparty plaintiff":{},
                              "thirdparty defendant":{},
                              "accountant": {},
                              "amicus": {},
                              "appraiser": {},
                              "arbitrator": {},
                              "assistant u.s. trustee": {},
                              "attorney": {},
                              "auctioneer": {},
                              "auditor": {},
                              "broker": {},
                              "claimant": {},
                              "consolidated claimant": {},
                              "consolidated counter claimant": {},
                              "consolidated counter defendant": {},
                              "consolidated cross claimant": {},
                              "consolidated cross defendant": {},
                              "consolidated defendant": {},
                              "consolidated plaintiff": {},
                              "consolidated third party": {},
                              "consolidated third party plaintiff": {},
                              "consultant": {},
                              "consumer privacy": {},
                              "creditor committee": {},
                              "creditor committee chair": {},
                              "custodian": {},
                              "debtor": {},
                              "debtor in possession": {},
                              "examiner": {},
                              "financial advisor": {},
                              "foreign representative": {},
                              "garnishee": {},
                              "health care ombudsman": {},
                              "in re": {},
                              "interim trustee": {},
                              "interpleader": {},
                              "intervenor": {},
                              "intervenor defendant": {},
                              "intervenor plaintiff": {},
                              "joint debtor": {},
                              "judge": {},
                              "liquidator": {},
                              "mediator": {},
                              "movant": {},
                              "non-filing spouse": {},
                              "objector": {},
                              "other professional": {},
                              "partner": {},
                              "petitioning creditor": {},
                              "realtor": {},
                              "receiver": {},
                              "special counsel": {},
                              "special master": {},
                              "stockholder": {},
                              "successor trustee": {},
                              "surveyor": {},
                              "taxpayer": {},
                              "third party defendant": {},
                              "third party plaintiff": {},
                              "u.s. trustee": {},
                              "witness ": {},
                              "consolidated third party defendant": {},
                              "fourth party defendant": {},
                              "fourth party plaintiff": {},
                              "interested non-party": {},
                              "notice party": {},
                              "unknown": {},
                              "surety": {},
                              "interpreter": {},
                              "estate plaintiff": {},
                              "estate defendant": {},
                              "relief defendant": {},
                              "executor defendant": {},
                              "executor plaintiff": {},
                              "estate of": {},
                              "mediation assistance program counsel": {},
                              "recruited counsel": {},
                              "relator": {},
                              "deponent": {},
                              "deponent": {},
                              "proposed": {},
                              "court monitor": {},
                              "consol defendant": {},
                              "notice only party":{},
                              "notice":{},
                              "consol plaintiff":{},
                              "consol counter defendant":{},
                              "habeas attorney general service list":{},
                              "mediator (adr panel)":{},
                              "in re debtor":{},
                              "real party in interest defendant": {},
                              "witness": {},
                              "counter-claimant": {},
                              "counter-defendant": {},
                              "intervenor dft": {},
                              "miscellaneous": {},
                              "cross-defendant": {},
                              "cross-claimant": {},
                              "3rd party plaintiff": {},
                              "3rd party defendant": {},
                              "neutral": {},
                              "guardian ad litem party": {},
                              "requesting party": {},
                              "objecting party": {},
                              "special mediation counsel": {},
                              "URL_path":str(self.docket),
                              "missed_parses":[],
                              }
        self.case_id = docket[docket.rfind(str(os.path.sep))+1:].partition('.html')[0]
        self.miss_list = []
        self.roles = ['Defendant','Plaintiff','Petitioner','Respondent',
                      'Appellee','Appellant','Trustee','Creditor',
                      'Material Witness', 'Interested Party', 'Counter Claimant',
                      'Miscellaneous Party','Counter Defendant','Cross Claimant',
                      'Cross Defendant','ThirdParty Plaintiff','ThirdParty Defendant',
                      'Accountant', 'Amicus', 'Appraiser', 'Arbitrator', 'Assistant U.S. Trustee',
                      'Attorney', 'Auctioneer', 'Auditor', 'Broker', 'Claimant',
                      'Consolidated Claimant', 'Consolidated Counter Claimant',
                      'Consolidated Counter Defendant', 'Consolidated Cross Claimant',
                      'Consolidated Cross Defendant', 'Consolidated Defendant',
                      'Consolidated Plaintiff', 'Consolidated Third Party', 'Consolidated Third Party Plaintiff',
                      'Consultant', 'Consumer Privacy', 'Creditor Committee', 'Creditor Committee Chair',
                      'Custodian', 'Debtor', 'Debtor In Possession', 'Examiner', 'Financial Advisor',
                      'Foreign Representative', 'Garnishee', 'Health Care Ombudsman', 'In Re', 'Interim Trustee',
                      'Interpleader', 'Intervenor', 'Intervenor Defendant', 'Intervenor Plaintiff',
                      'Joint Debtor', 'Judge', 'Liquidator', 'Mediator', 'Movant', 'Non-Filing Spouse', 'Objector',
                      'Other Professional', 'Partner', 'Petitioning Creditor', 'Realtor', 'Receiver',
                      'Special Counsel', 'Special Master', 'Stockholder', 'Successor Trustee', 'Surveyor',
                      'Taxpayer', 'Third Party Defendant', 'Third Party Plaintiff', 'U.S. Trustee', 'Witness ',
                      'Consolidated Third Party Defendant', 'Fourth Party Defendant', 'Fourth Party Plaintiff',
                      'Interested Non-Party', 'Notice Party', 'Unknown', 'Surety', 'Interpreter', 'Estate Plaintiff',
                      'Estate Defendant', 'Relief Defendant', 'Executor Defendant', 'Executor Plaintiff', 'Estate of',
                      'Mediation Assistance Program Counsel', 'Recruited Counsel', 'Relator', 'Deponent', 'Proposed',
                      'surety','deponent','Court Monitor','Debtor in Possession','proposed','estate of','executor plaintiff',
                      'executor defendant','Consol Defendant','Notice Only Party','Notice','Consol Plaintiff','unknown',
                      'Consol Counter Defendant','HABEAS ATTORNEY GENERAL SERVICE LIST','Mediator (ADR Panel)','In re Debtor',
                      'In Re Debtor',"Real Party In Interest Defendant",'Witness',"Counter-claimant","Counter-defendant",
                      "Intervenor Dft","Miscellaneous","Cross-defendant","Cross-claimant","3rd party plaintiff","3rd party defendant",
                      "Neutral","Guardian Ad Litem Party","Requesting Party","Objecting Party","Special Mediation Counsel"
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
        for ordinal_number, row in enumerate(table.find_all('tr')):
            full_row = {"date":'',"number":'',"ordinal_number":'',"links":{},"text":''}
            for column in row.find_all('td'):
                if first:
                    full_row["text"] = row.get_text().strip()
                    first = False
                    break
                else:
                    if "/" in column.get_text() and column.get_text().replace("/","").isnumeric():
                        full_row["date"] = column.get_text()
                    elif len(column.get_text()) < 6:
                        full_row["number"] = column.get_text().strip()
                    else:
                        full_row["text"] = column.get_text().strip()
            the_links = [[str(a['href']),a.get_text()] for a in row.find_all('a') if a.has_attr('href')]
            for link in the_links:
                full_row["links"][link[0]] = link[1]
            full_row["ordinal_number"] = ordinal_number
            self.parsed_docket["docket_text"]["rows"].append(full_row)

    def parse_docket_footers(self, table):
        table_text = table.get_text()
        if "agistrate" not in table_text and ' ' not in table_text:
            self.parsed_docket["docket_flags"] += table_text.replace("\n","")
        elif "Docket Text" in table_text:
            self.parse_docket_text(table, table_text)
        elif "PACER" in table_text:
            self.parsed_docket["pacer_receipt"] += table_text
        elif "Date Filed" in table_text:
            self.parse_header(table_text)
        else:
            self.parse_header(table_text)

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
        return ' '.join([line for line in underline.get_text().strip().split() if "(" not in line and ")" not in line])

    def update_role_list(self):
        searchable_area = str(self.soup).partition("Docket Text")[0]
        searchable_soup = BeautifulSoup(searchable_area, 'html.parser')
        for underline in searchable_soup.find_all('u'):
            underline = self.clean_underline(underline)
            if underline in self.other_known_underlines:
                continue
            elif underline in self.roles:
                continue
            else:
                print('Hey! Listen! File {c} contains a role not yet seen: "{r}"'.format(c=self.case_id, r=underline,))
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
                else:
                    if "Docket Text" in table_text:
                        self.parse_docket_text(table, table_text)
                    else:
                        self.parse_roles(table, table_topic, roles)

    def update_addl_field_list(self, line, name, value, loc="Header", key="addl_docket_fields", rolekey=None):
        if key == "addl_docket_fields":
            check = {"field_name_attempt":name,
                     "field_value_attempt":value,
                     "found_in_section":loc}
            if check not in self.parsed_docket[key]:
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
                case_type, colon, docket_case_id = line.partition(":")
                self.parsed_docket["docket_case_id"] = docket_case_id.strip().split()[0]
                if "civil" in case_type.lower():
                    self.update_addl_field_list(line, "case_type", "Civil")
                elif "criminal" in case_type.lower():
                    self.update_addl_field_list(line, "case_type", "Criminal")
                else:
                    self.update_addl_field_list(line, "case_type", case_type)
            elif "docket" in line.lower() and "case" in line.lower() and "#" in line:
                case_type, colon, docket_case_id = line.partition(":")
                self.parsed_docket["docket_case_id"] = docket_case_id.strip().split()[0]
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

    def overload_fields(self, existing, new):
        if not existing:
            return new
        elif isinstance(existing, list):
            return existing + [new]
        else:
            return [existing, new]

    def parse_contact_blob(self, role, key, num, non_reps=False, formatted=None):
        other_fields = []
        unsure = 0
        fields = {'org':'',
                  'purview':'',
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
                     'l.l.c','Firm','Ltd','LTD', 'Dept','Department','LLp','Llp',
                     'LTd','ltd',' llp']
        postal_words = ['P.O.','PO ','Box','BOX','St.','Street','Blvd','Hwy','Jnc',
                        'Boulevard','Dr.','Drive','Ave','Highway','Junction',
                        'Rd','Road','Circle','Place','Ridge','Terrace','Sq.',
                        'Square','Lane','Ln','Crescent','Alley','Bvd','Court',
                        'Expressway','Freeway','Jct','Parkway', 'North', 'South',
                        'East', 'West', 'Nw', 'Sw','Pier',]
        second_address = ['Suite','Ste.','Floor','Room','Unit','Apt ','Apt.','Apartment']
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
            try:
                lines = self.parsed_docket[role][key]["reps"][num].pop("blob")
            except(KeyError):
                lines = []
            try:
                italics = self.parsed_docket[role][key]["reps"][num].pop("fields")
            except(KeyError):
                italics = []
        else:
            lines = [non_reps]
            italics = formatted
        for line in lines:
            tokens = line.split()
            if not line:
                continue
            elif line.strip() == '.' or line.lower() == "v.":
                continue
            elif line.strip() == '' or line.strip == ' ':
                continue
            elif not non_reps and ' '.join(line.split()) == self.parsed_docket[role][key]["reps"][num]["name_attempt"]:
                continue
            elif 'by' in line.strip()[0:2]:
                continue
            elif "@" in line and "." in line:
                premail, colon, email = line.partition(":")
                fields["email"] = self.overload_fields(fields["email"], email)
            elif len(tokens) > 1 and tokens[-2] in states:
                fields["state"] = self.overload_fields(fields["state"], tokens[-2])  #tokens[-2]
                if len(tokens) > 3:
                    fields["city"] = self.overload_fields(fields["city"], ' '.join(tokens[0:-2]).replace(",","")) #' '.join(tokens[0:-2]).replace(",","")
                else:
                    fields["city"] = self.overload_fields(fields["city"], tokens[0].replace(",",""))
                fields["zip"] = self.overload_fields(fields["zip"], tokens[-1])
            elif len(tokens) > 2 and tokens[-1].isnumeric() and tokens[-2].upper() in states:
                fields["state"] = self.overload_fields(fields["state"], tokens[-2])
                if len(tokens) > 3:
                    fields["city"] = self.overload_fields(fields["city"], ' '.join(tokens[0:-2]).replace(",",""))
                else:
                    fields["city"] = self.overload_fields(fields["city"], tokens[0].replace(",",""))
                fields["zip"] = self.overload_fields(fields["zip"], tokens[-1])
            elif 'district of' in line.lower():
                fields["purview"] = self.overload_fields(fields["purview"], line)
            elif any(word in line for word in postal_words):
                if not fields["postal_address"][0]:
                    fields["postal_address"][0] = line
                else:
                    fields["postal_address"].insert(1, line)
            elif any(word in line.title() for word in postal_words):
                if not fields["postal_address"][0]:
                    fields["postal_address"][0] = line
                else:
                    fields["postal_address"].insert(1, line)
            elif any(word in line.title() for word in second_address):
                if not fields["postal_address"][1]:
                    fields["postal_address"][1] = line
                else:
                    fields["postal_address"].append(line)
            elif any(word in line for word in org_words):
                fields["org"] = self.overload_fields(fields["org"], line)
            elif any(word in line.title() for word in building_words):
                fields["building"] = self.overload_fields(fields["building"], line)
            elif len(tokens) > 1 and tokens[0].isnumeric() and tokens[1].isalpha():
                fields["postal_address"][0] = line
            elif len(tokens) > 2 and tokens[0].isnumeric() and tokens[2].isalpha():
                fields["postal_address"][0] = line
            elif self.clean_number(line).isnumeric():
                if "(" in line and ")" in line:
                    fields["phone"] = self.overload_fields(fields["phone"], self.clean_number(line))
                elif len(self.clean_number(line,' ').split()) == 1:
                    fields["id_number"] = self.overload_fields(fields["id_number"], line)
                elif len(self.clean_number(line,' ').split()[-1]) == 4 and len(self.clean_number(line,' ').split()[-2]) == 3:
                    fields["phone"] = self.overload_fields(fields["phone"], self.clean_number(line))
                elif len(self.clean_number(line)) == 10 or len(self.clean_number(line)) == 7:
                    fields["phone"] = self.overload_fields(fields["phone"], self.clean_number(line))
                else:
                    fields["id_number"] = self.overload_fields(fields["id_number"], line)
            elif "Fax" in line.title():
                fax = line.partition(":")[2]
                fields["fax"] = self.overload_fields(fields["fax"], self.clean_number(fax))
            elif line in italics:
                if "lead" in line.lower():
                    fields["lead"] = self.overload_fields(fields["lead"], line)
                elif "designation" in line.lower():
                    designation = line.partition(":")[2]
                    fields["designation"].append(designation)
                elif "terminated" in line.lower():
                    date = line.partition(":")[2]
                    fields["date_terminated"] = self.overload_fields(fields["date_terminated"], date)
                elif "notice" in line.lower():
                    fields["to_be_noticed"] = self.overload_fields(fields["to_be_noticed"], line)
                else:
                    fields["other_italics"].append(line)
            elif "pro se" in line.lower():
                fields["pro_se"] = self.overload_fields(fields["pro_se"], line)
            elif any(word in line.title() for word in self.roles):
                fields["title"] = self.overload_fields(fields["title"], line)
            elif any(word in line.title() for word in title_words):
                fields["title"] = self.overload_fields(fields["title"], line)
            elif "district" in line.lower():
                fields["district"] = self.overload_fields(fields["district"], line)
            elif line[-6:].isnumeric():
                fields["id_number"] = self.overload_fields(fields["id_number"], line)
            else:
                if "(" in line and ")" in line and "-" in line:
                    fields["phone"] = self.overload_fields(fields["phone"], line)
                elif len(line) > 12 and line[3] == '-' and line[0:3].isnumeric() and line[4:7].isnumeric() and line[7] == '-' and line[8:12].isnumeric():
                    fields["phone"] = self.overload_fields(fields["phone"], line)
                else:
                    label = "unsure_"+str(unsure)
                    fields[label] = line
                    unsure += 1
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

    def refine_role_blobs(self, role, key, name):
        addl_fields = []
        try:
            lines = self.parsed_docket[role][key].pop("blob")
        except(KeyError):
            lines = []
        try:
            italics = self.parsed_docket[role][key].pop("italics")
        except(KeyError):
            italics = []
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
        try:
            self.parsed_docket[role][key]["reps"].pop("total_blob")
        except(KeyError):
            pass
        for num in self.parsed_docket[role][key]["reps"].keys():
            self.parse_contact_blob(role, key, num)

    def refine_blob_parsing(self):
        for role in self.roles:
            if self.parsed_docket[role.lower()]:
                for num in self.parsed_docket[role.lower()].keys():
                    self.refine_role_blobs(role.lower(), num, self.parsed_docket[role.lower()][num]["name_attempt"])

    def main(self):
        try:
            self.open_docket()
            self.update_role_list()
            self.parse_docket_header()
            self.parse_docket()
            self.refine_header_parsing()
            self.refine_blob_parsing()
            if self.parsed_docket["missed_parses"]:
                print("Hey! Listen! We failed to parse {c} lines in file {f}".format(c=len(self.parsed_docket["missed_parses"]), f=self.case_id))
        except Exception as e:
            print('Hey! Listen! File {p} failed with exception "{f}"'.format(p=os.path.basename(self.docket), f=e))
            self.parsed_docket["FAILURE"] = True


if __name__ == "__main__":
    CorpusParser(sys.argv[1])
