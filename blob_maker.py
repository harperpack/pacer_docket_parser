#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 16 14:41:11 2020

@author: harper
"""


import requests
from bs4 import BeautifulSoup

class Extract:
    
    def __init__(self, soup, parse_type=None):
        self.soup = soup
        self.text = soup.get_text()
        self.defendant_blob = {} # defendant blob : reps: rep_blobs, : judges: judge_blobs
        self.plaintiff_blob
    
    def successive_blobs(self):
        for line in self.text.splitlines():
            if len(line) < 1:
                continue
            tokens = line.split()
            if "assigned to" in line.lower():
                judge_blob = line
                continue
            if tokens[0].lower() == "defendant":
                def_blob = self.text.partition(line)[2].partition("represented by")[0]
                rep_blob = self.text.partition(def_blob)[2]
    
    def try_defendant_blobs(self):
        start = "<b><u>" # start defendant, plaintiff, petitioner, respondent
        end = ">represented" # start representation
        end2 = "<b><u>" # start charges
        end3 = "</table>" # end defendant, plaintiff, petitioner, respondent
        
    def defendant_blob(self):
        blob = self.text.partition("")
    

path = '/Users/harper/Documents/nu_work/noacri/data/1-06-cr-00887.html'

def open_one(path):
    with open(path, "r") as f:
        docket_html = f.read()
    docket_text_blob = ''
    case_data_blob = ''
    defendant_blobs = {} 
    plaintiff_blobs = {}
    petitioner_blobs = {}
    respondent_blobs = {}
    case_id = path.partition("data/")[2].partition(".html")[0]
    case = {}
    soup = BeautifulSoup(docket_html, 'html.parser')
    lll = soup.find_all('h3')
    for l in lll:
        print(l.get_text())
    tables = soup.find_all("table")
    count = 0
    for table in tables:
        table_text = table.get_text()
        table_label = table.find('b')
        if table_label:
            for element in table_label.descendants:
                print(element)
            #print(table_label.get_text())
        continue
        if "Docket Text" in table_text:
            docket_text_blob = table_text
        elif "Defendant" in table_text and "represented" in table_text.lower():
            if "assigned to" in table_text.lower():
                judge = table_text.partition("to:")[2].partition("Defendant")[0]
            else:
                judge = ''
            defendant = table_text.partition("Defendant")[2].partition("represented")[0]
            if " Counts" in table_text:
                reps = table_text.partition(defendant)[2].partition(" Counts")[0]
                charges = table_text.partition(" Counts")[2]
            else:
                reps = table_text.partition(defendant)[2]
                charges = ''
            defendant_number = len(defendant_blobs.keys())
            defendant_blobs[defendant_number] = {"defendant":defendant,"reps":reps,"judge":judge,"charges":charges}
        elif "Plaintiff" in table_text:
            if "represented" in table_text:
                plaintiff = table_text.partition("Plaintiff")[2].partition("represented")[0]
                reps = table_text.partition(plaintiff)[2]
            else:
                plaintiff = table_text.partition("Plaintiff")[2]
                reps = ''
            plaintiff_number = len(plaintiff_blobs.keys())
            plaintiff_blobs[plaintiff_number] = {"plaintiff":plaintiff,"reps":reps,}
        elif "Date Filed" in table_text:
            case_data_blob = table_text
        elif "Petitioner" in table_text:
            if "represented" in table_text:
                petitioner = table_text.partition("Petitioner")[2].partition("represented")[0]
                reps = table_text.partition(petitioner)[2]
            else:
                petitioner = table_text.partition("Petitioner")[2]
                reps = ''
            petitioner_number = len(petitioner_blobs.keys())
            petitioner_blobs[petitioner_number] = {"petitioner":petitioner,"reps":reps,}
        elif "Respondent" in table_text:
            respondent_number = len(respondent_blobs.keys())
            respondent_blobs[respondent_number] = {"respondent":table_text}
        else:
            print(table_text)
    case[case_id] = {"case_data":case_data_blob,
                    "docket_text":docket_text_blob,
                    "defendant":defendant_blobs,
                    "plaintiffs":plaintiff_blobs,
                    "petitioner":petitioner_blobs,
                    "respondent":respondent_blobs}
    print(case)
#        print("======= TABLE " + str(count) + " =======")
#        print(table.get_text())
#        print("--------------")
#        print("\n")
#        count += 1
#    title_blob
#    text = soup.get_text()
#    count = 0
#    lines = text.split("\n")
#    for line in text.splitlines():
#        #print(len(line))
#        print(line)
#        count += 1
#        if count > 40:
#            break
    #print(soup.get_text())

open_one(path)

## num defendants + threshold of defendant blob
# <b><u>Defendant
# (1)</u></b>

## defendant name
#<tr>
#				<td valign="top" width="40%">
#					<b>
# </b>

## representative name
# represented
# <b>
# </b>
