#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 15 13:19:55 2020

@author: harper
"""

import requests
from bs4 import BeautifulSoup

class Extract:
    
    def __init__(self, soup, parse_type=None):
        self.defendants = {"names":
                            {"template":
                                {"reps":
                                    {"template":
                                        {"name":'',
                                         "office":'',
                                         "street":'',
                                         "suite":'',
                                         "city":'',
                                         "state":'',
                                         "zip":'',
                                         "phone":'',
                                         "fax":'',
                                         "email":'',
                                         "lead":'', # boolean
                                         "noticed":'', # boolean
                                         "designation":'',
                                         "terminated_date":'',
                                         },
                                    },
                                },
                            },
                        }
        self.judge = {"names":
                        {"template":
                            {"name":'',
                             "assigned_to":'', # boolean
                             "docket_numbers":[]
                             },
                        },
                    }
        self.plaintiffs = {"names":
                            {"template":
                                {"reps":
                                    {"template":
                                        {"name":'',
                                         "office":'',
                                         "street":'',
                                         "suite":'',
                                         "city":'',
                                         "state":'',
                                         "zip":'',
                                         "phone":'',
                                         "fax":'',
                                         "email":'',
                                         "lead":'', # boolean
                                         "noticed":'', # boolean
                                         "designation":'',
                                         "terminated_date":'',
                                         },
                                    },
                                },
                            },
                        }
        self.docket = {"title":'',
                       "case_id":'',
                       "suffix":'',
                       "case_type":'',
                       "court_type":'',
                       "purview":'',
                       "date_filed":'',
                       "date_terminated":'',
                       "flag_text":'',
                       "city":'',
                       "state":''}
        self.soup = soup
        self.text = soup.get_text()
        self.knowledge = {"districts":
                            ["Western","Eastern","Northern","Southern","Central","Middle"],}
        if parse_type == "text":
            self.text_parse()
        
    def parse_soup(self):
        pass
    
    def text_parse(self):
        line_count = 0
        scope = "head"
        last_nonblank_line = ''
        for line in self.text.splitlines():
            if len(line) < 1:
                continue
            tokens = line.split()
            if scope == "head":
                if "QueryReportsUtilitiesLogout" in line:
                    last_nonblank_line = line
                    continue
                if line_count < 20 and line.isupper():
                    self.docket.flag_text = line
                    last_nonblank_line = line
                    continue
                if "district of" in line.lower():
                    self.docket.purview = line.partition("-")[0]
                    self.docket.state = tokens.index(tokens.index("of")+1)
                    self.docket.court_type = "District"
                    self.docket.city = tokens[-1].replace("(",'').replace(")",'')
                    last_nonblank_line = line
                    continue
                if "circuit" in line.lower():
                    self.docket.court_type = "Circuit"
                    last_nonblank_line = line
                    continue
                    # MOAR?
                if "docket for case" in line.lower():
                    if "docket" != tokens[0].lower():
                        self.docket.case_type = tokens[0].title()
                    self.docket.case_id = [x for x in tokens if not x.isalpha() and len(x) > 5][0]
                    last_nonblank_line = line
                    continue
                if "case title" in line.lower():
                    self.docket.title = line.partition("itle:")[2]
                    last_nonblank_line = line
                    continue
                if "date filed" in line.lower():
                    self.docket.date_filed = line.partition("iled:")[2]
                    last_nonblank_line = line
                    continue
                if "date terminated" in line.lower():
                    self.docket.date_terminated = line.partition("inated:")[2]
                    last_nonblank_line = line
                    continue
                if "assigned to" in line.lower():
                    j = line.partition("to:")[2]
                    self.judge.names[j] = {"name":j,"assigned_to":True,"docket_numbers":[0]}
                    last_nonblank_line = line
                    continue
                if tokens[0].lower() == "defendant":
                    scope = "defendant"
                    last_nonblank_line = line
                    continue
            elif scope == "defendant":
                if line.istitle() and last_nonblank_line.split()[0].lower == "defendant":
                    self.defendants[line] = {"name":line,
                                             "terminated":'',
                                             "charges":{},
                                             "reps":{}}
                    defendant = line
                    last_nonblank_line = line
                    continue
                if "terminated" == tokens[0].lower():
                    self.defendants[defendant].terminated = line.lower().partition("terminated:")[2]
                    last_nonblank_line = line
                    continue
                if "represented by" in line:
                    check_name = line.lower().partition("represented by")[2].split()
                    if len(check_name) > 1:
                        rep_name = ' '.join(check_name)
                        self.defendants[defendant].reps[rep_name] = {"name":rep_name,
                                                                     "office":'',
                                                                     "street":'',
                                                                     "suite":'',
                                                                     "city":'',
                                                                     "state":'',
                                                                     "zip":'',
                                                                     "phone":'',
                                                                     "fax":'',
                                                                     "email":'',
                                                                     "lead":'', # boolean
                                                                     "noticed":'', # boolean
                                                                     "designation":'',
                                                                     "terminated_date":'',
                                                                     }
                    
                
#            if tokens[0] == "United" or tokens[0] == "U.S." or tokens[0] == "US":
#                ind = tokens.index("Court")
#                try:
#                    self.docket.court_type = tokens[ind-1]
#                except(IndexError):
#                    self.docket.court_type = line
#                continue
            if "docket for case" in line.lower():
                if tokens[0].lower() != "docket":
                    self.docket.case_type = tokens[0]
                
            #if len(line) < 
            #elif tokens[0] == "United" or tokens[0] == "U.S." or tokens[0] == "US":
            #self.docket.
            line += 1

path = '/Users/harper/Documents/nu_work/noacri/data/1-06-cr-00887.html'

def open_one(path):
    with open(path, "r") as f:
        docket_html = f.read()
    soup = BeautifulSoup(docket_html, 'html.parser')
#    title_blob
    text = soup.get_text()
    count = 0
#    lines = text.split("\n")
    for line in text.splitlines():
        #print(len(line))
        print(line)
        count += 1
        if count > 40:
            break
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
