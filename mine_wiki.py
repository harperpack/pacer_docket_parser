#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 28 15:55:55 2020

@author: harper
"""

from bs4 import BeautifulSoup
import requests
import json

class Table:
    
    def __init__(self, name, root):
        self.name = name
        self.root = root
        self.state_abbrevs = {
                        'Alabama': 'AL',
                        'Alaska': 'AK',
                        'Arizona': 'AZ',
                        'Arkansas': 'AR',
                        'California': 'CA',
                        'Colorado': 'CO',
                        'Columbia': 'DC',
                        'Connecticut': 'CT',
                        'Delaware': 'DE',
                        'District of Columbia': 'DC',
                        'D.C.': 'DC',
                        'Florida': 'FL',
                        'Georgia': 'GA',
                        'Guam': 'GU',
                        'Hawaii': 'HI',
                        'Idaho': 'ID',
                        'Illinois': 'IL',
                        'Indiana': 'IN',
                        'Iowa': 'IA',
                        'Kansas': 'KS',
                        'Kentucky': 'KY',
                        'Louisiana': 'LA',
                        'Maine': 'ME',
                        'Maryland': 'MD',
                        'Massachusetts': 'MA',
                        'Michigan': 'MI',
                        'Minnesota': 'MN',
                        'Mississippi': 'MS',
                        'Missouri': 'MO',
                        'Montana': 'MT',
                        'Nebraska': 'NE',
                        'Nevada': 'NV',
                        'New Hampshire': 'NH',
                        'New Jersey': 'NJ',
                        'New Mexico': 'NM',
                        'New York': 'NY',
                        'North Carolina': 'NC',
                        'North Dakota': 'ND',
                        'Northern Mariana Islands':'MP',
                        'Ohio': 'OH',
                        'Oklahoma': 'OK',
                        'Oregon': 'OR',
                        'Palau': 'PW',
                        'Pennsylvania': 'PA',
                        'Puerto Rico': 'PR',
                        'Rhode Island': 'RI',
                        'South Carolina': 'SC',
                        'South Dakota': 'SD',
                        'Tennessee': 'TN',
                        'Texas': 'TX',
                        'Utah': 'UT',
                        'Vermont': 'VT',
                        'Virgin Islands': 'VI',
                        'Virginia': 'VA',
                        'Washington': 'WA',
                        'West Virginia': 'WV',
                        'Wisconsin': 'WI',
                        'Wyoming': 'WY',
                        }
        self.rows = []
        self.row = {}
    
    def clear_row(self):
        self.row = {}
    
    def add_entry(self, key, value):
        self.row[key] = str(value)
    
    def append(self):
        self.rows.append(self.row)
        self.clear_row()
    
    def complete_row(self, soup, label):
        if label == "circuit":
            self.add_entry(label,soup["title"])
        else:
            self.add_entry(label,soup.get_text())
        table_page = requests.get(self.root+soup["href"])
        table_soup = BeautifulSoup(table_page.text, 'html.parser')
        infobox = table_soup.find("table", {'class':['infobox infobox vcard']})
        self.add_entry("full_name", infobox.find("th").get_text())
        self.add_entry("abbreviation", infobox.find("td").get_text().replace("(",'').replace(")",''))
        if label == "circuit":
            post = infobox.find("div").find("a").get_text().split(',')
            self.add_entry("city",post[0])
            try:
                self.add_entry("state",self.state_abbrevs[post[1].strip()])
            except(IndexError):
                self.add_entry("state","NY")
        else:
            state = self.row["district"].partition("of ")[2].replace("the",'').strip()
            self.add_entry("state",self.state_abbrevs[state])
            for a in infobox.find_all("a"):
                test = a.get_text()
                if test and "Court" not in test and "Federal" not in test:
                    self.add_entry("city",test)
                    break
            for a in infobox.find_all("a"):
                if a.has_attr("title") and "Circuit" in a["title"]:
                    self.add_entry("circuit_court",a.get_text())
        self.append()

class DistrictAndCircuitScraper:
    
    def __init__(self, root, path, output_dir):
        self.root = root
        self.path = path
        self.districts = Table("DistrictCourts", self.root)
        self.circuits = Table("CircuitCourts", self.root)
        self.appl_flag = "United States Court of Appeals"
        self.dist_flag = "United States District"
        self.soup = BeautifulSoup(requests.get(self.path).text, 'html.parser')
        self.output_dir = output_dir
        self.main()
    
    def scrape(self):
        for table in self.soup.find_all("table"):
            for column in table.find_all("p"):
                for link in column.find_all('a'):
                    if link.has_attr("title") and self.appl_flag in link["title"]:
                        self.circuits.complete_row(link,"circuit")
            for unordered_list in table.find_all("ul"):
                for li in unordered_list.find_all("li"):
                    link = li.find("a")
                    if link != None and link.has_attr("title") and self.dist_flag in link["title"]:
                        self.districts.complete_row(link,"district")
    
    def output(self):
#        for row in self.circuits.rows:
#            for key in row.keys():
#                print(key+"\t"+row[key])
#            print("\n")
#        print("-----")
#        print("=====")
#        print("-----")
        for row in self.districts.rows:
            if "city" not in row.keys():
                for key in row.keys():
                    print(key+"\t"+row[key])
                print("\n")
            elif "Court" in row["city"] or "Building" in row["city"]:
                for key in row.keys():
                    print(key+"\t"+row[key])
                print("\n")
    
    def save(self):
        for output in ["districts.json", "circuits.json"]:
            if output == "districts.json":
                data = {"rows":self.districts.rows}
            else:
                data = {"rows":self.circuits.rows}
            filename = self.output_dir + output
            with open(filename,"w") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
    
    def main(self):
        self.scrape()
        self.save()
#        self.output()

DistrictAndCircuitScraper("https://en.wikipedia.org", 
                          "https://en.wikipedia.org/wiki/United_States_courts_of_appeals",
                          "/Users/harper/Documents/nu_work/nsf/noacri/code/docket_parsing/")
