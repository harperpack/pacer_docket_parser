# TO DO:
## add recusal parser
## add waiver parser

## CHARGE DEFICIENCIES:
### not catching prison without prison words ("dft sentenced" example)
### not catching sentences which mention something to say it does NOT exist (e.g., with no sr example)
### not capturing 'to be determined'
### not capturing sentences which list the smallest charge last (concurrent starting with total example)
### capture CRIMINAL MONETARY PENALTIES?
### capture STANDARD CONDITIONS OF SUPERVISION?
### capture sentences which do not use numbers?
##
## PERSISTENT FAILURES
## 4-16-cr-00278-GRS
##

## some details:
### - initial parser should bark if issues, but may throw a lot into "unsure":
##### > will miss unexpected (anywhere outside of docket entries) hyperlinks
##### > will parse poorly on dockets without:
######## >> a structured header,
######## >> a body with parties and optionally reps and optionally charges, where
######## >>     parties have maybe a line beneath  names, reps have address blobs,
######## >>     and charges have dispositions, and
######## >> a structured footer (i.e., docket entries table)
### - secondary parser is also imperfect:
##### > charge parsing is imperfect
##### > judges not de-duped; may be info e.g., assigned_to only mentioned once then lost in later deduping
##### > judges may be duped in highly-similar fashion (e.g., Murray Chun, Murray L Chun, etc.)
##### > attorney addresses can be listed as "ADDRESS EXISTS ALREADY" or something
##### > skips any attorney or party listed as 'Court Use Only' or 'Internal Use Only' or 'Service List' or 'Probation Department' or 'Pretrial Services'
##### > title/capacity parsing can leak information, but the assumption is that
##### >     this is immaterial unless the docket is poorly parsed (see above)
##### >     TODO re: failing dockets with too many potential title candidates (> 3?)

### REMOVE PRINTS, NO_FIND_ RETURN VALS
### CAPTURING SEALED AND UNSEALED AS SEPARATE MOTIONS, SEPARATELY

## questionable:
### address normalization
### org disambiguation
### entity disambiguation

import json
import time
from datetime import date, datetime
import os.path
from pathlib import Path
import spacy
import statistics
from fuzzywuzzy import fuzz
from bs4 import BeautifulSoup
import traceback
import sys

class DocketTables:

    def __init__(self, in_dir='./parsed_dockets/pipeline', state=''):
        self.in_dir = in_dir #if in_dir else "./parsed_dockets/test/"
        self.docket_tables = {}
        self.failure_tables = {}
        self.shared_tables = {"misses":{"failures":[],}}
        self.case_ids = []
        self.miss_count = []
        self.json_queue = []
        self.total = 0
        self.default_state = state
        self.main()

    def build_json_queue(self):
        if os.path.isfile(self.in_dir):
            self.json_queue.append(self.in_dir)
        elif os.path.isdir(self.in_dir):
            for (dirpath, dirnames, filenames) in os.walk(self.in_dir):
                for filename in filenames:
                    if filename.endswith('.json'):
                        full_filename = str(os.path.join(dirpath, filename))
                        if 'parselist' not in full_filename:
                            self.json_queue.append(os.path.join(dirpath, filename))

    def load_json(self, json_file):
        with open(json_file, "r") as f:
            self.parsed = json.load(f)

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

    def save_latest(self, filename):
        filename_success = filename.replace('pipeline','noacri_dockets').replace('.json','_PARSED.json')
        with open(filename_success, "w", encoding="utf-8") as f:
            json.dump(self.docket_tables, f, ensure_ascii=False, indent=4)
        filename_failure = filename.replace('pipeline','failures').replace('.json','_FAILURE.json')
        with open(filename_failure, "w", encoding="utf-8") as f:
            json.dump(self.failure_tables, f, ensure_ascii=False, indent=4)

    def save_docket(self, filename, save_type, save_table):
        if save_type == "success":
            filename = filename.replace('pipeline','noacri_dockets').replace('.json','_PARSED.json')
        else:
            filename = filename.replace('pipeline','failures').replace('.json','_FAILURE.json')
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(save_table, f, ensure_ascii=False, indent=4)

    def save_final(self):
        miss_filename = self.in_dir.replace('pipeline','failures') + str(os.path.sep) + "__finalMISS.json"
        with open(miss_filename, "w", encoding="utf-8") as f:
            json.dump(self.shared_tables["misses"], f, ensure_ascii=False, indent=4)
        if self.shared_tables["cities"]:
            filename_city = "./table_json/cities.json"
            with open(filename_city, "w", encoding="utf-8") as f:
                json.dump(self.shared_tables["cities"], f, ensure_ascii=False, indent=4)
        if self.shared_tables["charges"]:
            filename_charge = "./table_json/charges.json"
            with open(filename_charge, "w", encoding="utf-8") as f:
                json.dump(self.shared_tables["charges"], f, ensure_ascii=False, indent=4)

    def populate_nos(self):
        with open("./table_json/nos.json", "r", encoding="utf-8") as f:
            self.shared_tables["nos"] = json.load(f)

    def populate_circuit(self):
        with open("./table_json/circuits.json", "r", encoding="utf-8") as f:
            circuit = json.load(f)
            self.shared_tables["circ"] = circuit["rows"]

    def populate_district(self):
        with open("./table_json/districts.json", "r", encoding="utf-8") as f:
            dist = json.load(f)
            self.shared_tables["dist"] = dist["rows"]

    def populate_roles(self):
        with open("./table_json/roles.json", "r", encoding="utf-8") as f:
            self.shared_tables["roles"] = json.load(f)

    def populate_designations(self):
        with open("./table_json/designations.json", "r", encoding="utf-8") as f:
            self.shared_tables["des"] = json.load(f)

    def populate_charges(self):
        with open("./table_json/charges.json", "r", encoding="utf-8") as f:
            self.shared_tables["charges"] = json.load(f)

    def populate_cities(self):
        with open("./table_json/cities.json", "r", encoding="utf-8") as f:
            self.shared_tables["cities"] = json.load(f)

    def build_cities(self):
        if "cities" not in self.shared_tables.keys():
            self.shared_tables["cities"] = {}
        for court in self.shared_tables["dist"]:
            combined = court["city"]+court["state"]
            if combined not in self.shared_tables["cities"].keys():
                self.shared_tables["cities"][combined] = {"city":court["city"],"state":court["state"]}
        for court in self.shared_tables["circ"]:
            combined = court["city"]+court["state"]
            if combined not in self.shared_tables["cities"].keys():
                self.shared_tables["cities"][combined] = {"city":court["city"],"state":court["state"]}

    def load(self):
        self.populate_roles()
        self.populate_nos()
        self.populate_circuit()
        self.populate_district()
        self.populate_designations()
        self.populate_charges()
        self.populate_cities()

    def ensure_output_dirs(self):
        failure_dir = self.in_dir.replace('pipeline','failures')
        success_dir = self.in_dir.replace('pipeline','noacri_dockets')
        for dir_to_make in [failure_dir,success_dir]:
            if not os.path.exists(dir_to_make):
                os.makedirs(dir_to_make)

    def main(self):
        start = time.time()
        self.load()
        nlp = spacy.load("en_core_web_sm")
        self.build_json_queue()
        load_time = round(time.time() - start,2)
        length = len(self.json_queue)
        print("Successfully loaded {c} JSONs in {t} seconds.".format(c=len(self.json_queue),t=load_time))
        self.ensure_output_dirs()
        before_parse = time.time()
        try:
            for json_count, json_file in enumerate(self.json_queue,start=1):
                start_parse = time.time()
                try:
                    self.load_json(json_file)
                    local_misses = []
                    for case_count, case in enumerate(self.parsed,start=1):
                        docket = DocketTable(case,self.parsed[case],self.shared_tables,nlp,self.in_dir,self.default_state)
                        self.shared_tables["cities"] = docket.cities
                        self.shared_tables["des"] = docket.des
                        self.shared_tables["charges"] = docket.charges
                        add_misses = False
                        if docket.missed_parses:
                            for miss in docket.missed_parses:
                                if miss:
                                    add_misses = True
                                    break
                        save_table = {}
                        save_type = "success"
                        if add_misses:
                            self.shared_tables["misses"][case] = docket.missed_parses
                            local_misses.append(docket.miss_count)
                            save_table["d"] = docket.tables
                            save_table["moises"] = docket.missed_parses
                            save_type = "failure"
                        else:
                            save_table = docket.tables
                        self.save_docket(json_file, save_type, save_table)
                        self.total += 1
                    mn = 0
                    md = 0
                    if local_misses:
                        mn = round(statistics.mean(local_misses),2)
                        md = round(statistics.median(local_misses),2)
                        self.miss_count += local_misses
                    latest_time = round(time.time() - start_parse,2)
                except Exception as e:
                    print('...parsing of {j} failed with exception "{f}"\n'.format(j=json_file[json_file.rfind('/')+1:],f=e))
                    traceback.print_exc()
                    self.shared_tables["misses"]["failures"].append(json_file)
                self.progress(json_count, length)
        except Exception as e:
            print('...\n...entire operation failed with exception "{f}"'.format(f=e))
        self.save_final()
        mn = ' na '
        md = ' na '
        if self.miss_count:
            mn = round(statistics.mean(self.miss_count),2)
            md = round(statistics.median(self.miss_count),2)
        done_time = round(time.time() - before_parse,2)
        rate = 0
        incidence = ' na '
        if self.total:
            rate = round(self.total/done_time,2)
            incidence = round(sum(self.miss_count)/self.total,2)
        print("\n----------")
        print("Completed parsing.")
        try:
            print("{c} dockets, {q} misses, {j} JSONs in {t} seconds.".format(c=self.total,q=len(self.miss_count),j=json_count,t=done_time))
            print("Final statistics:\t{t}dckt/s\t{m}miss/dckt\tmedian: {d}\tmean: {n}".format(t=rate,m=incidence,d=md,n=mn))
        except Exception as e:
            print('...\n...printing, too, fails: "{f}"'.format(f=e))


class DocketTable:

    def __init__(self, case_id, parsed_case, shared_tables, nlp, in_dir, default_state=''):
        self.case_id = case_id
        self.parsed = parsed_case
        self.new_case_id = self.case_id[:self.case_id.rfind('_')] + "|||" + self.case_id_val()
        self.parse_later = {}
        self.skip_list = ['Use Only','Pretrial Services','Probation Department','Service List',]
        self.missed_parses = []
        self.nos = shared_tables["nos"]
        self.roles = shared_tables["roles"]
        self.cities = shared_tables["cities"]
        self.dist = shared_tables["dist"]
        self.circ = shared_tables["circ"]
        self.des = shared_tables["des"]
        self.charges = shared_tables["charges"]
        self.tables = { # listed in fill order
        # NO FOREIGN KEYS, NO RELATIONSHIPS
        # "Charges":{}, # no fkey
        # "Designations":{}, # no fkey
        # "Roles":{}, # no fkey
        # ASSEMBLED VIA SCRAPE
        # "Cities":{}, # no fkey
        # "CircuitCourt":{}, # fkey to Cities; REL to Case, Cities, DistrictCourt,
        # "DistrictCourt":{}, # fkeys to CircuitCourt, Cities; REL to Case, Cities, CircuitCourt
        # "NatureOfSuit":{}, # no fkey; REL to Case
        # FOREIGN KEYS ONE OR MORE LEVELS UP - 0
        "Case":{"case_id":self.new_case_id,"jurisdiction":None}, # fkeys to DistrictCourt, CircuitCourt, NatureOfSuit; REL to DistrictCourt, CircuitCourt, CasesInOtherCourts, JudgeOnCase, DocketEntry, NatureOfSuit, Party, Statutes, Offenses, Dispositions, Attorney, DocketHTML, UserTags
        "DocketHTML":{"case_id":self.new_case_id,"html":None,"parse":None},
        # "AttorneyOrg":{}, # fkey to Cities; REL to Cities, Attorneys
        # FOREIGN KEYS ONE OR MORE LEVELS UP - 1
        "JudgeOnCase":{}, # fkeys to Case, Judge; REL to Case, Judge
        "CasesInOtherCourts":{}, # fkey to Case; REL to Case
        #"DocketEntry":{}, # fkey to Case, Judge; REL to Attachment
        # FOREIGN KEYS ONE OR MORE LEVELS UP - 2
        # "Attachment":{}, # fkey to DocketEntry
        "Party":{}, # fkey to Case, CasesInOtherCourts, Roles; REL to Aliases, CasesInOtherCourts, Roles, Attorneys, ChargeEvents
        # FOREIGN KEYS ONE OR MORE LEVELS UP - 3
        # "Attorney":{}, # fkeys to AttorneyOrg, Case, Party; REL to AttorneyOrg, Case, Contact, Party
        # "Contact":{}, # fkey to Attorney; REL to Attorney
        # "Aliases":{}, # fkey to Party
        # "ChargeEvents":{}, # fkeys to Party, Statute, Offenses; REL to Statutes, Offenses, Dispositions
        # "Dispositions":{}, # fkeys to ChargeEvents, DispositionTypes; REL to DispositionTypes
        "DocketEntry":{}, # moved here for output readability
        }
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
        self.miss_count = 0
        self.nlp = nlp
        self.in_dir = in_dir
        self.tried_city = False
        self.default_state = default_state
        self.write = False
        self.main()

    def set_default_state(self):
        counts = {}
        for dist in self.dist:
            state = ''
            for name, abbreviation in self.state_abbrevs.items():
                if abbreviation.lower() == dist["state"].lower():
                    state = name
            index = self.tables["DocketHTML"]["html"].lower().find(dist["district"].lower())
            if index == -1:
                continue
            counts[(state,dist["district"])] = index
        dist_most = [sys.maxsize,'']
        for state_tuple, index in counts.items():
            if index < dist_most[0]:
                dist_most = [index, state_tuple]
        if dist_most:
            if 'New York' not in dist_most[1][0]:
                print(self.case_id)
            self.default_state = dist_most[1][0]
            return dist_most[1][1]
        counts = {}
        for state in self.state_abbrevs.keys():
            if self.default_state and self.default_state.lower().strip() == state.lower():
                return state
            counts[state] = self.tables["DocketHTML"]["html"].lower().count(state.lower())
        most = [0,'']
        for state, count in counts.items():
            if count >= most[0]:
                most = [count, state]
        self.default_state = most[1]
        return ''

    def case_id_val(self):
        try:
            val = self.parsed["docket_case_id"]
        except(KeyError):
            val = ""
        return val

    def abbrev(self,state):
        if not state:
            return '??'
        elif state.title() in self.state_abbrevs.keys():
            return self.state_abbrevs[state]
        elif state.split()[0].title() in self.state_abbrevs.keys():
            return self.state_abbrevs[state.split()[0].title()]
        elif state.strip() == 'DC':
            return 'DC'
        elif str("New " + state) in self.state_abbrevs.keys():
            return self.state_abbrevs[str("New " + state)]
        elif state.strip().upper() in self.state_abbrevs.values():
            return state.upper()
        else:
            tokens = state.split()
            length = len(tokens)
            for i in range(length):
                if tokens[i].title() in self.state_abbrevs.keys():
                    return self.state_abbrevs[tokens[i].title()]
                elif tokens[i].lower() == 'new' or tokens[i].lower() == 'west':
                    if i < length - 1:
                        if str(tokens[i]+' '+tokens[i+1]).title().strip() in self.state_abbrevs.keys():
                            return self.state_abbrevs[str(tokens[i]+' '+tokens[i+1]).title().strip()]
        return "??"

    def review_misses(self):
        to_be_removed = []
        docket_flags = False
        case_name = False
        if "docket_flags" in self.tables["Case"].keys():
            docket_flags = True
        if "case_name" in self.tables["Case"].keys():
            case_name = True
        for obj in self.missed_parses:
            if obj == "NO_FIND_MATCH_CITY_02 Northern District of Texas,  NV" or obj == "NO_FIND_MATCH_CITY_01 Northern District of Texas,  NV":
                to_be_removed.append(obj)
                continue
            if isinstance(obj, str):
                if "CASE_TITLE_DUPLICATES_" in obj:
                    if self.tables["Case"].get("case_name"):
                        if obj.replace("CASE_TITLE_DUPLICATES_",'') in self.tables["Case"].get("case_name"):
                            to_be_removed.append(obj)
                            continue
                elif "NO_FIND_MATCH_CITY" in obj:
                    if self.tried_city:
                        continue
                    elif obj[-2:].isupper():
                        city, comma, state = obj.rpartition('(')[2].partition(',')
                        city, state = self.match_cities(city, state)
                        self.add_col("Case","city_id",city)
                        self.add_col("Case","state_id",state)
                        to_be_removed.append(obj)
                        self.tried_city = True
                        continue
                continue
            elif not obj:
                to_be_removed.append(obj)
                continue
            else:
                try:
                    value = obj["field_value_attempt"]
                    if not value:
                        to_be_removed.append(obj)
                        continue
                    elif value == "Live Database":
                        to_be_removed.append(obj)
                        continue
                    if value.strip().replace('-','').isnumeric():
                        to_be_removed.append(obj)
                        self.add_other_case_no('case in other court',value)
                        continue
                    elif '-' in value and value.split(',')[0].replace('-','').strip().isnumeric():
                        to_be_removed.append(obj)
                        for case_no in value.split(','):
                            self.add_other_case_no('case in other court',case_no)
                        continue
                    add_flag = False
                    if value.isupper():
                        add_flag = True
                    elif sum([1 if x.isupper() else 0 for x in value.split(',')]) > 0:
                        add_flag = True
                    if add_flag:
                        if not docket_flags:
                            self.add_col("Case","docket_flags",value)
                        else:
                            self.tables["Case"]["docket_flags"] += ',' + value
                        to_be_removed.append(obj)
                        continue
                    if '(' in value and ')' in value and 'district' in value.lower():
                        if self.tables["Case"].get("city_id") != None:
                            continue
                        to_be_removed.append(obj)
                        if self.tables["Case"].get("district_id") != None and self.tables["Case"].get("city_id") != None:
                            continue
                        city = value.rpartition('(')[2].rpartition(')')[0]
                        try:
                            state = value.partition(''.join(['(',city,')']))[0].split()[-1]
                        except:
                            state = self.default_state
                        city, state = self.match_cities(city, state)
                        self.add_col("Case","city_id",city)
                        self.add_col("Case","state_id",state)
                        if "district_id" not in self.tables["Case"].keys():
                            print("Somehow no district here...")
                    if not add_flag and not case_name:
                        to_be_removed.append(obj)
                        self.add_col("Case","case_name",value)
                        continue
                    if "-cv-" in value.lower() or "-cr-" in value.lower():
                        self.add_other_case_no('case in other court',value)
                        to_be_removed.append(obj)
                        continue
                    if "district of" in value.lower() and "no_find" not in value.lower() and self.tables["Case"].get("district_id") == None:
                        #district_id = self.match_court(value, '', self.default_state, "district")
                        self.add_col("Case","district_id",value)
                        to_be_removed.append(obj)
                    continue
                except:
                    continue
        for obj in to_be_removed:
            try:
                self.missed_parses.remove(obj)
            except:
                continue

    def report_misses(self):
        report = False
        review = False
        for miss in self.missed_parses:
            if miss:
                review = True
                break
        if review:
            self.review_misses()
            for miss in self.missed_parses:
                if miss:
                    report = True
                    break
            if report and self.missed_parses:
                self.miss_count = len(self.missed_parses)

    def add_col(self, table, label, value):
        self.tables[table][label] = value

    def make_date(self, date_str, not_str=False):
        date_str = date_str.strip()
        try:
            for delimeter in ['/','-','.','_','|',' ',':']:
                if delimeter in date_str:
                    dates = date_str.split(delimeter)
                    break
            year = ''
            month = ''
            day = ''
            for date in dates:
                if len(date) == 4:
                    year = int(date)
                else:
                    if int(date) < 13:
                        if not month:
                            month = int(date)
                        else:
                            day = int(date)
                    else:
                        day = int(date)
            date = datetime(year,month,day)
            date = datetime.date(date)
            if not_str:
                return date
            return date.isoformat()
        except:
            print("We experienced an error with make_date(), but how?")

    def add_other_case_no(self, field, value):
        key_value = value.lower().strip()
        if key_value not in self.tables["CasesInOtherCourts"].keys():
            category = ''
            for word in ['magistrate','lead','member','appeals','district','circuit',
                         'probate','criminal','civil','bankruptcy','arbitration',
                         'related','superior','juvenile','county','parish',]:
                if word in field.lower():
                    category = word
                    break
                elif word in key_value:
                    category = word
                    break
            if not category:
                for word in ['eastern','southern','western','middle','northern','central',]:
                    if word in field.lower():
                        category = word
                        break
                    elif word in key_value:
                        category = word
                        break
            if not category and ("rel_case" in field.lower() or "rel_case" in key_value):
                category = 'related'
            self.add_col("CasesInOtherCourts",key_value,{"case_id":self.new_case_id,"other_id":value,"category":category})

    def match_nature_of_suit(self, value):
        for token in value.split():
            token = self.remove_stop_words(token.strip())
            if self.is_float(token):
                if token in self.nos.keys():
                    return self.nos[token]
                elif token[1:] in self.nos.keys():
                    return self.nos[token[1:]]
            else:
                for key in self.nos.keys():
                    if token.lower() == self.nos[key]["label"].lower():
                        return self.nos[key]
        report_string = "NO_FIND_NOS_"+value
        self.missed_parses.append(report_string)
        return value.title()

    def a_z_clean(self,strng):
        strng = self.remove_stop_words(strng)
        return  ' '.join(sorted(strng.lower().split()))

    def clean_district(self, court):
        try:
            singles = ['DC','MA','ME','MN','NH','PR','RI','DE','VT','VI','NJ','MD',
                        'SC','NE','ND','SD','AK','AZ','GU','HI','ID','MT','NV','MP',
                        'CO','KS','OR','NM','UT','WY',]
            if not court:
                return court
            court = court.lower()
            district_words = ['Eastern','Southern','Western','Middle','Northern',
                            'Central',]
            st = ''
            dw = ''
            for state in self.state_abbrevs.keys():
                if state.lower() in court:
                    st = state
            if not st:
                for state in self.state_abbrevs.keys():
                    if self.state_abbrevs[state] in court:
                        st = state
            for word in district_words:
                if word.lower() in court:
                    dw = word
                    if st:
                        if 'Islands' in st:
                            st += 'the '
                        return dw + ' District of ' + st
            if st:
                if self.state_abbrevs[st] in singles:
                    return 'District of ' + st
            for word in ['district of','dist. of','dist of']:
                if word in court:
                    pre_w, throwaway, post_w = court.partition(word)
                    pre_h = [x.strip() for x in pre_w.split()]
                    pre = pre_h[-1]
                    post_h = [x.strip() for x in post_w.split()]
                    post = post_h[0]
                    if pre[0] == 'c':
                        pre = 'Central '
                    elif pre[0] == 'e':
                        pre = 'Eastern '
                    elif pre[0] == 'm':
                        pre = 'Middle '
                    elif pre[0] == 'n':
                        pre = 'Northern '
                    elif pre[0] == 's':
                        pre = 'Southern '
                    elif pre[0] == 'w':
                        pre = 'Western '
                    else:
                        pre = ''
                    if post[0] == 'a':
                        if 'b' in post and 'm' in post:
                            post = 'Alabama'
                        elif 'z' in post and 'o' in post:
                            post = 'Arizona'
                        elif 'l' in post and 'k' in post:
                            post = 'Alaska'
                        elif 'k' in post and 'n' in post:
                            post = 'Arkansas'
                    elif post[0] == 'c':
                        if 'f' in post and 'r' in post:
                            post = 'California'
                        elif 'd' in post and 'a' in post:
                            post = 'Colorado'
                        elif 'm' in post and 'b' in post:
                            post = 'Columbia'
                        elif 't' in post and 'i' in post:
                            post = 'Connecticut'
                    elif post[0] == 'd':
                        if 'w' in post and 'r' in post:
                            post = 'Delaware'
                        elif 'c' in post:
                            post = 'Connecticut'
                    elif post[0] == 'f':
                        if 'l' in post and 'a' in post:
                            post = 'Florida'
                    elif post[0] == 'g':
                        if 'u' in post and 'm' in post:
                            post = 'Guam'
                        elif 'e' in post and 'i' in post:
                            post = 'Georgia'
                    elif post[0] == 'h':
                        if 'w' in post and 'i' in post:
                            post = 'Hawaii'
                    elif post[0] == 'i':
                        if 'l' in post and 'n' in post:
                            post = 'Illinois'
                        elif 'w' in post and 'a' in post:
                            post = 'Iowa'
                        elif 'd' in post and 'h' in post:
                            post = 'Idaho'
                    elif post[0] == 'k':
                        if 'n' in post and 's' in post:
                            post = 'Kansas'
                        elif 'y' in post and 'c' in post:
                            post = 'Kentucky'
                    elif post[0] == 'l':
                        if 'o' in post and 'u' in post:
                            post = 'Louisiana'
                    elif post[0] == 'm':
                        if 'e' in post and 's' in post and 'o' in post and 'i' in post:
                            post = 'Minnesota'
                        elif 'y' in post and 'l' in post and 'd' in post:
                            post = 'Maryland'
                        elif 'h' in post and 'i' in post and 'g' in post:
                            post = 'Michigan'
                        elif 't' in post and 'h' in post and 'u' in post:
                            post = 'Massachusetts'
                        elif 'o' in post and 'i' in post and 'r' in post:
                            post = 'Missouri'
                        elif 'p' in post and 'i' in post and 's' in post:
                            post = 'Mississippi'
                        elif 'e' in post and 'i' in post and 'n' in post:
                            post = 'Maine'
                        elif 't' in post and 'n' in post and 'a' in post:
                            post = 'Montana'
                    elif post[0] == 'n':
                        if 'e' in post and 'w' in post:
                            if len(post_h) > 1:
                                post_2 = post_h[1]
                                if 'h' in post_2 and 'p' in post_2:
                                    post = "New Hampshire"
                                elif 'm' in post and 'x' in post:
                                    post = 'New Mexico'
                                elif 'j' in post and 'r' in post:
                                    post = 'New Jersey'
                                elif 'y' in post and 'k' in post:
                                    post = 'New York'
                        elif 'v' in post and 'a' in post and 'd' in post:
                            post = 'Nevada'
                        elif 'b' in post and 'k' in post and 's' in post:
                            post = 'Nebraska'
                        elif ('t' in post and 'h' in post) or 'n.' in post:
                            if len(post_h) > 1:
                                post_2 = post_h[1]
                                if 'd' in post_2 and 'k' in post_2:
                                    post = "North Dakota"
                                elif 'c' in post and 'l' in post:
                                    post = 'North Carolina'
                                elif 'm' in post and 'r' in post:
                                    post = 'Northern Mariana Islands'
                    elif post[0] == 'o':
                        if 'k' in post and 'h' in post and 'm' in post:
                            post = 'Oklahoma'
                        elif 'h' in post and 'i' in post:
                            post = 'Ohio'
                        elif 'r' in post and 'g' in post and 'e' in post:
                            post = 'Oregon'
                    elif post[0] == 'p':
                        if 'a' in post and 'u' in post and 'l' in post:
                            post = 'Palau'
                        elif 'u' in post and 'o' in post and 'c' in post:
                            post = 'Puerto Rico'
                        elif 'e' in post and 'n' in post:
                            post = 'Pennsylvania'
                    elif post[0] == 'r':
                        if 'h' in post and 'd' in post and 'e' in post:
                            post = 'Rhode Island'
                    elif post[0] == 's':
                        if ('u' in post and 'o' in post) or 's.' in post:
                            if len(post_h) > 1:
                                post_2 = post_h[1]
                                if 'c' in post_2 and 'r' in post_2:
                                    post = 'South Carolina'
                                elif 'k' in post_2 and 't' in post_2:
                                    post = 'South Dakota'
                    elif post[0] == 't':
                        if 'x' in post and 'a' in post:
                            post = 'Texas'
                        elif 'n' in post and 's' in post:
                            post = 'Tennessee'
                    elif post[0] == 'u':
                        if 't' in post and 'h' in post:
                            post = 'Utah'
                    elif post[0] == 'v':
                        if 't' in post and 'm' in post:
                            post = 'Vermont'
                        elif 'g' in post and 'i' in post:
                            if len(post_h) > 1:
                                if 'd' in post_h[1] and 's' in post_h[1] and 'i' in post_h[1]:
                                    post = "Virgin Islands"
                            if not post and 'g' in post and 'n' in post:
                                post = 'Virginia'
                    elif post[0] == 'w':
                        if 't' in post and 'g' in post and 'h' in post:
                            post = 'Washington'
                        elif 's' in post and 'c' in post and 'n' in post:
                            post = 'Wisconsin'
                        elif 'y' in post and 'm' in post and 'o' in post:
                            post = 'Wyoming'
                        elif ('e' in post and 's' in post and 't' in post) or 'w.' in post:
                            post = 'West Virginia'
                    else:
                        post = ''
                    if post:
                        if pre:
                            return pre + 'District of ' + post
                        return 'District of ' + post
            report_string = "NO_FIND_DISTRICT_{c}".format(c=court)
            self.missed_parses.append(report_string)
            return court
        except:
            return ''

    def clean_circuit(self, court):
        court = court.lower()
        circuit_words = ['First','Second','Third','Fourth','Fifth','Sixth',
                        'Seventh','Eighth','Ninth','Tenth','Eleventh',
                        'Federal','District of Columbia','1st','2nd','3rd','4th',
                        '5th','6th','7th','8th','9th','10th','11th','Fed.']
        for word in circuit_words:
            if word.lower() in court:
                return word + ' Circuit'
        if 'circuit' in court or 'cir' in court:
            token = court.partition('circuit')[0].split()[-1].strip()
            if token[0] == 'f':
                if 'd' in token and 'e' in token:
                    return 'Federal Circuit'
                elif 'r' in token:
                    if 'u' in token:
                        return 'Fourth Circuit'
                    elif 't' in token:
                        return 'First Circuit'
                elif 'h' in court and 't' in court:
                    return 'Fifth Circuit'
            elif token[0] == 's':
                if 'x' in token:
                    return 'Sixth Circuit'
                elif 'v' in token:
                    return 'Seventh Circuit'
                elif 'c' in token:
                    return 'Second Circuit'
            elif token[0] == 't':
                if 'r' in token:
                    return 'Third Circuit'
                elif 'e' in token:
                    return 'Tenth Circuit'
            elif token[0] == 'e':
                if 'v' in token:
                    return 'Eleventh Circuit'
                elif 'g' in token:
                    return 'Eighth Circuit'
            elif 'n' in token and 't' in token and 'h' in token:
                return 'Ninth Circuit'
            elif 'c' in token and 'm' in token and 'b' in token:
                return 'District of Columbia Circuit'
            elif '11' in token:
                return 'Eleventh Circuit'
            elif '10' in token:
                return 'Tenth Circuit'
            elif '9' in token:
                return 'Ninth Circuit'
            elif '8' in token:
                return 'Eighth Circuit'
            elif '7' in token:
                return 'Seventh Circuit'
            elif '6' in token:
                return 'Sixth Circuit'
            elif '5' in token:
                return 'Fifth Circuit'
            elif '4' in token:
                return 'Fourth Circuit'
            elif '3' in token:
                return 'Third Circuit'
            elif '2' in token:
                return 'Second Circuit'
            elif '1' in token:
                return 'First Circuit'
        report_string = "NO_FIND_CIRCUIT_{c}".format(c=court)
        self.missed_parses.append(report_string)
        return court

    def match_court(self, court, city, state, key):
        if key == "district":
            court_dict = self.dist
            court = self.clean_district(court)
        elif key == "circuit":
            court_dict = self.circ
            court = self.clean_circuit(court)
        else:
            return None
        for row in court_dict:
            if court == row[key]:
                return row[key]
            elif self.a_z_clean(court) == self.a_z_clean(row[key]):
                return row[key]
        if state != "??":
            for row in court_dict:
                if state == row["state"]:
                    if city == row["city"]:
                        return row[key]
        for row in court_dict:
            if city == row["city"]:
                return row[key]
        report_string = "NO_FIND_MATCH_COUNRT_{v}".format(v=court)
        if self.parsed.get("URL_path"):
            abbrev = self.parsed.get("URL_path").rpartition("/")[0].rpartition("/")[2]
            if abbrev == 'cacd':
                return "Central District of California"
            elif abbrev == 'caed':
                return "Eastern District of California"
            elif abbrev == 'cand':
                return "Northern District of California"
            elif abbrev == 'casd':
                return "Southern District of California"
            elif abbrev == 'az':
                return "District of Arizona"
            elif abbrev == "nm":
                return "District of New Mexico"
        self.missed_parses.append(report_string)
        return court

    def add_judge(self, field, value, source):
        j = {"name":'',
             "status":field.partition("_to")[0],
             "honorific":'',
             "category":'',
             "case_id":self.new_case_id,
             "date_filed":'',
             "date_terminated":'',
             "original":value,}
        if "date_filed" in self.tables["Case"].keys():
            j["date_filed"] = self.tables["Case"]["date_filed"]
        if "date_terminated" in self.tables["Case"].keys():
            j["date_terminated"] = self.tables["Case"]["date_terminated"]
        honorifics = ['the Honorable', 'the Honourable','his Honor','his Honour',
                      'Honorable', 'Honourable','her Honor','her Honour','their Honor',
                      'their Honour','Honor','Honour','Hon.']
        titles = ['Magistrate Judge','Chief Judge','Senior Judge','Associate Judge',
                  'Supporting Judge','Acting Judge','Overseeing Judge','Presiding Judge',
                  'Chief Justice','Associate Justice','Main Judge','Lead Judge',
                  'Trial Judge','Sentencing Judge','Probate Judge','Parole Judge',
                  'Arbitrator','Judge','Esquire','Esq','Magistrate','Justice']
        for honor in honorifics:
            if honor.lower() in value.lower():
                j["honorific"] = honor.split()[-1]
                loc = value.lower().find(honor.lower())
                value = value[loc+len(honor):]
                break
        for title in titles:
            if title.lower() in value.lower():
                if title == 'Magistrate':
                    j["category"] = title + ' Judge'
                else:
                    j["category"] = title
                loc = value.lower().find(title.lower())
                value = value[loc+len(title):]
                break
        j["name"] = self.clean_ends(value)
        jtest = j["name"].lower()
        for h in honorifics:
            if h.lower() in jtest:
                jtest = jtest.replace(h.lower(),'')
        for t in titles:
            if t.lower() in jtest:
                jtest = jtest.replace(t.lower(),'')
        if 'initial appearance' in jtest:
            jtest = jtest.replace('initial appearance','')
        if len(jtest.split()) < 2:
            return ''
        elif jtest.split()[-1] == 'the':
            jtest = ' '.join(jtest.split()[:-1])
        if '.' in jtest and not any(word in jtest for word in ['sr.','jr.','dr.','mr.','esq.']):
            jtest = jtest.partition('.')[0]
        not_judge = ['defend','plaintiff','bail','order','decree','doc','decision',
                     'party','attorney','recorder','examiner','reporter','witness',
                     'motion','electronic','notice','consent','letter','summons',
                     'proclamation','court','/','entered']
        for n in not_judge:
            if n in jtest:
                return ''
        if not jtest:
            return ''
        unique = True
        for key in self.tables["JudgeOnCase"].keys():
            if j["name"] == key:
                unique = False
                break
            elif j["name"].lower() in key.lower():
                unique = False
                j["name"] = key
                break
            elif j["name"].lower().replace(',','').replace('.','').replace('-','') in key.lower().replace(',','').replace('.','').replace('-',''):
                unique = False
                j["name"] = key
                break
            elif key.lower().replace(',','').replace('.','').replace('-','') in j["name"].lower().replace(',','').replace('.','').replace('-',''):
                unique = False
                j["name"] = key
                break
        if unique:
            self.add_col("JudgeOnCase",j["name"],j)
        return j["name"]

    def find_judge(self, text):
        return_judges = []
        text = text.replace(':',' ').replace(';',' ').replace('_',' ')
        ltext = text.lower()
        honorifics = ['the Honorable', 'the Honourable','his Honor','his Honour',
                      'Honorable', 'Honourable','her Honor','her Honour','their Honor',
                      'their Honour','Honor','Honour',]
        titles = ['Magistrate Judge','Chief Judge','Senior Judge','Associate Judge',
                  'Supporting Judge','Acting Judge','Overseeing Judge','Presiding Judge',
                  'Chief Justice','Associate Justice','Main Judge','Lead Judge',
                  'Trial Judge','Sentencing Judge','Probate Judge','Parole Judge',
                  'Arbitrator','Judge','Esquire','Esq','Magistrate','Justice']
        for title in titles:
            honor = title
            if honor.lower() in ltext:
                name_start = len(honor)
                post_honor = text[ltext.find(honor.lower())+name_start:]
                judge_str = honor
                for count, token in enumerate(post_honor.split()):
                    if any(word.lower() in token.lower() for word in honorifics):
                        judge_str += ' ' + token
                        name_start = len(judge_str)
                    elif token.istitle() and not post_honor.isupper():
                        judge_str += ' ' + token
                    elif token.lower() == 'von' or token.lower() == 'van':
                        judge_str += ' ' + token
                    elif token.lower() == 'de' or token.lower() == 'du':
                        judge_str += ' ' + token
                    elif 'jr' in token.lower() or 'sr' in token.lower():
                        judge_str += ' ' + token
                    elif token.lower() == 'lo' or token.lower() == 'la':
                        judge_str += ' ' + token
                    elif token.lower() == 'los' or token.lower() == 'los':
                        judge_str += ' ' + token
                    elif count > 6:
                        break
                    else:
                        break
                names = judge_str[name_start:].split()
                if names and (len(names) == 3 or 'sr' in names[-1].lower() or 'jr' in names[-1].lower()):
                    return_judges.append(judge_str)
                    text = text[:ltext.find(judge_str.lower())]+' '+text[ltext.find(judge_str.lower())+name_start:]
                    ltext = text.lower()
                    continue
                prefix = judge_str[:name_start]
                look_here = ' '.join(text[ltext.find(prefix.lower())+name_start:].split()[0:6])
                doc = self.nlp(look_here)
                for ent in doc.ents:
                    if ent.label_ == 'PERSON':
                        judge_str = prefix + ' ' + ent.text
                        return_judges.append(judge_str)
                        text = text[:ltext.find(prefix.lower())]+' '+text[ltext.find(prefix.lower())+len(prefix):].replace(ent.text,' ')
                        ltext = text.lower()
                        break
        for honor in honorifics:
            if honor.lower() in ltext:
                name_start = len(honor)
                post_honor = text[ltext.find(honor.lower())+name_start:]
                judge_str = honor
                for count, token in enumerate(post_honor.split()):
                    if any(word.lower() in token.lower() for word in titles):
                        judge_str += ' ' + token
                        name_start = len(judge_str)
                    elif token.istitle() and not post_honor.isupper():
                        judge_str += ' ' + token
                    elif token.lower() == 'von' or token.lower() == 'van':
                        judge_str += ' ' + token
                    elif token.lower() == 'de' or token.lower() == 'du':
                        judge_str += ' ' + token
                    elif 'jr' in token.lower() or 'sr' in token.lower():
                        judge_str += ' ' + token
                    elif token.lower() == 'lo' or token.lower() == 'la':
                        judge_str += ' ' + token
                    elif token.lower() == 'los' or token.lower() == 'los':
                        judge_str += ' ' + token
                    elif count > 5:
                        break
                    else:
                        break
                names = judge_str[name_start:].split()
                if names and (len(names) == 3 or 'sr' in names[-1].lower() or 'jr' in names[-1].lower()):
                    return_judges.append(judge_str)
                    text = text[:ltext.find(judge_str.lower())]+' '+text[ltext.find(judge_str.lower())+name_start:]
                    ltext = text.lower()
                    continue
                prefix = judge_str[:name_start]
                look_here = ' '.join(text[ltext.find(prefix.lower())+name_start:].split()[0:5])
                doc = self.nlp(look_here)
                for ent in doc.ents:
                    if ent.label_ == 'PERSON':
                        judge_str = prefix + ' ' + ent.text
                        return_judges.append(judge_str)
                        text = text[:ltext.find(prefix.lower())]+' '+text[ltext.find(prefix.lower())+len(prefix):].replace(ent.text,' ')
                        ltext = text.lower()
                        break
        return return_judges

    def parse_judge(self, entry_text):
        lower_text = entry_text.lower()
        status = ''
        both = []
        if 'assign' in lower_text:
            both.append(lower_text.find('assign'))
            status = 'assign'
        if 'refer' in lower_text:
            if status:
                status = 'both'
                both.append(lower_text.find('refer'))
            else:
                status = 'refer'
        judge_attempt = []
        for judge in self.tables["JudgeOnCase"].keys():
            full_name = self.tables["JudgeOnCase"][judge]["name"]
            written = self.tables["JudgeOnCase"][judge]["original"]
            if written.lower() in lower_text:
                judge_attempt.append(full_name)
                continue
            if full_name.lower() in lower_text:
                judge_attempt.append(full_name)
                continue
            last_name = full_name.split()[-1]
            if last_name.lower() in lower_text:
                judge_attempt.append(full_name)
                continue
            first_name = full_name.split()[0]
            for prefix in ['Judge ','Magistrate ','Justice ']:
                check = prefix+first_name
                if check.lower() in lower_text:
                    judge_attempt.append(full_name)
                    continue
                if len(last_name) > 4:
                    check = prefix+last_name[:4]
                    if check.lower() in lower_text:
                        judge_attempt.append(full_name)
                        continue
                if len(first_name) > 4:
                    check = prefix+first_name[:4]
                    if check.lower() in lower_text:
                        judge_attempt.append(full_name)
                        continue
        more_judges = self.find_judge(entry_text)
        judges = judge_attempt + more_judges
        if judges:
            return_judges = []
            for judge in judges:
                if status == 'both':
                    locj = lower_text.find(judge.split()[0].lower())
                    a_dst = abs(locj-both[0])
                    r_dst = abs(locj-both[1])
                    if a_dst < r_dst:
                        added_judge = self.add_judge("assigned_to",judge,'')
                    else:
                        added_judge = self.add_judge("referred_to",judge,'')
                elif status == 'assign':
                    added_judge = self.add_judge("assigned_to",judge,'')
                elif status == 'refer':
                    added_judge = self.add_judge("referred_to",judge,'')
                else:
                    added_judge = self.add_judge("_to",judge,'')
                unique = True
                for j in return_judges:
                    if added_judge == j:
                        unique = False
                        break
                    elif added_judge.lower() in j.lower():
                        unique = False
                        break
                if unique:
                    return_judges.append(added_judge)
            return return_judges
        return ''

    def clean_cities(self, city):
        for punct in ['/','-','.','_','|',':',',','division','Division',"'",'"','(',')','div','Div']:
            city = city.replace(punct, ' ')
        city = ' '.join([x.strip().title() for x in city.split()])
        city = city.replace('Saint','St').replace('Mount','Mt').replace('Fort','Ft').replace('Junction','Jct').replace(' ','')
        return city.lower()

    def test_city_matches(self, city, state):
        for citystate in self.cities.keys():
            if str(city+state) in citystate:
                return self.cities[citystate]["city"], self.cities[citystate]["state"]
            elif str(city+state).lower() in citystate.lower():
                return self.cities[citystate]["city"], self.cities[citystate]["state"]
            clean_input = self.clean_cities(city)
            clean_target = self.clean_cities(self.cities[citystate]["city"])
            if str(clean_input+state) in str(clean_target+self.cities[citystate]["state"]):
                return self.cities[citystate]["city"], self.cities[citystate]["state"]
            elif clean_input in clean_target and (state == '??' or not state):
                return self.cities[citystate]["city"], self.cities[citystate]["state"]
            elif int(fuzz.ratio(clean_input,clean_target)) >= 85 and state == self.cities[citystate]["state"]:
                return self.cities[citystate]["city"], self.cities[citystate]["state"]
            elif int(fuzz.ratio(clean_input,clean_target)) >= 85 and (state == '??' or not state):
                return self.cities[citystate]["city"], self.cities[citystate]["state"]
            else:
                continue
        return None, None

    def match_cities(self, city, state):
        state = self.abbrev(state)
        if not city or city.strip() == '':
            return None, None
        c, s = self.test_city_matches(city, state)
        if c:
            return c, s
        if len(city) < 4:
            if city.lower() not in ['nyc','la','bos','chi','det','atl','dc','sf','sea',]:
                report_string = "NO_FIND_MATCH_CITY_{v}, {s}".format(v=city,s=state)
                self.missed_parses.append(report_string)
                return None, None
            else:
                if 'nyc' in city.lower():
                    new_c = "New York City"
                elif 'la' in city.lower():
                    new_c = "Los Angeles"
                elif 'bos' in city.lower():
                    new_c = "Boston"
                elif 'chi' in city.lower():
                    new_c = "Chicago"
                elif 'det' in city.lower():
                    new_c = "Detroit"
                elif 'atl' in city.lower():
                    new_c = "Atlanta"
                elif 'dc' in city.lower():
                    new_c = "Washington, DC"
                elif 'sf' in city.lower():
                    new_c = "San Francisco"
                elif 'sea' in city.lower():
                    new_c = "Seattle"
                c, s = self.test_city_matches(new_c, state)
                if c:
                    return c, s
                else:
                    report_string = "NO_FIND_MATCH_CITY_{v}, {s}".format(v=city,s=state)
                    self.missed_parses.append(report_string)
                    return None, None
        try_new_state = []
        try_new_city = []
        doc = self.nlp(city)
        for ent in doc.ents:
            if ent.label_ == 'GPE':
                if ent.text in self.state_abbrevs.keys() or ent.text in self.state_abbrevs.values():
                    check = self.abbrev(ent.text)
                    if check != state:
                        try_new_state.append(check)
                else:
                    try_new_city.append(ent.text)
        if try_new_city:
            for newc in try_new_city:
                c, s = self.test_city_matches(newc, state)
                if c:
                    return c, s
                else:
                    if try_new_state:
                        for news in try_new_state:
                            c, s = self.test_city_matches(newc, news)
                            if c:
                                return c, s
        elif try_new_state:
            for news in try_new_state:
                c, s = self.test_city_matches(city, news)
                if c:
                    return c, s
        for char in city:
            if char.isnumeric():
                report_string = "NO_FIND_MATCH_CITY_{v}, {s}".format(v=city,s=state)
                self.missed_parses.append(report_string)
                return None, None
        tokens = city.split()
        if tokens:
            if tokens[-1] in self.state_abbrevs.keys() or tokens[-1] in self.state_abbrevs.values():
                city = ' '.join(tokens[:-1])
        if city.isupper() or city.islower():
            city = city.title()
        city = self.clean_ends(city)
        combined = city+state
        self.cities[combined] = {"city":city,"state":state}
        return self.cities[combined]["city"],self.cities[combined]["state"]

    def parse_addl_docket_fields(self, obj_list):
        state = ''
        city = ''
        district = ''
        circuit = ''
        court_type = {}
        date_filed = ''
        date_terminated = ''
        case_title = ''
        latest_judge = ''
        non_title_words = ['v.','et','al','of','the','USA','U.S.A','and','vs.','U.S.','US','v','vs']
        for obj in obj_list:
            try:
                field = obj["field_name_attempt"]
                value = obj["field_value_attempt"]
                source = obj["found_in_section"]
            except(KeyError):
                print("{C}\tADDL DOCKET FIELDS".format(c=self.case_id[:-7]))
                continue
            if not value:
                continue
            elif 'case in other court:' in value.lower():
                pre, label, case_no = value.lower().partition('case in other court:')
                if 'usdc' in case_no:
                    field = 'district'
                elif 'usca' in case_no:
                    field = 'appeals'
                elif 'usbc' in case_no:
                    field = 'bankruptcy'
                else:
                    field = field+' '+pre+' '+label
                self.add_other_case_no(field, case_no)
            elif field == "court_type":
                if "district" in value.lower():
                    court_type["district"] = ''
                elif "circuit" in value.lower():
                    court_type["circuit"] = ''
            elif field == "district":
                if "district" in court_type.keys():
                    if not court_type["district"]:
                        court_type["district"] = value
                    else:
                        if not district:
                            district = value
                        else:
                            district += '*|*' + value
                elif "circuit" not in court_type.keys():
                    court_type["district"] = value
                else:
                    if not district:
                        district = value
                    else:
                        district += '*|*' + value
            elif field == "circuit":
                if "circuit" in court_type.keys():
                    if not court_type["circuit"]:
                        court_type["circuit"] = value
                    else:
                        if not circuit:
                            circuit = value
                        else:
                            circuit += '*|*' + value
                else:
                    if not circuit:
                        circuit = value
                    else:
                        circuit += '*|*' + value
            elif field == "court_city":
                city = value
            elif field == "court_state":
                state = value
            elif field == "case_type":
                if "civil" in value.lower():
                    self.add_col("Case","case_type","civil")
                else:
                    self.add_col("Case","case_type","criminal")
            elif field == "case_title":
                if not case_title:
                    case_title = ' '.join([x.title() if x not in non_title_words else x for x in value.split()])
                    self.add_col("Case","case_name",case_title)
                else:
                    self.missed_parses.append("CASE_TITLE_DUPLICATES_"+case_title)
            elif field == "date_filed":
                date_filed = self.make_date(value, not_str=True)
                self.add_col("Case","date_filed",date_filed.isoformat())
                self.add_col("Case","year",str(int(date_filed.year)))
            elif field == "date_terminated":
                date_terminated = self.make_date(value, not_str=True)
                self.add_col("Case","date_terminated",date_terminated.isoformat())
            elif field == "assigned_to" or field == "referred_to":
                judge_id = self.add_judge(field, value, source)
                latest_judge = field
            elif "case_number" in field or "other_court" in field or field == "rel_case" or field == 'lead_case':
                if "case_numbers" in field:
                    other_cases = value.split(',')
                    for c in other_cases:
                        self.add_other_case_no(field, c)
                else:
                    self.add_other_case_no(field, value)
            elif field == "cause":
                self.add_col("Case",field,value)
            elif field == "demand":
                self.add_col("Case",field,value)
            elif field == "jury_demand":
                self.add_col("Case",field,value)
            elif field == "nature_of_suit":
                nature_of_suit_id = self.match_nature_of_suit(value)
                self.add_col("Case","nature_of_suit_id",nature_of_suit_id)
            elif field == "jurisdiction":
                self.add_col("Case",field,value)
            elif "status" in field:
                self.add_col("Case","case_status",value)
            elif "download" in field:
                self.add_col("Case","download_court",value)
            elif "error" in field:
                self.add_col("Case","entered_in_error",value)
            elif "sealed" in field:
                self.add_col("Case","sealed",value)
            elif self.is_float(field) and isinstance(self.is_float(value[0]),float):
                case_no = field + ':' + value
                self.add_other_case_no('member', case_no)
            elif field == 'member_case':
                if 'view member case' in value.lower():
                    value = 'member_case'
                self.add_other_case_no(field, value)
            elif '_' in field and self.includes_state(field):
                self.add_other_case_no("district", value)
            elif '_cases' in field:
                for case_no in value.split(','):
                    if self.seems_like_case_no(case_no):
                        self.add_other_case_no(field, case_no)
            elif field == 'in_re' or 'in re ' in value.lower() or field == 're':
                if not case_title:
                    case_title = ' '.join([x.title() if x not in non_title_words else x for x in value.split()])
                    self.add_col("Case","case_name",case_title)
                else:
                    self.missed_parses.append("CASE_TITLE_DUPLICATES_"+case_title)
            elif 'case' in field and self.seems_like_case_no(value):
                self.add_other_case_no('', value)
            elif field == "unsure":
                if "unknown" in value.lower():
                    continue
                elif ' et al' in value.lower():
                    if not case_title:
                        case_title = ' '.join([x.title() if x not in non_title_words else x for x in value.split()])
                        self.add_col("Case","case_name",case_title)
                    else:
                        self.missed_parses.append("CASE_TITLE_DUPLICATES_"+case_title)
                elif ' vs. ' in value.lower() or ' v. ' in value.lower() or ' v ' in value.lower() or ' v.' in value.lower() or ' vs.' in value.lower() or ' vs ' in value.lower() or 'in the matter of' in value.lower() or '-v-' in value.lower() or ' vs, ' in value.lower():
                    if not case_title:
                        case_title = ' '.join([x.title() if x not in non_title_words else x for x in value.split()])
                        self.add_col("Case","case_name",case_title)
                    else:
                        self.missed_parses.append("CASE_TITLE_DUPLICATES_"+case_title)
                elif 'court' in value.lower():
                    pre_c, c, post_c = value.lower().partition('court')
                    self.add_other_case_no(pre_c+c, post_c)
                elif self.seems_like_case_no(value):
                    self.add_other_case_no("district", value)
                elif 'usdc' in value.lower() or 'usbc' in value.lower() or 'usca' in value.lower():
                    if 'usdc' in value.lower():
                        self.add_other_case_no('district', value.lower().replace('usdc',' ').strip())
                    elif 'usca' in value.lower():
                        self.add_other_case_no('appeals', value.lower().replace('usca',' ').strip())
                    elif 'usbc' in value.lower():
                        self.add_other_case_no('bankruptcy', value.lower().replace('usbc',' ').strip())
                elif ' Judge ' in value:
                    judge_id = self.add_judge(latest_judge, value.strip(), source)
                elif 'court of' in value.lower():
                    pre_comma, comma, post_comma = value.partition(',')
                    if not post_comma:
                        self.add_other_case_no(value, value)
                    else:
                        self.add_other_case_no(pre_comma, post_comma)
                else:
                    self.missed_parses.append(obj)
            else:
                self.missed_parses.append(obj)
            if source != "Header":
                if not self.handle_key_error(self.parse_later,source):
                    self.parse_later[source] = []
                self.parse_later[source].append(obj)
        city, state = self.match_cities(city, state)
        self.add_col("Case","city_id",city)
        self.add_col("Case","state_id",state)
        court_circuit = ''
        if 'circuit' in court_type.keys():
            court_circuit = self.match_court(court_type["circuit"], city, state, "circuit")
            if court_circuit:
                self.add_col("Case","circuit_id",court_circuit)
                self.add_col("Case","court_type","circuit")
        if 'district' in court_type.keys():
            district_id = self.match_court(court_type["district"], city, state, "district")
            self.add_col("Case","district_id",district_id)
            if not court_circuit:
                self.add_col("Case","court_type","district")
            else:
                self.missed_parses.append("NO_FIND_COURT-TYPE_|"+court_circuit+'|'+district_id)
        if circuit:
            for other in circuit.split('*|*'):
                self.add_other_case_no("circuit", other)
        if district:
            for other in district.split('*|*'):
                self.add_other_case_no("district", other)
        if date_filed and date_terminated:
            duration = date_terminated - date_filed
            self.add_col("Case","case_duration",duration.days)

    def seems_like_case_no(self, value):
        has_alpha = False
        has_pair_alpha = False
        has_num = False
        has_pair_num = False
        has_dash = False
        has_colon = False
        length = len(value)
        for i in range(length):
            char = value[i]
            if i == length - 1:
                pair = ''
            else:
                pair = char + value[i+1]
            if char.isalpha():
                has_alpha = True
            elif char.isnumeric():
                has_num = True
            elif char == '-':
                has_dash = True
            elif char == ':':
                has_colon = True
            if pair:
                if pair.isalpha():
                    has_pair_alpha = True
                elif pair.isnumeric():
                    has_pair_num = True
            if has_alpha and has_dash:
                if has_pair_num:
                    return True
                elif has_num and has_colon:
                    return True
        return False

    def includes_state(self, value):
        for key in self.state_abbrevs.keys():
            if key.lower() in value.lower():
                return key
            elif key.lower() in ' '.join(value.replace('_',' ').split()):
                return key
        return False

    def handle_key_error(self, dict, try_key):
        try:
            value = dict[try_key]
            return value
        except(KeyError):
            return ""

    def parse_docket_text(self, parsed):
        try:
            test = parsed["rows"]
        except(TypeError):
            found = False
            for parse in parsed:
                if "Proceedings for case" in parse and "are not available" in parse:
                    entry = {}
                    entry["ordinal_number"] = 0
                    entry["case_id"] = self.new_case_id
                    entry["entry_text"] = parsed
                    self.add_col("DocketEntry",0,parsed)
                    found = True
            if not found and parsed:
                print('\n--PARSE as list: '+self.new_case_id+'\n')
                self.missed_parses.append(parsed)
            return None
        for obj in parsed["rows"]:
            if not obj["date"] and not obj["number"] and obj["ordinal_number"] == 0:
                continue
            entry = {}
            entry["entry_number"] = obj["number"]
            entry["entry_date"] = self.make_date(obj["date"])
            entry["ordinal_number"] = obj["ordinal_number"]
            entry["case_id"] = self.new_case_id
            entry["entry_text"] = obj["text"]
            if "sealed" in entry["entry_text"].lower():
                entry["sealed"] = True
            else:
                entry["sealed"] = None
            entry["entered_in_error"] = None
            for verb in ['filed','reported','entered','recorded','transcribed','listed','added',]:
                phrase = verb + ' in error'
                if phrase in entry["entry_text"].lower():
                    entry["entered_in_error"] = True
                    break
            entry["judge_name"] = self.parse_judge(entry["entry_text"])
            # entry["judge_nid"] = self.add_judge("_to",["judge_name"],'')
            entry["jurisdiction"] = self.tables["Case"]["jurisdiction"]
            attachments = {}
            for count, link in enumerate(obj["links"].keys()):
                if count == 0 and obj["links"][link] == obj["number"]:
                    entry["url"] = link
                    continue
                attachments[str(count)] = {"url":link,"text":obj["links"][link]}
            entry["attachments"] = attachments
            self.add_col("DocketEntry",obj["ordinal_number"],entry)

    def classify_charge_type(self, field):
        charge_type = ''
        status = ''
        if "count" in field:
            charge_type = "count"
        elif "offense" in field:
            charge_type = "offense"
        elif "complaint" in field:
            charge_type = "complaint"
        else:
            charge_type = "other"
        if "pending" in field:
            status = "pending"
        elif "opening" in field:
            status = "opening"
        elif "terminated" in field:
            status = "terminated"
        else:
            status = None
        return charge_type, status

    def remove_stop_words(self,line):
        stop_words = [' the ',' of ',' to ',' by ',' for ',' in ',' at ',' her ',
                      ' his ',' he ',' she ',' it ',' was ',' were ',' is ',
                      ' are ',' as ',]
        for word in stop_words:
            line = line.replace(word,' ')
            line = line.replace(word.title(),' ')
            line = line.replace(word.upper(),' ')
        for word in ['(',')','{','}','_','<','>','~','`',':']:
            line = line.replace(word, ' ').strip()
        return line

    def is_float(self,test_string):
        try:
            return float(test_string)
        except(ValueError):
            try:
                if ".00" in test_string:
                    test_string = test_string.partition(".00")[0] + ".00"
                if "$" in test_string:
                    test_string = test_string.partition("$")[2].strip()
                if test_string:
                    return float(test_string)
                else:
                    return float("a")
            except(ValueError):
                try:
                    for char in ['$',',','-','_','?','<','>','(',')','!','+',';','"',']','[',]:
                        test_string = test_string.replace(char,'')
                    test_string = test_string.strip()
                    return float(test_string)
                except(ValueError):
                    try:
                        sum = 0
                        written_numbers = {'one':1,'two':2,'three':3,'fourteen':14,
                                            'five':5,'sixteen':16,'seventeen':17,
                                            'eighteen':18,'nineteen':19,'ten':10,
                                            'eleven':11,'twelve':12,'thirteen':13,
                                            'four':4,'fifteen':15,'sixty':60,
                                            'seventy':70,'eighty':80,'ninety':90,
                                            'twenty':20,'thirty':30,'forty':40,
                                            'fifty':50,'six':6,'seven':7,'eight':8,
                                            'nine':9,'hundred':100}
                        test_string = test_string.lower()
                        for num in written_numbers.keys():
                            if num in test_string.lower():
                                test_string = test_string.replace(num,'').strip()
                                sum += written_numbers[num]
                        if sum > 0:
                            if test_string and test_string.strip() != '':
                                return float(test_string)
                            else:
                                return float(sum)
                    except(ValueError):
                        return False

    def find_last_index(self, after, before, words, backup_words, units=["months","years"]):
        order = [after,before]
        segment = [order[0],units[0]]
        last_idx = max([order[0].rfind(word) for word in words])
        if last_idx == -1:
            last_idx = max([order[1].rfind(word) for word in words])
            segment[0] = order[1]
            if last_idx == -1:
                last_idx = max([order[0].rfind(word) for word in backup_words])
                segment = [order[0],units[1]]
                if last_idx == -1:
                    last_idx = max([order[1].rfind(word) for word in backup_words])
                    segment[0] = order[1]
        return last_idx, segment

    def extract_disposition_value(self, line, keyword, location, term=""):
        before = line[:location]
        after = line[location:]
        clean_before = self.remove_stop_words(before)
        clean_after = self.remove_stop_words(after)
        value = None
        units = None
        helpers = ['total','Total','TOTAL']
        last_idx = -1
        if keyword in ['probation','supervised_release']:
            keywords = ['year','Year','YEAR',]
            backup_keywords = ["month","MONTH","Month"]
            last_idx, segment = self.find_last_index(clean_after, clean_before, keywords, backup_keywords,units=["years","months"])
        elif keyword in ['community_service']:
            keywords = ['hours','Hours','HOURS']
            backup_keywords = ['HRS','Hrs','hrs']
            last_idx, segment = self.find_last_index(clean_before, clean_after, keywords, backup_keywords,units=["hours","hours"])
        elif keyword in ['prison']:
            keywords = ["month","MONTH","Month"]
            backup_keywords = ['year','Year','YEAR']
            last_idx, segment = self.find_last_index(clean_after, clean_before, keywords, backup_keywords)
        elif keyword in ['assessment','fine','restitution']:
            if "$" in clean_after and "$" not in clean_before:
                tokens = [word for word in clean_after.split() if "$" in word]
                length = len(tokens)
                for i in range(length):
                    idx = length - i - 1
                    suspect = tokens[idx]
                    if self.is_float(suspect):
                        return (self.is_float(suspect), "Dollars")
            elif "$" in clean_before and "$" not in clean_after:
                tokens = [word for word in clean_before.split() if "$" in word]
                length = len(tokens)
                for i in range(length):
                    idx = length - i - 1
                    suspect = tokens[idx]
                    if self.is_float(suspect):
                        return (self.is_float(suspect), "Dollars")
            elif "$" not in clean_before and "$" not in clean_after:
                keywords = ['.00','dollars','Dollars','DOLLARS',]
                backup_keywords = ['AMOUNT','Amount','amount']
                last_idx, segment = self.find_last_index(clean_before, clean_after, keywords, backup_keywords,units=["Dollars","Dollars"])
            else:
                pseudo_lines = line.split(", ")
                for pseudo_line in pseudo_lines:
                    if term in pseudo_line and ("$" in pseudo_line or ".00" in pseudo_line):
                        tokens = [token for token in pseudo_line.split() if "$" in token or ".00" in token]
                        length = len(tokens)
                        for i in range(length):
                            idx = length - i - 1
                            suspect = tokens[idx]
                            if self.is_float(suspect):
                                return (self.is_float(suspect), "Dollars")
                return (value, units)
        if last_idx == -1:
            if keyword == 'prison' and not value:
                if 'time served' in line.lower():
                    return ('Time Served',None)
                elif 'revoke' in line.lower() or 'terminat' in line.lower() or 'cancel' in line.lower():
                    return ('Terminated',None)
            elif not value and (keyword == 'supervised_release' or keyword == 'probation'):
                if ' life' in line.lower():
                    return ('Life',None)
                elif 'revoke' in line.lower() or 'terminat' in line.lower() or 'cancel' in line.lower():
                    return ('Terminated',None)
            return (value, units)
        try:
            search_area = segment[0][:last_idx].strip().split()
            length = len(search_area)
            for i in range(length):
                idx = length - i - 1
                if self.is_float(search_area[idx]):
                    return (self.is_float(search_area[idx]),segment[1])
        except(TypeError):
            print("Failed to find {k} within {l}".format(k=keyword,l=segment[0]))
            pass
        try:
            search_area = segment[0][last_idx:].strip().split()
            length = len(search_area)
            for i in range(length):
                idx = length - i - 1
                if self.is_float(search_area[idx]):
                    return (self.is_float(search_area[idx]),segment[1])
        except(TypeError):
            print("Failed to find {k} within {l}".format(k=keyword,l=segment[0]))
        if keyword == 'prison' and not value:
            if 'time served' in line.lower():
                return ('Time Served',None)
            elif 'revoke' in line.lower() or 'terminat' in line.lower() or 'cancel' in line.lower():
                return ('Terminated',None)
        elif not value and (keyword == 'supervised_release' or keyword == 'probation'):
            if ' life' in line.lower():
                return ('Life',None)
            elif 'revoke' in line.lower() or 'terminat' in line.lower() or 'cancel' in line.lower():
                return ('Terminated',None)
        # Failure case
        return (value, units)

    def classify_disposition(self,line,charge_dict,index):
        types = charge_dict
        prison_words = ['prison', 'PRISON', 'BOP', 'penitentiary', 'PENITENTIARY',
                        'Prison','Penitentiary','Impr','IMPR','impr','impi','IMPI',
                        'Impi','pirson','PIRSON','Pirson']
        probation_words = ['Probation','probation','PROBATION',]
        supervision_words = ['supervised release','Supervised Release',
                            'Supervised release','SUPERVISED RELEASE','SR',]
        rest_words = ["Restitution","restitution","RESTITUTION"," REST ",]
        fine_words = [" FINE "," Fine "," fine ",' FINE',' Fine',' fine','FINE ','Fine ']
        dismissed_words = ["DISMISS","dismiss","Dismiss"]
        acquit_words = ['Acquit','acquit','ACQUIT']
        assessment_words = ['Assessment','ASSESSMENT','assessment','SA-',' SA ','SA ']
        community_words = ["Community service","community service","COMMUNITY SERVICE",
                            "Community Service"]
        rule_20_words = ['Rule 20','RULE 20','rule 20','Consent to Transfer',
                        'CONSENT TO TRANSFER','consent to transfer']
        for word in probation_words:
            location = line.find(word)
            if location == -1:
                continue
            types["probation"]["found"] = (index,location)
            value, units = self.extract_disposition_value(line, "probation", location)
            if not types["probation"]["value"] and not types["probation"]["units"]:
                types["probation"]["value"] = value
                types["probation"]["units"] = units
            elif types["probation"]["units"] == units or value == 'Life' or value == 'Terminated':
                types["probation"]["value"] = value
                types["probation"]["units"] = units
            break
        for word in supervision_words:
            location = line.find(word)
            if location == -1:
                continue
            types["supervised_release"]["found"] = (index,location)
            value, units = self.extract_disposition_value(line, "supervised_release", location)
            if not types["supervised_release"]["value"] and not types["supervised_release"]["units"]:
                types["supervised_release"]["value"] = value
                types["supervised_release"]["units"] = units
            elif types["supervised_release"]["units"] == units or value == 'Life' or value == 'Terminated':
                types["supervised_release"]["value"] = value
                types["supervised_release"]["units"] = units
            break
        for word in prison_words:
            location = line.find(word)
            if location == -1:
                continue
            types["prison"]["found"] = (index,location)
            value, units = self.extract_disposition_value(line, "prison", location)
            if types["prison"]["value"] and types["prison"]["units"]:
                if types["prison"]["units"] == units:
                    sr = types["supervised_release"]["value"]
                    pr = types["probation"]["value"]
                    if sr or pr:
                        sr_u = types["supervised_release"]["units"]
                        pr_u = pr = types["probation"]["units"]
                        if sr == value and sr_u == units:
                            pass
                        elif pr == value and pr_u == units:
                            pass
                        else:
                            types["prison"]["value"] = value
                            types["prison"]["units"] = units
                    else:
                        types["prison"]["value"] = value
                        types["prison"]["units"] = units
                elif value == 'Time Served' or value == 'Terminated':
                    types["prison"]["value"] = value
                    types["prison"]["units"] = units
            else:
                types["prison"]["value"] = value
                types["prison"]["units"] = units
            break
        for word in rest_words:
            location = line.find(word)
            if location == -1:
                continue
            types["restitution"]["found"] = (index,location)
            value, units = self.extract_disposition_value(line, "restitution", location, term=word)
            if not types["restitution"]["value"] and not types["restitution"]["units"]:
                types["restitution"]["value"] = value
                types["restitution"]["units"] = units
            elif value and types["restitution"]["units"] == units:
                types["restitution"]["value"] = value
                types["restitution"]["units"] = units
            break
        for word in fine_words:
            location = line.find(word)
            if location == -1:
                continue
            types["fine"]["found"] = (index,location)
            value, units = self.extract_disposition_value(line, "fine", location, term=word)
            if not types["fine"]["value"] and not types["fine"]["units"]:
                types["fine"]["value"] = value
                types["fine"]["units"] = units
            elif value and types["fine"]["units"] == units:
                types["fine"]["value"] = value
                types["fine"]["units"] = units
            break
        for word in dismissed_words:
            location = line.find(word)
            if location == -1:
                continue
            types["dismissal"]["found"] = (index,location)
            types["dismissal"]["value"] = None
            types["dismissal"]["units"] = None
            break
        for word in acquit_words:
            location = line.find(word)
            if location == -1:
                continue
            types["acquittal"]["found"] = (index,location)
            types["acquittal"]["value"] = None
            types["acquittal"]["units"] = None
            break
        for word in assessment_words:
            location = line.find(word)
            if location == -1:
                continue
            types["assessment"]["found"] = (index,location)
            value, units = self.extract_disposition_value(line, "assessment", location, term=word)
            if not types["assessment"]["value"] and not types["assessment"]["units"]:
                types["assessment"]["value"] = value
                types["assessment"]["units"] = units
            elif value and types["assessment"]["units"] == units:
                types["assessment"]["value"] = value
                types["assessment"]["units"] = units
            break
        for word in community_words:
            location = line.find(word)
            if location == -1:
                continue
            types["community_service"]["found"] = (index,location)
            value, units = self.extract_disposition_value(line, "community_service", location)
            if not types["community_service"]["value"] and not types["community_service"]["units"]:
                types["community_service"]["value"] = value
                types["community_service"]["units"] = units
            elif value and types["community_service"]["units"] == units:
                types["community_service"]["value"] = value
                types["community_service"]["units"] = units
            break
        for word in rule_20_words:
            location = line.find(word)
            if location == -1:
                continue
            types["rule_20_transfer"]["found"] = (index,location)
            types["rule_20_transfer"]["value"] = None
            types["rule_20_transfer"]["units"] = None
        return types

    def add_disposition_details(self, line):
        if line == None:
            return None
        types = {"prison":{"found":False,"value":None,"units":None},
                "probation":{"found":False,"value":None,"units":None},
                "supervised_release":{"found":False,"value":None,"units":None},
                "community_service":{"found":False,"value":None,"units":None},
                "restitution":{"found":False,"value":None,"units":None},
                "fine":{"found":False,"value":None,"units":None},
                "assessment":{"found":False,"value":None,"units":None},
                "dismissal":{"found":False,"value":None,"units":None},
                "acquittal":{"found":False,"value":None,"units":None},
                "rule_20_transfer":{"found":False,"value":None,"units":None},
                }
        sublines = line.split(". ")
        for count, subline in enumerate(sublines):
            count *= 10
            if ";" in subline:
                subsublines = subline.split(";")
                for subcount, sub in enumerate(subsublines, start=1):
                    count += subcount
                    types = self.classify_disposition(sub,types,count)
                continue
            types = self.classify_disposition(subline,types,count)
        submit = {}
        for key in types.keys():
            if types[key]["found"]:
                submit[key] = {"type":key,
                                "value":types[key]["value"],
                                "units":types[key]["units"]}
        if not submit:
            return None
        else:
            return submit

    def add_charge(self, charge):
        if not charge:
            return None
        elif charge.lower().replace('.','') == 'usa' or charge.lower() == 'united states of america':
            report_string = "NO_FIND_CHARGE_{v}".format(v=charge)
            self.missed_parses.append(report_string)
            return charge
        for key in self.charges.keys():
            if key == charge.lower():
                return self.charges.get(key)
            elif key == ' '.join(charge.lower().strip().split()).strip():
                return self.charges.get(key)
            elif key == charge[:-1].lower():
                return self.charges.get(key)
        report_string = "NO_FIND_CHARGE_{v}".format(v=charge)
        if self.write:
            self.charges[charge.lower()] = charge
        else:
            self.missed_parses.append(report_string)
        return charge

    def parse_charges(self, charge_array):
        charges = {}
        last_field = ''
        for obj in charge_array:
            field = obj["field_name_attempt"]
            value = obj["field_value_attempt"]
            if not field:
                continue
            if field != "disposition":
                charges[field] = {}
                charge_type, status = self.classify_charge_type(field.lower().strip())
                keys = []
                for count, line in enumerate(value):
                    key = charge_type+str(count)
                    charges[field][key] = {"type":charge_type,
                                            "status":status,
                                            "label":"",
                                            "counts":None,
                                            "disposition_text":"",
                                            "disposition_details":{}}
                    tokens = line.split()
                    if not tokens:
                        continue
                    if "(" in tokens[-1]:
                        counts = tokens[-1][tokens[-1].rfind('(')+1:].replace(')','').strip()
                        if not counts.isalpha():
                            charges[field][key]["counts"] = counts
                            line = line[:line.rfind("("+counts)]
                    charges[field][key]["label"] = self.add_charge(line.strip())
                    keys.append(key)
            else:
                if value[0] == "":
                    continue
                for count, line in enumerate(value):
                    if not keys or count >= len(keys):
                        new_key = str(field+count)
                        disposition_string = ' '.join(value[count:])
                        charges[last_field][new_key] = {"disposition_text":disposition_string,
                                                        "disposition_details":self.add_disposition_details(disposition_string)}
                        break
                    else:
                        charges[last_field][keys[count]]["disposition_text"] = line
                        charges[last_field][keys[count]]["disposition_details"] = self.add_disposition_details(line)
            last_field = field
        return charges

    def match_designations(self, value):
        for designation in self.des.keys():
            if designation.lower() == value:
                return self.des[designation]["label"]
            if self.des[designation].get("alt_label"):
                for alt in self.des[designation]["alt_label"]:
                    if alt.lower() == value.lower().strip():
                        return self.des[designation]["label"]
        if 'pro se' in value.lower():
            return self.des["PRO SE"]["label"]
        report_string = "NO_FIND_DESIGNATION_{v}".format(v=value)
        self.missed_parses.append(report_string)
        return value.title()

    def parse_reps(self,reps):
        label = "attorney"
        attorneys = {}
        for index in reps:
            rep = reps[index]
            idx = label + str(index)
            attorneys[idx] = {"name":'',"firm":{},"contact":{},"designations":[],"date_terminated":"","case_id":self.new_case_id}
            org = {"name":None,"street_address":None,"city":None,"zip":None}
            orgname = ''
            orgaddress = ''
            see_above = False
            for key in rep.keys():
                if key == "name_attempt":
                    if any(word in rep[key] for word in self.skip_list):
                        break
                    attorneys[idx]["name"] = rep["name_attempt"]
                elif key == "other_fields":
                    for obj in rep[key]:
                        field = obj["field_name_attempt"]
                        if "unsure" in field:
                            field = "unsure"
                        value = obj["field_value_attempt"]
                        if any(word in value for word in self.skip_list):
                            break
                        if field in ["phone","email","fax","prisoner_id"]:
                            attorneys[idx]["contact"][field] = value
                        elif field in ["designation","lead","to_be_noticed","other_italics","pro_se"]:
                            attorneys[idx]["designations"].append(self.match_designations(value.lower().strip()))
                        elif "see above for address" in value.lower():
                            see_above = True
                        elif field == "date_terminated":
                            attorneys[idx]["date_terminated"] = value
                        elif field == "org" or field == 'title' or field == 'district':
                            if not orgname:
                                orgname = value
                            else:
                                orgname += ' ' + value
                        elif field in ["building","id_number","unsure"]:
                            if "unsure" in field and ("law" in value.lower() or "esq" in value.lower()):
                                if not orgname:
                                    orgname = value
                                else:
                                    orgname = value + " " + orgname
                            elif not orgaddress:
                                orgaddress = value
                            else:
                                orgaddress = value + " " + orgaddress
                        elif field == "postal_address":
                            if not orgaddress:
                                orgaddress = value
                            else:
                                orgaddress += ' ' + value
                        elif field in ["city","state","zip",]:
                            org[field] = value
                        elif field == "purview":
                            if not orgname:
                                orgname = value
                            else:
                                orgname = value + " " + orgname
                        else:
                            self.missed_parses.append(obj)
            if see_above:
                attorneys[idx]["firm"] = {"name":"ALREADY LISTED IN THIS DOCKET"}
                continue
            org["name"] = orgname
            org["street_address"] = orgaddress
            if "city" in org.keys() and "state" in org.keys():
                city, state = self.match_cities(org["city"],org["state"])
                org["city"] = city
                org["state"] = state
            attorneys[idx]["firm"] = org
        return attorneys

    def extract_title(self, value):
        title_words = ["Chief","Warden","Principal","Attorney","Deputy Sheriff","Judge",
                       "Detective","Lead","Senior","Assistant","Junior",
                       "Acting","Advocate","Commissioner","President",
                       "Liaison","Bondsman","Public Defender","Counsel","Lieutenant",
                       'Correction Officer','Deputy',"Sheriff","Officer",
                       'Postmaster General','Dr.','Interim','Executive',
                       'Chairman','Chairwoman','Head','Unit Manager','Manager','Esq',
                       'Director','Secretary','Administrator','Leader']
        final_words = ['Department','Organization','Administration','Bureau','Dept',
                       'Union','Force','Group','Agency','Division',
                       'Unit','Squad','Platoon','Penitentiary','Penitentiaries',
                       "Court",]
        for word in title_words:
            if word.lower() in value.lower():
                for addl_word in title_words:
                    if addl_word.lower() in value.lower():
                        test = word + ' ' + addl_word
                        if test.lower() in value.lower():
                            word = test
                for check in [' of ',' for ',',',' in ',' to ',' by ',' with ']:
                    composite = word.lower() + check
                    if composite in value.lower():
                        composite = word + check
                        role = [composite,]
                        end_lowercase = 0
                        loc = value.lower().find(word.lower())
                        search_string = value[loc+len(composite):]
                        for token in search_string.split():
                            if token.istitle():
                                role.append(token)
                                end_lowercase = 0
                            elif token.isupper() and not value.isupper():
                                role.append(token)
                                end_lowercase = 0
                            elif token in ['of','for','by','the','with','in','to']:
                                role.append(token)
                                end_lowercase += 1
                            else:
                                break
                        if end_lowercase:
                            end_lowercase *= -1
                            role = role[:end_lowercase]
                        return ' '.join([x.strip() for x in role])
                for final in final_words:
                    if final.lower() in value.lower():
                        start = value.lower().find(word.lower())
                        end = value.lower().find(final.lower())
                        if start >= end:
                            end = value.lower().rfind(final.lower())
                            if start >= end:
                                continue
                        return value[start:end+len(final)]
                return word
        for word in ['company','corporation','partnership','individual']:
            if word in value.lower():
                a_loc = value.lower().rfind(' a ')
                w_loc = value.lower().rfind(word)
                if a_loc == -1:
                    a_loc = value.lower().rfind(' an ')
                    if a_loc == -1:
                        continue
                return value[a_loc:w_loc+len(word)].strip()
        return None

    def parse_role_title(self,value,name):
        capacity = None
        capacity_phrases = ['not only in h','not only in their','not in h','not in their',
                            'only in h','only in their','in h','in their','NOT ONLY IN H',
                            'NOT ONLY IN THEIR','NOT IN H','NOT IN THEIR','ONLY IN H',
                            'ONLY IN THEIR','IN HIS','IN HER','IN THEIR','capacity',
                            'CAPACITY','Capacity']
        for phrase in capacity_phrases:
            if phrase in value:
                before, the_phrase, after = value.partition(phrase)
                capacity = the_phrase + after
                after_title = self.extract_title(capacity)
                before_title = self.extract_title(before)
                if before_title:
                    if after_title:
                        if len(before_title) < len(after_title):
                            return self.clean_ends(before_title), self.clean_ends(capacity)
                        else:
                            return self.clean_ends(after_title), self.clean_ends(capacity)
                    else:
                        return self.clean_ends(before_title),self.clean_ends(capacity)
                break
        title = self.extract_title(value)
        return self.clean_ends(title), self.clean_ends(capacity)

    def match_role(self,field):
        for key in self.roles.keys():
            if field.title().strip() == key:
                return key
        if self.roles[key].get("alt_label"):
            for alt in self.roles[key]["alt_label"]:
                if field.title().strip() == alt:
                    return key
        for key in self.roles.keys():
            if field.title().strip() in key:
                return key
            elif key in field.title().strip():
                return key
            elif field.replace('-',' ').title().strip() in key:
                return key
            if self.roles[key].get("alt_label"):
                for alt in self.roles[key]["alt_label"]:
                    if field.title().strip() in alt:
                        return key
                    elif field.replace('-',' ').title().strip() in alt:
                        return key
                    elif alt in field.title().strip():
                        return key
                    elif field.lower().strip() in key.lower():
                        return key
                    elif field.lower().strip() in alt:
                        return key
        report_string = "NO_FIND_ROLE"+field
        self.missed_parses.append(report_string)
        return field.title()

    def check_gov(self, name, alt_name, title, reps, capacity):
        sure_gov = ['USA','United States','U.S.','United States of America','Social Security','Postmaster',
                    'Fannie Mae','Freddie Mac','Food and Drug Administration',
                    'Ginnie Mae','Government','Congress','Medicare','Medicaid','City of ',
                    'State of ','Police Dep',]
        gov_words = ['Federal ','Bureau','National','Service','Postal','US',
                    'Warden','Sheriff','Officer','Judge','Deputy','Committee',
                    'Lieutenant','Prison','Board','Agency','Detective','Penal',
                    'Governor','Administration','Commissioner','Mayor','District',
                    'Police','Secretary','Director','Corporal','General','Senator',
                    'Representative','Corps','Army','Navy','Marine','Air Force',
                    'Division','Center','Office','Court','Department','Solicitor',
                    'Dept','Defense','Chair','Chief','Council','Alder','County',]
        certainty = ''
        for word in sure_gov:
            if word in name or word in alt_name or word in title:
                return True
            elif word.lower() in name.lower() or word.lower() in alt_name.lower() or word.lower() in title.lower():
                return True
        for word in gov_words:
            if word in name or word in alt_name or word in title:
                certainty = 'Medium'
            elif word.lower() in name.lower() or word.lower() in alt_name.lower() or word.lower() in title.lower():
                certainty = 'Medium'
        for word in self.state_abbrevs.keys():
            if word in name or word in alt_name or word in title:
                if certainty:
                    return True
                elif word == name:
                    return True
                state = 'state of ' + word.lower()
                if state in name.lower() or state in alt_name.lower() or state in title.lower():
                    return True
                people = 'people of ' + word.lower()
                if people in name.lower() or people in alt_name.lower() or people in title.lower():
                    return True
                commonwealth = 'commonwealth of ' + word.lower()
                if commonwealth in name.lower() or commonwealth in alt_name.lower() or commonwealth in title.lower():
                    return True
                certainty = 'Medium'
            elif word.lower() in name.lower() or word.lower() in alt_name.lower() or word.lower() in title.lower():
                if certainty:
                    return True
                elif word == name:
                    return True
                state = 'state of ' + word.lower()
                if state in name.lower() or state in alt_name.lower() or state in title.lower():
                    return True
                people = 'people of ' + word.lower()
                if people in name.lower() or people in alt_name.lower() or people in title.lower():
                    return True
                commonwealth = 'commonwealth of ' + word.lower()
                if commonwealth in name.lower() or commonwealth in alt_name.lower() or commonwealth in title.lower():
                    return True
                certainty = 'Medium'
        if capacity:
            if 'not ' not in capacity.lower():
                if 'official' in capacity.lower():
                    if certainty:
                        return True
                    else:
                        certainty = 'Medium'
        total = 0
        emails = 0
        dot_gov = 0
        for rep in reps.keys():
            total += 1
            if 'contact' in reps[rep].keys():
                if 'email' in reps[rep]["contact"].keys():
                    emails += 1
                    email = reps[rep]["contact"]["email"]
                    if ".gov" in email.lower():
                        dot_gov += 1
                    elif '.mil' in email.lower():
                        dot_gov += 1
                    elif '.state' in email.lower() and '.us' in email.lower():
                        dot_gov += 1
                    elif "gov.com" in email.lower():
                        dot_gov += 1
        if total == 0 or emails == 0:
            return None
        if dot_gov >= 1:
            ratio = dot_gov / emails
            if certainty == 'Medium':
                if ratio > .3:
                    return True
            elif total >= 2 and ratio > .7:
                return True
        return None

    def clean_ends(self, t):
        if not t:
            return ''
        t = t.strip()
        if t[0] in [',','.','-','(',')','"',':',';','[','<','>',"'",']','*','^','&','%','+',]:
            t = t[1:]
        if not t:
            return ''
        if t[-1] in [',','.','-','(',')','"',':',';','[','<','>',"'",']','*','^','&','%','+',]:
            t = t[:-1]
        t = t.strip()
        return t

    def reconcile_title_overlap(self, n, t, c):
        if t in n:
            loc = n.find(t)
            check_end = loc + len(t) + 1
            if loc == 0 or check_end == len(n):
                n = n.replace(t,'')
                n = self.clean_ends(n)
        if n in t:
            t = t.replace(n,'')
        if c in t:
            t = t.replace(c,'')
        return n.strip(), t.strip(), c.strip()

    def clarify_party_fields(self, names, titles, unsure):
        names = names.split('||')
        if len(names) == 1 and not titles and not unsure:
            t, c = self.parse_role_title(names[0], names[0])
            n, t, c = self.reconcile_title_overlap(names[0],t,c)
        titles = titles.split('||')
        unks = unsure.split('||')
        candidates = []
        unique_values = []
        all = [names, titles, unks]
        for group in all:
            for member in group:
                t, c = self.parse_role_title(member, member)
                if member not in unique_values:
                    unique_values.append(member)
                    if t or c:
                        candidates.append((t,c))
        n = ''
        if names:
            n = names[0]
            for name in names:
                if n in name:
                    continue
                elif name in n:
                    continue
                else:
                    print("NAME CONCAT: {n} and {j}".format(n=n,j=name))
                    n += ' ' + name
        t = ''
        if titles:
            t = titles[0]
        c = ''
        if candidates:
            for tc in candidates:
                title = tc[0]
                capacity = tc[1]
                if title:
                    if not t or len(t) >= len(title):
                        t = title
                if capacity:
                    if not c or len(c) <= len(capacity):
                        c = capacity
        n, t, c = self.reconcile_title_overlap(n, t, c)
        if t and not c:
            title_words = ["Chief","Warden","Principal","Attorney","Deputy Sheriff","Judge",
                           "Detective","Lead","Senior","Assistant","Junior",
                           "Acting","Advocate","Commissioner","President",
                           "Liaison","Bondsman","Public Defender","Counsel","Lieutenant",
                           'Correction Officer','Deputy',"Sheriff","Officer",
                           'Postmaster General','Dr.','Interim','Executive',
                           'Chairman','Chairwoman','Head','Unit Manager','Manager','Esq',
                           'Director','Secretary','Administrator','Leader']
            if any(word in t.lower() for word in [' as ', 'capacity','individually','collectively']):
                c = t
                t = ''
            elif not any(word.lower() in t.lower() for word in title_words):
                for token in t.split():
                    if token.islower() and token not in ['of','and','for','by','the']:
                        c = t
                        t = ''
                        break
        return n, t, c

    def parse_party(self, label, parsed):
        for index in parsed:
            ent = parsed[index]
            idx = label + str(index)
            party = {idx:{"role_id":self.match_role(label),"alias":[],"case_id":self.new_case_id,"name":None}}
            name = ''
            title = ''
            capacity = ''
            unk = ''
            if idx in self.parse_later.keys():
                for obj in self.parse_later[idx]:
                    field = obj["field_name_attempt"]
                    value = obj["field_value_attempt"]
                    if not field:
                        continue
                    elif 'unsure' in field and "unknown" in value.lower():
                        continue
                    elif field == "assigned_to":
                        self.add_judge(field,value,idx)
                    elif field == "referred_to":
                        self.add_judge(field,value,idx)
                    elif "case_number" in field or "other_court" in field or field == "rel_case":
                         self.add_other_case_no(field, value)
                    else:
                        self.missed_parses.append(obj)
            for key in ent.keys():
                if key == "name_attempt":
                    if any(word in ent[key] for word in self.skip_list):
                        break
                    if not name:
                        name = ent[key]
                    else:
                        name += '||' + ent[key]
                    continue
                elif key == "other_fields":
                    for obj in ent[key]:
                        field = obj["field_name_attempt"]
                        value = obj["field_value_attempt"]
                        if any(word in value for word in self.skip_list):
                            break
                        if not field or not value or str(value).strip() == '':
                            continue
                        if field == "role_or_position" or field == "title":
                            if not title:
                                title = value
                            else:
                                title += '||' + value
                            continue
                        elif field == "date_terminated":
                            party[idx]["date_terminated"] = self.make_date(value)
                            continue
                        elif 'unsure' in field and ("unknown" in value.lower() or not value):
                            continue
                        elif field == "alias":
                            party[idx]["alias"].append(value)
                            continue
                        else:
                            if value == name or value == title:
                                continue
                            if not name:
                                name = value
                                continue
                            if not title:
                                title = value
                                continue
                            if unk:
                                unk += '||' + value
                                continue
                            else:
                                unk = value
                                continue
                elif key == "charges":
                    party[idx]["charges"] = self.parse_charges(ent[key])
                elif key == "reps":
                    party[idx]["reps"] = self.parse_reps(ent[key])
                else:
                    self.missed_parses.append(ent)
            if 'reps' not in party[idx].keys():
                reps = {}
            else:
                reps = party[idx]["reps"]
            n, t, c = self.clarify_party_fields(name, title, unk)
            party[idx]["name"] = n
            party[idx]["title"] = t
            party[idx]["capacity"] = c
            party[idx]["is_gov"] = self.check_gov(n,name,t,reps,c)
            self.add_col("Party",idx,party[idx])

    def parse_json(self):
        for field in self.parsed:
            value = self.parsed[field]
            if field == "docket_flags":
                case_no = ''
                if 'Case in other court:' in value:
                    value, field, case_no = value.lower().partition('Case in other court:')
                elif 'rel Case:' in value:
                    value, field, case_no = value.lower().partition('rel Case:')
                elif 'rel case' in value:
                    value, field, case_no = value.lower().partition('rel case')
                if case_no:
                    self.add_other_case_no(field, case_no)
                self.add_col("Case","docket_flags",value)
            elif field == "html":
                self.add_col("DocketHTML","html",value)
            elif field == "addl_docket_fields":
                self.parse_addl_docket_fields(value)
            elif field == "docket_text":
                self.parse_docket_text(value)
            elif field in ["docket_case_id","pacer_receipt","case_id"]:
                continue
            elif field == "URL_path":
                self.tables["DocketHTML"]["html"] = value
                self.tables["DocketHTML"]["parse"] = self.in_dir + '*|*' + self.case_id
            elif field != "missed_parses":
                self.parse_party(field, value)
            else:
                self.missed_parses.append(value)

    def get_html(self):
        with open(self.tables["DocketHTML"]["html"], "rb") as f:
            docket_html = f.read()
        soup = BeautifulSoup(docket_html, 'html.parser')
        self.tables["DocketHTML"]["html"] = str(soup)
        self.tables["DocketHTML"]["parse"] = self.parsed

    def main(self):
        self.parse_json()
        self.report_misses()
        self.get_html()
        district_id = self.set_default_state()
        if self.default_state not in self.tables["Case"].get("district_id",''):
            if district_id:
                self.add_col("Case","district_id",district_id)
            else:
                try:
                    district_labels = ['eastern','middle','western','northern','southern']
                    district_id = ' '.join([district_labels[0].title(),"District of",self.default_state]).strip()
                    district_indices = sorted([(self.tables["DocketHTML"]["html"].lower().find(label),label) for label in district_labels if self.tables["DocketHTML"]["html"].lower().find(label) > -1], key=lambda x: x[0])
                    district_id = ' '.join([district_indices[0][1].title(),"District of",self.default_state])
                    self.add_col("Case","district_id",district_id)
                except:
                    print("Could not find a district.")
        remove = []
        for miss in self.missed_parses:
            if "NO_FIND_MATCH_COUNRT_" in miss:
                remove.append(miss)
            elif "NO_FIND_CIRCUIT_" in miss:
                remove.append(miss)
        for obj in remove:
            try:
                self.missed_parses.remove(obj)
            except:
                continue

if __name__ == "__main__":
    if len(sys.argv) > 1:
        DocketTables(sys.argv[1])
    else:
        DocketTables()
