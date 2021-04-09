# this script bootstraps the noacri db from a variety of sources
# should only be run when you're building a database for the first time

# requires three paths in your env:
# 1) `jumbo` that points to the JumboDB/jumbo dir for imports
# 2) `noacri_judges` that points to a pandas dataframe pickle file for the judge data
# 3) `noacri_dockets` that points to a dir containing a flat list of docket json files

from datetime import datetime, date
import os
import sys
import json
import time
from bs4 import BeautifulSoup
import statistics
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial
sys.path += [os.environ["jumbo"]]

import numpy as np
import pandas as pd

from jumbodb.core import Jumbo

# db stuff
jdb = Jumbo(strictMode=True)
db = jdb.noacri
#js = db.Session()

from sqlalchemy.orm import scoped_session, sessionmaker

from sqlalchemy import create_engine

with open(os.environ["c3creds"]) as json_file:
    data = json.load(json_file)
    jumbo_cred = data["data_sources"]["noacri"]["location"]

ENGINE = create_engine(jumbo_cred, convert_unicode=True)

# source schema
JUDGE_COLS = ['nid', 'jid', 'FullName', 'Last Name', 'First Name', 'Middle Name', 'Suffix', 'Birth Month', 'Birth Day', 'Birth Year', 'Birth City', 'Birth State', 'Death Month', 'Death Day', 'Death Year', 'Death City', 'Death State', 'Gender', 'Race or Ethnicity', 'Court Type (1)', 'Court Name (1)', 'Appointment Title (1)', 'Appointing President (1)', 'Party of Appointing President (1)', 'Reappointing President (1)', 'Party of Reappointing President (1)', 'ABA Rating (1)', 'Seat ID (1)', 'Statute Authorizing New Seat (1)', 'Recess Appointment Date (1)', 'Nomination Date (1)', 'Committee Referral Date (1)', 'Hearing Date (1)', 'Judiciary Committee Action (1)', 'Committee Action Date (1)', 'Senate Vote Type (1)', 'Ayes/Nays (1)', 'Confirmation Date (1)', 'Commission Date (1)', 'Service as Chief Judge, Begin (1)', 'Service as Chief Judge, End (1)', '2nd Service as Chief Judge, Begin (1)', '2nd Service as Chief Judge, End (1)', 'Senior Status Date (1)', 'Termination (1)', 'Termination Date (1)', 'Court Type (2)', 'Court Name (2)', 'Appointment Title (2)', 'Appointing President (2)', 'Party of Appointing President (2)', 'Reappointing President (2)', 'Party of Reappointing President (2)', 'ABA Rating (2)', 'Seat ID (2)', 'Statute Authorizing New Seat (2)', 'Recess Appointment Date (2)', 'Nomination Date (2)', 'Committee Referral Date (2)', 'Hearing Date (2)', 'Judiciary Committee Action (2)', 'Committee Action Date (2)', 'Senate Vote Type (2)', 'Ayes/Nays (2)', 'Confirmation Date (2)', 'Commission Date (2)', 'Service as Chief Judge, Begin (2)', 'Service as Chief Judge, End (2)', '2nd Service as Chief Judge, Begin (2)', '2nd Service as Chief Judge, End (2)', 'Senior Status Date (2)', 'Termination (2)', 'Termination Date (2)', 'Court Type (3)', 'Court Name (3)', 'Appointment Title (3)', 'Appointing President (3)', 'Party of Appointing President (3)', 'Reappointing President (3)', 'Party of Reappointing President (3)', 'ABA Rating (3)', 'Seat ID (3)', 'Statute Authorizing New Seat (3)', 'Recess Appointment Date (3)', 'Nomination Date (3)', 'Committee Referral Date (3)', 'Hearing Date (3)', 'Judiciary Committee Action (3)', 'Committee Action Date (3)', 'Senate Vote Type (3)', 'Ayes/Nays (3)', 'Confirmation Date (3)', 'Commission Date (3)', 'Service as Chief Judge, Begin (3)', 'Service as Chief Judge, End (3)', '2nd Service as Chief Judge, Begin (3)', '2nd Service as Chief Judge, End (3)', 'Senior Status Date (3)', 'Termination (3)', 'Termination Date (3)', 'Court Type (4)', 'Court Name (4)', 'Appointment Title (4)', 'Appointing President (4)', 'Party of Appointing President (4)', 'Reappointing President (4)', 'Party of Reappointing President (4)', 'ABA Rating (4)', 'Seat ID (4)', 'Statute Authorizing New Seat (4)', 'Recess Appointment Date (4)', 'Nomination Date (4)', 'Committee Referral Date (4)', 'Hearing Date (4)', 'Judiciary Committee Action (4)', 'Committee Action Date (4)', 'Senate Vote Type (4)', 'Ayes/Nays (4)', 'Confirmation Date (4)', 'Commission Date (4)', 'Service as Chief Judge, Begin (4)', 'Service as Chief Judge, End (4)', '2nd Service as Chief Judge, Begin (4)', '2nd Service as Chief Judge, End (4)', 'Senior Status Date (4)', 'Termination (4)', 'Termination Date (4)', 'Court Type (5)', 'Court Name (5)', 'Appointment Title (5)', 'Appointing President (5)', 'Party of Appointing President (5)', 'Reappointing President (5)', 'Party of Reappointing President (5)', 'ABA Rating (5)', 'Seat ID (5)', 'Statute Authorizing New Seat (5)', 'Recess Appointment Date (5)', 'Nomination Date (5)', 'Committee Referral Date (5)', 'Hearing Date (5)', 'Judiciary Committee Action (5)', 'Committee Action Date (5)', 'Senate Vote Type (5)', 'Ayes/Nays (5)', 'Confirmation Date (5)', 'Commission Date (5)', 'Service as Chief Judge, Begin (5)', 'Service as Chief Judge, End (5)', '2nd Service as Chief Judge, Begin (5)', '2nd Service as Chief Judge, End (5)', 'Senior Status Date (5)', 'Termination (5)', 'Termination Date (5)', 'Court Type (6)', 'Court Name (6)', 'Appointment Title (6)', 'Appointing President (6)', 'Party of Appointing President (6)', 'Reappointing President (6)', 'Party of Reappointing President (6)', 'ABA Rating (6)', 'Seat ID (6)', 'Statute Authorizing New Seat (6)', 'Recess Appointment Date (6)', 'Nomination Date (6)', 'Committee Referral Date (6)', 'Hearing Date (6)', 'Judiciary Committee Action (6)', 'Committee Action Date (6)', 'Senate Vote Type (6)', 'Ayes/Nays (6)', 'Confirmation Date (6)', 'Commission Date (6)', 'Service as Chief Judge, Begin (6)', 'Service as Chief Judge, End (6)', '2nd Service as Chief Judge, Begin (6)', '2nd Service as Chief Judge, End (6)', 'Senior Status Date (6)', 'Termination (6)', 'Termination Date (6)','Other Federal Judicial Service (1)', 'Other Federal Judicial Service (2)', 'Other Federal Judicial Service (3)', 'Other Federal Judicial Service (4)', 'School (1)', 'Degree (1)', 'Degree Year (1)', 'School (2)', 'Degree (2)', 'Degree Year (2)', 'School (3)', 'Degree (3)', 'Degree Year (3)', 'School (4)', 'Degree (4)', 'Degree Year (4)', 'School (5)', 'Degree (5)', 'Degree Year (5)', 'Professional Career', 'Other Nominations/Recess Appointments']

# keys associated with appointments
APPTKEYS = ["Court Type", "Court Name", "Appointment Title", "Appointing President", "Party of Appointing President", "Reappointing President", "Party of Reappointing President", "ABA Rating", "Seat ID", "Statute Authorizing New Seat", "Recess Appointment Date", "Nomination Date", "Committee Referral Date", "Hearing Date", "Judiciary Committee Action", "Committee Action Date", "Senate Vote Type", "Ayes/Nays", "Confirmation Date", "Commission Date", "Service as Chief Judge, Begin", "Service as Chief Judge, End", "2nd Service as Chief Judge, Begin", "2nd Service as Chief Judge, End", "Senior Status Date", "Termination", "Termination Date"]

# keys that *should* be dates (these are gory/all over the place and need subsequent ETL work)
DATEKEYS = ["Recess Appointment Date", "Nomination Date", "Committee Referral Date", "Hearing Date", "Committee Action Date", "Confirmation Date", "Commission Date", "Service as Chief Judge, Begin", "Service as Chief Judge, End", "2nd Service as Chief Judge, Begin", "2nd Service as Chief Judge, End", "Senior Status Date", "Termination Date"]

# a helper function for dev time -- gives some sense of just wtf is going on with the dates in those DATEKEYS above
def dateTests():
    for i in range(1, 7):
        for dk in DATEKEYS:
            thiscol = dk+" ("+str(i)+")"
            vals = jdf.loc[jdf[thiscol].notnull()][thiscol].tolist()
            print("=======")
            print(thiscol)
            if len(vals) > 0:
                print(vals)
                print(vals[0])
            print(set([type(val) for val in vals]))
            print("=======")


#
#
# some general helper functions
def safeVal(val):
    if (type(val) in [int, float] and np.isnan(val)) or (type(val) == str and not val.strip()):
        return None
    return val

def safeDate(y, m, d):
    month = 1 if not safeVal(m) else int(safeVal(m))
    day = 1 if not safeVal(d) else int(safeVal(d))
    year = safeVal(y)
    # breakpoint()
    if not year:
        # must be a None
        return None
    try:
        year = int(year)
    except:
        year = int(year.replace("ca. ", ""))
    try:
        output = date(year, month, day)
    except:
        breakpoint()
    return output

def invalidDay(d, y):
    return not safeVal(d) and safeVal(y) is not None

def altSafeDate(datestr):
    # TODO: write the keyring of parsers necessary to get this into python Date objects
    # for now, we'll treat them as strings as it's unclear if this will be wildly useful
    # also, we don't want to mock in info if we only have dates...
    return safeVal(datestr)

def make_session():
    return scoped_session(sessionmaker(autocommit=False,
                                             autoflush=False,
                                             bind=ENGINE))

def show_progress(count, length):
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
    sys.stdout.write('\r|{b}| {p}          {f}\r'.format(b=progress_bar,p=prog,f=big))
    sys.stdout.flush()

#
#
# the actual plumbing
# writeJudges is the main function to call to do everything
verbose = False
def writeJudges():
    print("========\nLoading up all the federal judge data...\n========\n")
    # raw sources
    js = make_session()
    judgescsv = os.environ.get("noacri_judges")
    jdf = pd.read_csv(judgescsv)
    length = len(jdf.index)
    start = time.time()
    for count, jrow in enumerate(jdf.itertuples()):
        storedJudge = writeJudge(js, jrow)
        addAppts(js, jrow, storedJudge)
        writeAltFederalService(js, jrow, storedJudge)
        writeSchools(js, jrow, storedJudge)
        show_progress(count, length)
    duration = round(time.time() - start, 1)
    js.remove()
    print("\n========\nDone with {c} federal judges in {t}s...\n========".format(c=count,t=duration))

def writeJudge(js, jrow):
    if verbose:
        print("\n---> Writing judge with id: %s" % jrow[1])
    # do we already have this judge?
    storedJudge = js.query(db.Judge).filter_by(nid=jrow[1]).first()
    if not storedJudge:
        try:
            storedJudge = db.Judge(
                nid = safeVal(jrow[1]),
                jid = safeVal(jrow[2]),
                first_name = safeVal(jrow[5]),
                middle_name = safeVal(jrow[6]),
                last_name = safeVal(jrow[4]),
                suffix = safeVal(jrow[7]),
                birthday = safeDate(jrow[10], jrow[8], jrow[9]),
                birthday_year_only = invalidDay(jrow[9], jrow[10]),
                birth_city = safeVal(jrow[11]),
                birth_state = safeVal(jrow[12]),
                deathday = safeDate(jrow[15], jrow[13], jrow[14]),
                deathday_year_only = invalidDay(jrow[14], jrow[15]),
                death_city = safeVal(jrow[16]),
                death_state = safeVal(jrow[17]),
                gender = safeVal(jrow[18]),
                race_or_ethnicity = safeVal(jrow[19]),
                professional_career = safeVal(jrow[201]),
                other_nominations_or_recess_appointments = safeVal(jrow[202]))
            js.add(storedJudge)
            js.commit()
        except:
            breakpoint()
    return storedJudge

def getApptVals(jrow, idx=1):
    output = {
        "judge_nid": jrow[1],
        "appointment_number": idx
    }
    for appt in APPTKEYS:
        matches = [col for col in JUDGE_COLS if appt in col and "("+str(idx)+")" in col]
        targetidx = JUDGE_COLS.index(matches[0]) + 1
        output[appt] = jrow[targetidx]
    return output

def addAppts(js, jrow, judge):
    if verbose:
        print("Writing their appointments...")
    for i in range(1,7):
        apptVals = getApptVals(jrow, i)
        writeAppt(js, apptVals, judge)

def writeAppt(js, appt, judge):
    if not safeVal(appt["Court Type"]) and not safeVal(appt["Appointment Title"]):
        # this is likely a null entry...
        return False
    # or do we already have this one?
    storedAppt = js.query(db.Appointment)\
      .filter_by(judge_nid=appt["judge_nid"])\
      .filter_by(appointment_number=appt["appointment_number"])\
      .first()
    # breakpoint()
    if not storedAppt:
        newa = db.Appointment(
            judge_nid = safeVal(appt["judge_nid"]),
            appointment_number = safeVal(appt["appointment_number"]),
            court_type = safeVal(appt["Court Type"]),
            court_name = safeVal(appt["Court Name"]),
            appointment_title = safeVal(appt["Appointment Title"]),
            appointing_president = safeVal(appt["Appointing President"]),
            appointing_president_party = safeVal(appt["Party of Appointing President"]),
            reappointing_president = safeVal(appt["Reappointing President"]),
            reappointing_president_party = safeVal(appt["Party of Reappointing President"]),
            aba_rating = safeVal(appt["ABA Rating"]),
            seat_id = safeVal(appt["Seat ID"]),
            statute_authorizing_new_seat = safeVal(appt["Statute Authorizing New Seat"]),
            recess_appointment_date = altSafeDate(appt["Recess Appointment Date"]),
            nomination_date = altSafeDate(appt["Nomination Date"]),
            committee_referral_date = altSafeDate(appt["Committee Referral Date"]),
            hearing_date = altSafeDate(appt["Hearing Date"]),
            judiciary_committee_action = safeVal(appt["Judiciary Committee Action"]),
            committee_action_date = altSafeDate(appt["Committee Action Date"]),
            senate_vote_type = safeVal(appt["Senate Vote Type"]),
            ayes_nayes = safeVal(appt["Ayes/Nays"]),
            confirmation_date = altSafeDate(appt["Confirmation Date"]),
            commission_date = altSafeDate(appt["Commission Date"]),
            service_as_chief_judge_begin = safeVal(appt["Service as Chief Judge, Begin"]),
            service_as_chief_judge_end = safeVal(appt["Service as Chief Judge, End"]),
            second_service_as_chief_judge_begin = safeVal(appt["2nd Service as Chief Judge, Begin"]),
            second_service_as_chief_judge_end = safeVal(appt["2nd Service as Chief Judge, End"]),
            senior_status_date = safeVal(appt["Senior Status Date"]),
            termination_reason = safeVal(appt["Termination"]),
            termination_date = safeVal(appt["Termination Date"]))
        judge.appointments.append(newa)
        js.add(newa)
        js.commit()
        return newa

def writeAltFederalService(js, jrow, judge):
    if verbose:
        print("Writing their other federal service...")
    for i in range(181, 185):
        x = i - 180
        service = jrow[i]
        if not safeVal(service):
            continue
        # or do we already have this one?
        storedService = js.query(db.AltFederalService)\
          .filter_by(judge_nid=safeVal(jrow[1]))\
          .filter_by(appointment_number=x)\
          .first()
        if not storedService:
            news = db.AltFederalService(
                judge_nid=safeVal(jrow[1]),
                appointment_number=x,
                service=safeVal(service)
            )
            judge.other_federal_judicial_service.append(news)
            js.add(news)
            js.commit()

def writeSchools(js, jrow, judge):
    if verbose:
        print("Writing their school info...")
    # [185, 188, 191, 194, 197]
    for i in range(0, 5):
        idx = i+1
        x = (3*i) + 185
        school = jrow[x]
        if not safeVal(school):
            continue
        storedSchool = js.query(db.School)\
          .filter_by(judge_nid=safeVal(jrow[1]))\
          .filter_by(attendance_sequence=idx)\
          .first()
        if not storedSchool:
            degree = jrow[x+1]
            degree_year = jrow[x+2]
            news = db.School(
                judge_nid=safeVal(jrow[1]),
                attendance_sequence=idx,
                school=safeVal(school),
                degree=safeVal(degree),
                degree_year=safeVal(degree_year)
            )
            judge.schools.append(news)
            js.add(news)
            js.commit()

class DocketMetadataTables:

    def __init__(self, path_to_tables_dir):
        self.path = path_to_tables_dir
        self.shared_tables = {}
        self.start = time.time()

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
        # duration = round(time.time() - self.start,1)
        big = width * ' '
        # if duration > 0:
        #     rate = round(count / duration,1)
        # else:
        #     rate = '∞'
        # sys.stdout.write('\r|{b}| {p}% \t{r} x/s \t in {t}s \t{f}\r'.format(b=progress_bar,p=prog,r=rate,t=duration,f=big))
        sys.stdout.write('\r|{b}| {p} \t{f}\r'.format(b=progress_bar,p=prog, f=big))
        sys.stdout.flush()

    def get_session(self):
        return scoped_session(sessionmaker(autocommit=False,
                                                 autoflush=False,
                                                 bind=ENGINE))

    def load(self):
        if os.path.isdir(self.path):
            for (dirpath, dirnames, filenames) in os.walk(self.path):
                for filename in filenames:
                    if filename.endswith('.json'):
                        full_filename = str(os.path.join(dirpath, filename))
                        if 'charges' in full_filename:
                            with open(full_filename, "r", encoding="utf-8") as f:
                                self.shared_tables["charges"] = json.load(f)
                        elif 'roles' in full_filename:
                            with open(full_filename, "r", encoding="utf-8") as f:
                                self.shared_tables["roles"] = json.load(f)
                        elif 'designations' in full_filename:
                            with open(full_filename, "r", encoding="utf-8") as f:
                                self.shared_tables["designations"] = json.load(f)
                        elif 'nos' in full_filename:
                            with open(full_filename, "r", encoding="utf-8") as f:
                                self.shared_tables["nos"] = json.load(f)
                        elif 'districts' in full_filename:
                            with open(full_filename, "r", encoding="utf-8") as f:
                                self.shared_tables["districts"] = json.load(f)
                        elif 'circuits' in full_filename:
                            with open(full_filename, "r", encoding="utf-8") as f:
                                self.shared_tables["circuits"] = json.load(f)
                        elif 'cities' in full_filename:
                            with open(full_filename, "r", encoding="utf-8") as f:
                                self.shared_tables["cities"] = json.load(f)
                        elif 'd_labels' in full_filename:
                            with open(full_filename, "r", encoding="utf-8") as f:
                                self.shared_tables["d_labels"] = json.load(f)
                        elif 'motion_tags' in full_filename:
                            with open(full_filename, "r", encoding="utf-8") as f:
                                self.shared_tables["tags"] = json.load(f)

    def write(self):
        self.write_charges()
        self.write_roles()
        self.write_designations()
        self.write_disposition_labels()
        self.write_nos()
        self.write_cities()
        self.write_circuits()
        self.write_districts()
        self.write_tags()

    def get_stats(self, count):
        duration = round(time.time() - self.start,1)
        if duration > 0:
            rate = round(count / duration, 1)
        else:
            rate = '∞'
        return duration, rate

    def store_charge(self, js, charge):
        storedCharge = db.ChargeLabel(
            label = safeVal(charge)
        )
        js.add(storedCharge)
        js.commit()
        return storedCharge

    def write_charge(self, js, charge):
        storedCharge = js.query(db.ChargeLabel).filter_by(label=charge).first()
        # do we already have this charge?
        if not storedCharge:
            storedCharge = self.store_charge(js, charge)
        return None

    def write_charges(self):
        print("\n---> Writing charges...")
        length = len(self.shared_tables["charges"].keys())
        self.start = time.time()
        js = self.get_session()
        for count, key in enumerate(self.shared_tables["charges"].keys()):
            self.write_charge(js, self.shared_tables["charges"][key])
            self.progress(count, length)
        js.remove()
        duration, rate = self.get_stats(count)
        print("\nFinished writing {c} charges in {t}s, at {r} writes/s".format(c=count,t=duration,r=rate))

    def store_role(self, js, role):
        storedRole = db.Role(
            label = safeVal(role)
        )
        js.add(storedRole)
        js.commit()
        return storedRole

    def write_role(self, js, role):
        storedRole = js.query(db.Role).filter_by(label=role).first()
        # do we already have this role?
        if not storedRole:
            storedRole = self.store_role(js, role)
        return None

    def write_roles(self):
        print("\n---> Writing roles...")
        length = len(self.shared_tables["roles"].keys())
        self.start = time.time()
        js = self.get_session()
        for count, key in enumerate(self.shared_tables["roles"].keys()):
            self.write_role(js, self.shared_tables["roles"][key]["label"])
            self.progress(count, length)
        js.remove()
        duration, rate = self.get_stats(count)
        print("\nFinished writing {c} roles in {t}s, at {r} writes/s".format(c=count,t=duration,r=rate))

    def store_designation(self, js, designation):
        storedDesignation = db.Designation(
            label = safeVal(designation)
        )
        js.add(storedDesignation)
        js.commit()
        return storedDesignation

    def write_designation(self, js, designation):
        storedDesignation = js.query(db.Designation).filter_by(label=designation).first()
        # do we already have this designation?
        if not storedDesignation:
            storedDesignation = self.store_designation(js, designation)
        return None

    def write_designations(self):
        print("\n---> Writing designations...")
        length = len(self.shared_tables["designations"].keys())
        self.start = time.time()
        js = self.get_session()
        for count, key in enumerate(self.shared_tables["designations"].keys()):
            self.write_designation(js, self.shared_tables["designations"][key]["label"])
            self.progress(count, length)
        js.remove()
        duration, rate = self.get_stats(count)
        print("\nFinished writing {c} designations in {t}s, at {r} writes/s".format(c=count,t=duration,r=rate))

    def store_disposition_label(self, js, label):
        storedDisposition = db.DispositionLabel(
            label = safeVal(label)
        )
        js.add(storedDisposition)
        js.commit()
        return storedDisposition

    def write_disposition_label(self, js, label):
        storedDisposition = js.query(db.DispositionLabel).filter_by(label=label).first()
        # do we already have this disposition label?
        if not storedDisposition:
            storedDisposition = self.store_disposition_label(js, label)
        return None

    def write_disposition_labels(self):
        print("\n---> Writing disposition labels...")
        length = len(self.shared_tables["d_labels"]["labels"])
        self.start = time.time()
        js = self.get_session()
        for count, label in enumerate(self.shared_tables["d_labels"]["labels"]):
            self.write_disposition_label(js, label)
            self.progress(count, length)
        js.remove()
        duration, rate = self.get_stats(count)
        print("\nFinished writing {c} disposition labels in {t}s, at {r} writes/s".format(c=count,t=duration,r=rate))

    def store_nos(self, js, code, label, category):
        storedNos = db.NatureOfSuit(
            code = safeVal(code),
            label = safeVal(label),
            category = safeVal(category)
        )
        js.add(storedNos)
        js.commit()
        return storedNos

    def write_a_nos(self, js, nos_obj):
        nos_code = nos_obj["code"]
        storedNos = js.query(db.NatureOfSuit).filter_by(code=nos_code).first()
        if not storedNos:
            nos_label = nos_obj["label"]
            nos_category = nos_obj["category"]
            storedNos = self.store_nos(js, nos_code, nos_label, nos_category)
        return None

    def write_nos(self):
        print("\n---> Writing natures of suit...")
        length = len(self.shared_tables["nos"].keys())
        self.start = time.time()
        js = self.get_session()
        for count, key in enumerate(self.shared_tables["nos"].keys()):
            self.write_a_nos(js, self.shared_tables["nos"][key])
            self.progress(count, length)
        js.remove()
        duration, rate = self.get_stats(count)
        print("\nFinished writing {c} natures of suit in {t}s, at {r} writes/s".format(c=count,t=duration,r=rate))

    def store_city(self, js, city, state):
        storedCity = db.City(
            city = safeVal(city),
            state = safeVal(state)
        )
        js.add(storedCity)
        js.commit()
        return storedCity

    def write_city(self, js, city_obj):
        city_label = city_obj["city"]
        city_state = city_obj["state"]
        storedCity = js.query(db.City)\
                        .filter_by(city=city_label)\
                        .filter_by(state=city_state)\
                        .first()
        # do we already have this city?
        if not storedCity:
            storedCity = self.store_city(js, city_label, city_state)
        return None

    def write_cities(self):
        print("\n---> Writing cities...")
        length = len(self.shared_tables["cities"].keys())
        self.start = time.time()
        js = self.get_session()
        for count, key in enumerate(self.shared_tables["cities"].keys()):
            self.write_city(js, self.shared_tables["cities"][key])
            self.progress(count, length)
        js.remove()
        duration, rate = self.get_stats(count)
        print("\nFinished writing {c} cities in {t}s, at {r} writes/s".format(c=count,t=duration,r=rate))

    def store_circuit(self, js, label, full, abbrev, city):
        storedCircuit = db.CircuitCourt(
                    label = safeVal(label),
                    full_label = safeVal(full),
                    abbreviation = safeVal(abbrev),
                    primary_city_id = safeVal(city.id)
                )
        js.add(storedCircuit)
        js.commit()
        return storedCircuit

    def write_circuit(self, js, obj):
        storedCircuit = js.query(db.CircuitCourt).filter_by(label=obj["circuit"]).first()
        if not storedCircuit:
            city = js.query(db.City)\
                            .filter_by(city=obj["city"])\
                            .filter_by(state=obj["state"])\
                            .first()
            storedCircuit = self.store_circuit(js, obj["circuit"], obj["full_name"], obj["abbreviation"], city)
            storedCircuit.primary_city.append(city)
            city.circuit.append(storedCircuit)

    def write_circuits(self):
        print("\n---> Writing circuits...")
        length = len(self.shared_tables["circuits"]["rows"])
        self.start = time.time()
        js = self.get_session()
        for count, obj in enumerate(self.shared_tables["circuits"]["rows"]):
            self.write_circuit(js, obj)
            self.progress(count, length)
        js.remove()
        duration, rate = self.get_stats(count)
        print("\nFinished writing {c} circuits in {t}s, at {r} writes/s".format(c=count,t=duration,r=rate))
        return None

    def store_district(self, js, d, f, a, circuit, city):
        storedDistrict = db.DistrictCourt(
            label = safeVal(d),
            full_label = safeVal(f),
            abbreviation = safeVal(a),
            circuit_id = safeVal(circuit.id),
            primary_city_id = safeVal(city.id)
        )
        js.add(storedDistrict)
        js.commit()
        return storedDistrict

    def write_district(self, js, obj):
        storedDistrict = js.query(db.DistrictCourt).filter_by(label=obj["district"]).first()
        if not storedDistrict:
            city = js.query(db.City)\
                            .filter_by(city=obj["city"])\
                            .filter_by(state=obj["state"])\
                            .first()
            circuit = js.query(db.CircuitCourt).filter_by(label=obj["circuit_court"]).first()
            storedDistrict = self.store_district(js, obj["district"], obj["full_name"], obj["abbreviation"], circuit, city)
            storedDistrict.primary_city.append(city)
            city.district.append(storedDistrict)
            storedDistrict.circuit.append(circuit)
            circuit.districts.append(storedDistrict)
        return None

    def write_districts(self):
        print("\n---> Writing districts...")
        length = len(self.shared_tables["districts"]["rows"])
        self.start = time.time()
        js = self.get_session()
        for count, obj in enumerate(self.shared_tables["districts"]["rows"]):
            self.write_district(js, obj)
            self.progress(count, length)
        js.remove()
        duration, rate = self.get_stats(count)
        print("\nFinished writing {c} districts in {t}s, at {r} writes/s".format(c=count,t=duration,r=rate))

    def store_tag(self, js, category, label, count):
        storedTag = db.Tag(
            category = safeVal(category),
            label = safeVal(label),
            count = safeVal(count)
        )
        js.add(storedTag)
        js.commit()
        return storedTag

    def write_tag(self, js, obj):
        category = obj["category"]
        label = obj["label"]
        storedTag = js.query(db.Tag).filter_by(category=category).filter_by(label=label).first()
        if not storedTag:
            count = obj["count"]
            storedTag = self.store_tag(js, category, label, count)
        return None

    def write_tags(self):
        print("\n---> Writing tags...")
        length = len(self.shared_tables["tags"])
        self.start = time.time()
        js = self.get_session()
        for count, obj in enumerate(self.shared_tables["tags"].values()):
            self.write_tag(js, obj)
            self.progress(count, length)
        js.remove()
        duration, rate = self.get_stats(count)
        print("\nFinished writing {c} tags in {t}s, at {r} writes/s".format(c=count,t=duration,r=rate))

def writeDocketMetadataTables():
    print("\n========\nLoading up all the metadata tables...\n========")
    tables = DocketMetadataTables(os.environ.get(["noacri_metadata_tables"]))
    tables.load()
    tables.write()
    print("\n========\nDone with the metadata...\n========\n")

class DocketTables:

    def __init__(self, path_to_dockets_dir):
        self.path = path_to_dockets_dir
        self.parse_dir = os.environ.get("pipeline_dockets")
        self.json_queue = []
        self.start_docket = time.time()
        self.total_dockets_processed = 0
        self.original_parse_index = {}
        self.parsed = {}
        self.done = []
        self.length = 0
        self.original_parse_index = ''
        self.done_times = []

    def get_session(self):
        return scoped_session(sessionmaker(autocommit=False,
                                                 autoflush=False,
                                                 bind=ENGINE))

    def progress(self):
        # drawn from https://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console/27871113
        self.total_dockets_processed += 1
        width = 60
        if self.length > 0:
            completion = float(self.total_dockets_processed / self.length)
        else:
            completion = float(1)
        filled = int(width * completion)
        prog = round(100 * completion,1)
        prog = str(prog) + "%" + ' ' * (5 - len(str(prog)))
        progress_bar = '█' * filled + '-' * (width - filled)
        rate = 0.0
        if self.done_times:
            rate = round(statistics.mean(self.done_times),1)
        # now = round(time.time() - self.start_all,1)
        big = width//2 * ' '
        sys.stdout.write('\r|{b}| {p} \t{r}s/docket\t{t} d\t{f}\r'.format(b=progress_bar, p=prog, r=rate, t=self.total_dockets_processed, f=big))
        sys.stdout.flush()

    def get_parse(self, filename, check=False):
        # self.origin_filename = filename
        self.parse_stub = filename.partition(self.path)[2].partition('_PARSED')[0]
        # parse = parse_dir + parse_str + .json
        # filename = filename.partition("parsed_dockets")[2].replace('/noacri_dockets/','./pipeline/').replace('_PARSED.json','.json')
        # if not filename or "pipeline" not in filename:
        #     filename = self.origin_filename
        #     filename = filename.replace('noacri_dockets','pipeline').replace('_PARSED.json','.json')
        filename = self.parse_dir + self.parse_stub + ".json"
        if not check:
            with open(filename, "r") as f:
                self.parsed = json.load(f)
            self.filename = filename
        else:
            parse = {}
            with open(filename, "r") as f:
                parse = json.load(f)
            if check in parse.keys():
                return parse[check]
            return False

    def load(self):
        skips = 0
        if os.path.isdir(self.path):
            for (dirpath, dirnames, filenames) in os.walk(self.path):
                for filename in filenames:
                    if filename.endswith('.json'):
                        full_filename = str(os.path.join(dirpath, filename))
                        if '_PARSED' in full_filename and full_filename not in self.done:
                            self.json_queue.append(os.path.join(dirpath, filename))
                            self.length += 1 #int(full_filename.rpartition("_dockets")[0].rpartition('/')[2])
                        elif full_filename in self.done:
                            skips += 1
                            # print("Skipping {c}...".format(c=full_filename))
        print("Loaded {c} files, skipped {s} files.".format(c=len(self.json_queue),s=skips))

    def load_dockets(self, file):
        with open(file, "r", encoding="utf-8") as f:
            return json.load(f)

    def load_completes(self, file):
        try:
            self.done = self.load_dockets(file)["done"]
        except:
            self.done = []

    def save_completes(self, file):
        d = {"done":self.done}
        with open(file, "w") as f:
            json.dump(d, f, ensure_ascii=False, indent=4)

    def retrieve_parse(self, id):
        try:
            return self.parsed[id]
        except:
            if not self.original_parse_index:
                filename = "./id_matching_table.json"
                self.original_parse_index = self.load_dockets(filename)
            filename = self.original_parse_index.get(id)
            if not filename:
                fail = 1/0
            with open(filename, "r") as f:
                container = json.load(f)
            return container[id]
            # current_index = self.file_count + 1
            # for file in self.json_queue:
            #     parse = self.get_parse(file, id)
            #     if parse:
            #         return parse
            # for file in self.json_queue[current_index:]:
            #     parse = self.get_parse(file, id)
            #     if parse:
            #         return parse
            # for file in self.json_queue[:current_index]:
            #     parse = self.get_parse(file, id)
            #     if parse:
            #         return parse
            # main = "/Users/harper/Documents/nu_work/c3/c3-JumboDB/jumbodb/sources/noacri/import/r2/noacri_dockets/"
            # for file in ["150_dockets_20200506_13_of_17_PARSED.json",
            #              "150_dockets_20200506_16_of_17_PARSED.json",
            #              "150_dockets_20200506_12_of_17_PARSED.json",
            #              "150_dockets_20200506_11_of_17_PARSED.json",
            #              "150_dockets_20200506_14_of_17_PARSED.json",
            #              "150_dockets_20200506_8_of_17_PARSED.json",
            #              "150_dockets_20200506_10_of_17_PARSED.json",
            #              "150_dockets_20200506_15_of_17_PARSED.json",
            #              "150_dockets_20200506_9_of_17_PARSED.json",
            #              "150_dockets_20200506_5_of_17_PARSED.json",
            #              "150_dockets_20200506_1_of_17_PARSED.json",
            #              "150_dockets_20200506_4_of_17_PARSED.json",
            #              "150_dockets_20200506_2_of_17_PARSED.json",
            #              "150_dockets_20200506_7_of_17_PARSED.json",
            #              "150_dockets_20200506_3_of_17_PARSED.json"]:
            #     file = main + file
            #     parse = self.get_parse(file, id)
            #     if parse:
            #         return parse
            # fail = 1/0

    def write_docket(self, docket):
        js = self.get_session()
        try:
            # id_str = docket["DocketHTML"]["parse"].partition('*|*')[2]
            # parse = self.retrieve_parse(id_str)
            trunk = DBIngestion()
            new = trunk.write_docket(js, docket)
            if new:
                self.done_times.append(new)
                test = time.time()
                js.commit()
                commit_time = round(time.time() - test,2)
                print("\nCommit took {c}s.".format(c=commit_time))
        except Exception as e:
            print("Failure with {e} for case {c} in {f}".format(e=e,c=docket["Case"].get("case_id"),f=self.json_queue[self.file_count]))
        js.remove()
        self.progress()
        return None

    def write(self):
        self.start_all = time.time()
        for file_count, file in enumerate(reversed(self.json_queue)):
            # if file_count < 10:
            #     continue
            self.file_count = file_count
            start = time.time()
            dockets_json = self.load_dockets(file)
            load_time = round(time.time() - start,2)
            #print("\nLoad took {l}s.".format(l=load_time))
            #self.get_parse(file)
            self.write_docket(dockets_json)
            # for value in dockets_json.values():
            #     self.write_docket(value)
            # pool = ThreadPool(4)
            # results = pool.map(self.write_docket, dockets_json.values())
            self.done.append(file)
            #self.save_completes("./done.json")
        process_time = round(time.time() - self.start_all,2)
        if process_time > 0:
            rate = self.total_dockets_processed / process_time
        else:
            rate = '∞'
        print("\nWrote {c} dockets at {r} dockets/s.".format(c=self.total_dockets_processed,r=rate))
        print("\n...fin")


class DBIngestion:

    def __init__(self):
        self.html_url = os.environ.get("html_dockets")
        #self.write_docket(js, docket, parse)

    def load_dockets(self, file):
        with open(file, "r", encoding="utf-8") as f:
            return json.load(f)

    def already_written(self, js, case_id):
        storedHTML = js.query(db.DocketHTML).filter_by(case_id=case_id).first()
        if storedHTML:
            return True
        return False

    def ensure_correct_case_type(self, case):
        # 1 = pull case
        # 2 = extract case_type
        if case.case_type:
            case_type = case.case_type
        else:
            case_type = ''
        # 3 = extract docket_html
        if case.docket_html:
            html = case.docket_html[0].html
        else:
            html = ''
        if "CIVIL DOCKET FOR CASE" in html and case_type == "civil":
            pass
        elif "CRIMINAL DOCKET FOR CASE" in html and case_type == "criminal":
            pass
        elif case_type == "civil" and ("CIVIL DOCKET" in html or "Civil Docket" in html or "civil docket" in html):
            pass
        elif case_type == "criminal" and ("CRIMINAL DOCKET" in html or "Criminal Docket" in html or "criminal docket" in html):
            pass
        elif case_type == "civil" and "CRIMINAL DOCKET FOR CASE" in html:
            print("Changing {c} from civil to criminal.".format(c=case.case_id))
            case.case_type = "criminal"
        elif case_type == "criminal" and "CIVIL DOCKET FOR CASE" in html:
            case.case_type = "civil"
        else:
            pass
        return case

    def misc_prep(self, url):
        pass
        #self.dist_stub = "District of Arizona"
        #html_id_and_suffix = '/' + url.rpartition("/")[2]
        #self.dist_stub = url.rpartition(html_id_and_suffix)[0].rpartition('/')[2]
        # stub = url.partition("docket_parsing/")[2].partition("/")[2]
        # self.dist_stub = stub.partition("/")[0]
        #self.html_url += '/' + self.dist_stub + html_id_and_suffix
        # self.ingest_path = os.get(noacri_dockets) + parse_label + finish_stamp + parse_suffix
        # self.parse_path = os.get(pipeline_dockets) + parse_label + parse_suffix
        # self.html_path = os.get(docket_html) + html_cat + html_id + html_suffix
        # path to ingestion directory
        ## /home/c3lab/c3-JumboDB/jumbodb/sources/noacri/import/noacri_dockets
        # path to pipeline directory
        ## /Users/harper/Documents/nu_work/c3/c3-JumboDB/jumbodb/sources/noacri/import/pipeline
        # path to html directory
        ## /c3-JumboDB/jumbodb/sources/noacri/import/DONE_pipeline
        # html_id
        ## 4-16-cr-00230-GRS
        # html cat
        ## il_n
        # html Suffix
        ## .html
        # parse label
        ## 150_dockets_20200315_5_of_749
        # parse suffix
        ## .json
        # finish stamp
        ## _PARSED

    def get_dist_stub(self):
        if docket["Case"].get("district_id",""):
            return docket["Case"].get("district_id","")
        else:
            states = {'Alabama': 'AL','Alaska': 'AK','Arizona': 'AZ','Arkansas': 'AR',
                      'California': 'CA','Colorado': 'CO','Columbia': 'DC','Connecticut': 'CT',
                      'Delaware': 'DE','District of Columbia': 'DC','D.C.': 'DC',
                      'Florida': 'FL','Georgia': 'GA','Guam': 'GU','Hawaii': 'HI',
                      'Idaho': 'ID','Illinois': 'IL','Indiana': 'IN','Iowa': 'IA',
                      'Kansas': 'KS','Kentucky': 'KY','Louisiana': 'LA','Maine': 'ME',
                      'Maryland': 'MD','Massachusetts': 'MA','Michigan': 'MI','Minnesota': 'MN',
                      'Mississippi': 'MS','Missouri': 'MO','Montana': 'MT','Nebraska': 'NE',
                      'Nevada': 'NV','New Hampshire': 'NH','New Jersey': 'NJ','New Mexico': 'NM',
                      'New York': 'NY','North Carolina': 'NC','North Dakota': 'ND','Northern Mariana Islands':'MP',
                      'Ohio': 'OH','Oklahoma': 'OK','Oregon': 'OR','Palau': 'PW','Pennsylvania': 'PA',
                      'Puerto Rico': 'PR','Rhode Island': 'RI','South Carolina': 'SC','South Dakota': 'SD',
                      'Tennessee': 'TN','Texas': 'TX','Utah': 'UT','Vermont': 'VT','Virgin Islands': 'VI',
                      'Virginia': 'VA','Washington': 'WA','West Virginia': 'WV','Wisconsin': 'WI','Wyoming': 'WY',
                    }
            counts = {}
            for state in states.keys():
                index = docket["DocketHTML"]["html"].lower().find(state.lower())
                if index == -1:
                    continue
                counts[state] = index
            dist_most = [sys.maxsize,'']
            for state, index in counts.items():
                if index < dist_most[0]:
                    dist_most = [index, state]
            return dist_most[1]

    def write_docket(self, js, docket):
        if not self.already_written(js, docket["Case"].get("case_id")):
            start = time.time()
            self.dist_stub = self.get_dist_stub()
            #self.misc_prep(docket["DocketHTML"].get("html"))
            case = self.write_case(js, docket["Case"])
            judges = self.write_judges(js, docket["JudgeOnCase"], case)
            self.write_other_cases(js, docket["CasesInOtherCourts"], case)
            self.write_docket_entries(js, docket["DocketEntry"], judges, case)
            self.write_parties(js, docket["Party"], case)
            self.write_docket_html(js, case, docket["DocketHTML"])
            case = self.ensure_correct_case_type(case)
            #js.commit()
            return round(time.time()-start,1)
            #print("Wrote {c}".format(c=docket["Case"].get("case_id")))
        # else:
        #     print("Skipping {c}".format(c=docket["Case"].get("case_id","")))

    def safe_val_id(self, obj):
        try:
            return obj.id
        except:
            return None

    def store_case(self, js, case, nos, city, district, circuit):
        storedCase = db.Case(
                case_id = safeVal(case.get("case_id")),
                docket_flags = safeVal(case.get("docket_flags")),
                case_type = safeVal(case.get("case_type")),
                case_name = safeVal(case.get("case_name")),
                court_type = safeVal(case.get("court_type")),
                city_id = safeVal(self.safe_val_id(city)),
                district_id = safeVal(self.safe_val_id(district)),
                circuit_id = safeVal(self.safe_val_id(circuit)),
                year = safeVal(case.get("year")),
                date_filed = safeVal(case.get("date_filed")),
                date_terminated = safeVal(case.get("date_terminated")),
                case_duration = safeVal(case.get("case_duration")),
                case_status = safeVal(case.get("case_status")),
                jury_demand = safeVal(case.get("jury_demand")),
                demand = safeVal(case.get("demand")),
                cause = safeVal(case.get("cause")),
                jurisdiction = safeVal(case.get("jurisdiction")),
                download_court = safeVal(case.get("download_court")),
                nature_of_suit_id = safeVal(self.safe_val_id(nos)),
                entered_in_error = safeVal(case.get("entered_in_error")),
                sealed = safeVal(case.get("sealed"))
        )
        js.add(storedCase)
        #js.commit()
        return storedCase

    def dist_from_url(self, js):
        if not self.dist_stub:
            print("Missing dist stub")
            failure = 1/0
        abbrev = self.dist_stub
        #abbrev = self.origin_url.partition('pipeline/')[2].partition('/')[0]
        if abbrev == 'ga_n':
            district_id = "Northern District of Georgia"
        elif abbrev == 'ga_s':
            district_id = "Southern District of Georgia"
        elif abbrev == 'il_n':
            district_id = "Northern District of Illinois"
        elif abbrev == 'in_n':
            district_id = "Northern District of Indiana"
        elif abbrev == 'in_s':
            district_id = "Southern District of Indiana"
        elif abbrev == 'gamd':
            district_id = "Middle District of Georgia"
        elif abbrev == 'ilcd':
            district_id = "Central District of Illinois"
        elif abbrev == 'cacd':
            district_id = "Central District of California"
        elif abbrev == 'caed':
            district_id = "Eastern District of California"
        elif abbrev == 'cand':
            district_id = "Northern District of California"
        elif abbrev == 'casd':
            district_id = "Southern District of California"
        else:
            district_id = abbrev
            print("Problem with abbrev {a} in {h}".format(a=abbrev, h=self.html_url))
        storedDistrict = js.query(db.DistrictCourt).filter_by(label=district_id).first()
        return storedDistrict

    def write_case(self, js, case_table):
        storedCase = js.query(db.Case).filter_by(case_id=case_table.get("case_id")).first()
        if not storedCase:
            nos = ''
            if case_table.get("nature_of_suit_id"):
                nos = js.query(db.NatureOfSuit).filter_by(code=case_table["nature_of_suit_id"]["code"]).first()
            city = js.query(db.City)\
                        .filter_by(city=case_table.get("city_id"))\
                        .filter_by(state=case_table.get("state_id"))\
                        .first()
            district = ''
            circuit = ''
            if case_table.get("court_type") == "circuit":
                circuit = js.query(db.CircuitCourt).filter_by(label=case_table.get("circuit_id")).first()
            else:
                district = js.query(db.DistrictCourt).filter_by(label=case_table.get("district_id")).first()
                if district:
                    circuit = js.query(db.CircuitCourt).filter_by(label=district.circuit[0].label).first() # is this right?
                else:
                    district = self.dist_from_url(js)
                    if district:
                        circuit = js.query(db.CircuitCourt).filter_by(label=district.circuit[0].label).first() # is this right?
            storedCase = self.store_case(js, case_table, nos, city, district, circuit)
            if nos:
                storedCase.nature_of_suit.append(nos)
                nos.cases.append(storedCase)
            if district:
                storedCase.district.append(district)
                district.cases.append(storedCase)
            else:
                print("No district...? for {c}".format(c=storedCase.case_id))
            if circuit:
                storedCase.circuit.append(circuit)
                circuit.cases.append(storedCase)
            else:
                print("No circuit...? for {c}".format(c=storedCase.case_id))
            if city:
                storedCase.city.append(city)
                city.cases.append(storedCase)
            else:
                print("No city...? for {c}".format(c=storedCase.case_id))
        return storedCase

    def get_html(self):
        #filename = self.origin_url.replace('pipeline','html')
        with open(self.html_url, "rb") as f:
            docket_html = f.read()
        # just return raw html?
        soup = BeautifulSoup(docket_html, 'html.parser')
        return str(soup)

    def gather_original_parse_files(self):
        if not self.original_parse_index:
            filename = "./id_matching_table.json"
            self.original_parse_index = self.load_dockets(filename)

    def manual_failure_case_handling(self, id):
        container = {}
        filename = ''
        self.gather_original_parse_files()
        filename = self.original_parse_index.get(id)
        if not filename:
            fail = 1/0
        with open(filename, "r") as f:
            container = json.load(f)
        return container[id]

    def obtain_local_parse(self, id_str):
        try:
            # parse = self.parsed[dockets_json[key]["DocketHTML"]["parse"].partition('*|*')[2]]
            parse = self.parsed[id_str.partition('*|*')[2]]
            return parse
        except(KeyError):
            parse = self.manual_failure_case_handling(id_str.partition('*|*')[2])
            return parse

    def store_html(self, js, case, obj):
        parse = obj.get("parse",'')
        if parse:
            parse.pop("URL_path", None)
        storedHTML = db.DocketHTML(
            case_id = safeVal(case.case_id),
            html = safeVal(obj.get("html")),
            parse = safeVal(str(parse))
        )
        js.add(storedHTML)
        # js.commit()
        return storedHTML

    def write_docket_html(self, js, case, obj):
        if case.docket_html:
            storedHTML = js.query(db.DocketHTML).filter_by(case_id=case.case_id).first()
        else:
            storedHTML = ''
        # do we already have this HTML?
        # we shouldn't - if we did, we should not have begun to write this docket
        if not storedHTML:
            # if not parse:
            #     for file in self.json_queue:
            #         parse = self.get_parse(file, parse_string)
            #         if parse:
            #             break
            #     if not parse:
            #         print("Somehow failed to find parse for {c}".format(c=case.case_id))
            #         fail = 1/0
                # parse = self.obtain_local_parse(parse_string)
            storedHTML = self.store_html(js, case, obj)
            storedHTML.case.append(case)
            case.docket_html.append(storedHTML)
        return None

    def split_j_name(self, name):
        if not name:
            return None, None, None, None
        tokens = name.split()
        length = len(tokens)
        if length == 1:
            return name.strip(), None, None, None
        else:
            for word in ['jr.','sr.','ii','iii','iv','jr','sr','esq.','esq']:
                if word == tokens[-1].lower().strip():
                    if length == 2:
                        return tokens[0].strip(), None, None, tokens[-1].strip()
                    else:
                        first = tokens[0].strip() # + ' ' + tokens[-1].strip()
                        middle = ' '.join(tokens[1:-2]).strip()
                        last = tokens[-2].strip()
                        suffix = tokens[-1].strip()
                        return first, middle, last, suffix
            if length == 2:
                return tokens[0].strip(), None, tokens[-1].strip(), None
            else:
                return tokens[0].strip(), ' '.join(tokens[1:-1]).strip(), tokens[-1].strip(), None

    def store_judge(self, js, first, middle, last, suffix):
        storedJudge = db.Judge(
            first_name = safeVal(first),
            middle_name = safeVal(middle),
            last_name = safeVal(last),
            suffix = safeVal(suffix)
        )
        js.add(storedJudge)
        # js.commit()
        return storedJudge

    def store_judge_on_case(self, js, j, case, storedJudge):
        storedJudgeOnCase = db.JudgeOnCase(
            judge_nid = safeVal(storedJudge.id),
            case_id = safeVal(case.case_id),
            date_start = safeVal(j["date_filed"]),
            date_end = safeVal(j["date_terminated"]),
            name = safeVal(j["name"]),
            status = safeVal(j["status"]),
            category = safeVal(j["category"])
        )
        js.add(storedJudgeOnCase)
        return storedJudgeOnCase

    def write_judge(self, js, j, case):
        first, middle, last, suffix = self.split_j_name(j["name"])
        storedJudge = js.query(db.Judge)\
                        .filter_by(first_name=first)\
                        .filter_by(middle_name=middle)\
                        .filter_by(last_name=last)\
                        .filter_by(suffix=suffix)\
                        .first()
        if not storedJudge:
            storedJudge = self.store_judge(js, first, middle, last, suffix)
        storedJudgeOnCase = js.query(db.JudgeOnCase)\
                            .filter_by(case_id=case.case_id)\
                            .filter_by(judge_nid=storedJudge.id)\
                            .first()
        if not storedJudgeOnCase:
            storedJudgeOnCase = self.store_judge_on_case(js, j, case, storedJudge)
            storedJudgeOnCase.judge.append(storedJudge)
            storedJudgeOnCase.case.append(case)
            case.judges.append(storedJudgeOnCase)
            storedJudge.cases.append(storedJudgeOnCase)
        return storedJudge

    def write_judges(self, js, judge_table,case):
        judges = []
        if not judge_table:
            return None
        for judge in judge_table.keys():
            if not judge_table[judge]:
                continue
            elif not judge_table[judge].get("name"):
                continue
            storedJudge = self.write_judge(js, judge_table[judge], case)
            judges.append(storedJudge)
        return judges

    def sp_store_other_case(self, obj):
        storedOtherCase = db.CasesInOtherCourts(
            case_id = safeVal(obj.get("case_id")),
            other_case_id = safeVal(obj.get("other_id")),
            category = safeVal(obj.get("category"))
        )
        return storedOtherCase

    def write_other_cases(self, js, cases_table, case):
        if not cases_table:
            return None
        if not case.other_case_numbers:
            just_ids = []
        else:
            just_ids = [other.other_case_id for other in case.other_case_numbers]
        new_to_add = [obj for obj in cases_table.values() if obj.get("other_id") not in just_ids]
        # other_case_rows = []
        # for count, other_case in enumerate(new_to_add):
        #     other_case_rows.append(self.sp_store_other_case(other_case))
        #     if count % 2000 == 0:
        #         js.commit()
        if new_to_add and len(new_to_add) > 24:
            p = Pool(processes=8)
            other_case_rows = p.map(self.sp_store_other_case, new_to_add)
            p.close()
            p.join()
        else:
            other_case_rows = []
            for other_case in new_to_add:
                other_case_rows.append(self.sp_store_other_case(other_case))
        if other_case_rows:
            for row in other_case_rows:
                js.add(row)
                case.other_case_numbers.append(row)
                row.cases.append(case)
        # END NEW
        return None

    def ensure_int(self, maybe_int):
        try:
            return int(maybe_int)
        except:
            if str(maybe_int).isalpha():
                return None
            real_num = ''
            for char in str(maybe_int):
                if char.isnumeric():
                    real_num += char
            try:
                return int(real_num)
            except:
                return None

    def sp_store_docket_entry(self, e):
        storedDocketEntry = db.DocketEntry(
            entry_number = safeVal(self.ensure_int(e.get("entry_number"))),
            ordinal_number = safeVal(e.get("ordinal_number")),
            url = safeVal(e.get("url")),
            jurisdiction = safeVal(e.get("jurisdiction")),
            date_filed = safeVal(e.get("entry_date")),
            text = safeVal(e.get("entry_text")),
            entered_in_error = safeVal(e.get("entered_in_error")),
            sealed = safeVal(e.get("sealed")),
            case_id = safeVal(e.get("case_id"))
        )
        # js.add(storedDocketEntry)
        # js.commit()
        return [storedDocketEntry, e]

    def sp_write_docket_entry(self, js, entry, judges, case, storedDocketEntry):
        if storedDocketEntry:
            for entry_j in entry.get("judge_name",[]):
                first, middle, last, suffix = self.split_j_name(entry_j)
                if judges:
                    for docket_j in judges:
                        if docket_j.first_name == first and docket_j.middle_name == middle and docket_j.last_name == last and docket_j.suffix == suffix:
                            storedDocketEntry.judges.append(docket_j)
                            docket_j.docket_entries.append(storedDocketEntry)
            if entry.get("attachments"):
                # we'll need storedDocketEntry.id, so we commit
                # js.commit()
                skip_query = False
                # check = js.query(db.Attachment).filter_by(entry_id=storedDocketEntry.id).first()
                if not storedDocketEntry.attachments:
                    skip_query = True
                for a in entry.get("attachments",[]):
                    storedAttachment = self.write_attachment(js, entry["attachments"][a],storedDocketEntry, skip_query)
                    storedDocketEntry.attachments.append(storedAttachment)
                    #storedAttachment.docket_entry.append(storedDocketEntry)
            case.docket_entries.append(storedDocketEntry)
            storedDocketEntry.case.append(case)
        return None

    def write_docket_entries(self, js, entries_table, judges, case):
        if not entries_table:
            return None
        # storedDocketEntry = js.query(db.DocketEntry).filter_by(case_id=case.case_id).all()
        if not case.docket_entries:
            just_ordinals = []
        else:
            just_ordinals = [entry.ordinal_number for entry in case.docket_entries]
        new_to_add = [obj for obj in entries_table.values() if obj.get("ordinal_number") not in just_ordinals]
        entry_rows = []
        if new_to_add and len(new_to_add) > 24:
            p = Pool(processes=8)
            entry_rows = p.map(self.sp_store_docket_entry, new_to_add)
            p.close()
            p.join()
        else:
            for entry_to_store in new_to_add:
                entry_rows.append(self.sp_store_docket_entry(entry_to_store))
        if entry_rows:
            for row in entry_rows:
                entry = row[1]
                storedDocketEntry = row[0]
                js.add(storedDocketEntry)
                self.sp_write_docket_entry(js, entry, judges, case, storedDocketEntry)
        return None

    def store_attachment(self, js, a, d):
        storedAttachment = db.Attachment(
            url = safeVal(a["url"]),
            text = safeVal(a["text"]),
            entry_id = safeVal(d.id)
        )
        js.add(storedAttachment)
        # js.commit()
        return storedAttachment

    def write_attachment(self, js, attachment, docketEntry, skip_querying):
        storedAttachment = ''
        if not skip_querying:
            storedAttachment = js.query(db.Attachment)\
                                .filter_by(url=attachment["url"])\
                                .filter_by(entry_id=docketEntry.id)\
                                .first()
        if not storedAttachment:
            storedAttachment = self.store_attachment(js, attachment, docketEntry)
        return storedAttachment

    def store_party(self, js, party, role, case):
        if role:
            storedParty = db.Party(
                name = safeVal(party.get("name")),
                date_terminated = safeVal(party.get("date_terminated")),
                title = safeVal(party.get("title")),
                capacity = safeVal(party.get("capacity")),
                role_id = safeVal(role.id),
                case_id = safeVal(case.case_id),
                is_gov = safeVal(party.get("is_gov")),
            )
        else:
            storedParty = db.Party(
                name = safeVal(party.get("name")),
                date_terminated = safeVal(party.get("date_terminated")),
                title = safeVal(party.get("title")),
                capacity = safeVal(party.get("capacity")),
                role_id = safeVal(None),
                case_id = safeVal(case.case_id),
                is_gov = safeVal(party.get("is_gov")),
            )
        js.add(storedParty)
        # js.commit()
        return storedParty

    def write_party(self, js, case, party, parties, skip_querying):
        role = js.query(db.Role).filter_by(label=parties[party].get("role_id")).first()
        storedParty = ''
        if not skip_querying:
            if role:
                storedParty = js.query(db.Party)\
                                .filter_by(case_id=case.case_id)\
                                .filter_by(role_id=role.id)\
                                .filter_by(name=parties[party].get("name"))\
                                .first()
            else:
                storedParty = js.query(db.Party)\
                                .filter_by(case_id=case.case_id)\
                                .filter_by(name=parties[party].get("name"))\
                                .first()
        if not storedParty:
            commit = False
            storedParty = self.store_party(js, parties[party], role, case)
            storedParty.role.append(role)
            storedParty.case.append(case)
            case.parties.append(storedParty)
            for alias in parties[party].get("alias",[]):
                if not commit:
                    #js.commit()
                    commit = True
                storedAlias = self.write_alias(js, alias, storedParty)
                # storedAlias.party.append(storedParty)
                storedParty.aliases.append(storedAlias)
            if parties[party].get("reps"):
                if not commit:
                    #js.commit()
                    commit = True
                skip_query = False
                # check = storedAttorney = js.query(db.Attorney).filter_by(case_id=case.case_id).filter_by(party_id=storedParty.id).first()
                if not storedParty.attorneys:
                    skip_query = True
                for rep in parties[party]["reps"].values():
                    storedAttorney = self.write_attorney(js, rep, storedParty, case, skip_query)
                    storedAttorney.party.append(storedParty)
                    storedAttorney.case.append(case)
                    storedParty.attorneys.append(storedAttorney)
                    case.attorneys.append(storedAttorney)
            if parties[party].get("charges"):
                if not commit:
                    # js.commit()
                    commit = True
                skip_query = False
                # check = js.query(db.Charges).filter_by(case_id=case.case_id).filter_by(party_id=storedParty.id).first()
                if not storedParty.charges:
                    skip_query = True
                for charge_category in parties[party]["charges"].values():
                    for charge in charge_category.values():
                        storedCharge = self.write_charge(js, charge, storedParty, case, skip_query)
                        #storedCharge.party.append(storedParty)
                        storedCharge.case.append(case)
                        storedParty.charges.append(storedCharge)
                        case.charges.append(storedCharge)
        return storedParty

    def write_parties(self, js, parties, case):
        if not parties:
            return None
        skip_querying = False
        # check = js.query(db.Party).filter_by(case_id=case.case_id).first()
        if not case.parties:
            skip_querying = True
        for party in parties.keys():
            storedParty = self.write_party(js, case, party, parties, skip_querying)
        return None

    def store_alias(self, js, alias, party):
        storedAlias = db.Alias(
            text = safeVal(alias),
            party_id = safeVal(party.id)
        )
        js.add(storedAlias)
        # js.commit()
        return storedAlias

    def write_alias(self, js, alias, party):
        storedAlias = js.query(db.Alias)\
                        .filter_by(text=alias)\
                        .filter_by(party_id=party.id)\
                        .first()
        if not storedAlias:
            storedAlias = self.store_alias(js, alias, party)
        return storedAlias

    def store_attorney(self, js, a, case, party):
        storedAttorney = db.Attorney(
            name = safeVal(a.get("name")),
            date_terminated = safeVal(a.get("date_terminated")),
            case_id = safeVal(case.case_id),
            party_id = safeVal(party.id)
        )
        js.add(storedAttorney)
        # js.commit()
        return storedAttorney

    def write_attorney(self, js, a, party, case, skip_querying):
        storedAttorney = ''
        if not skip_querying:
            storedAttorney = js.query(db.Attorney)\
                            .filter_by(name=a.get("name"))\
                            .filter_by(case_id=case.case_id)\
                            .filter_by(party_id=party.id)\
                            .first()
        if not storedAttorney:
            storedAttorney = self.store_attorney(js, a, case, party)
            if a.get("designations"):
                for d in a.get("designations"):
                    storedDesignation = js.query(db.Designation).filter_by(label=d).first()
                    if not storedDesignation:
                        out = ''.join(['"',case.case_id,'": ','"',d,'"'])
                        print(out)
                        stop = input("Can we skip this? ")
                        if stop == "y":
                            continue
                        else:
                            fail = 1/0
                    storedAttorney.designations.append(storedDesignation)
            if a.get("contact"):
                for c in a.get("contact").keys():
                    storedContact = self.write_contact(js, a["contact"][c], c, storedAttorney)
                    storedContact.attorney.append(storedAttorney)
                    storedAttorney.contact.append(storedContact)
            if a.get("firm"):
                storedFirm = self.write_firm(js, a["firm"])
                storedFirm.attorneys.append(storedAttorney)
                storedAttorney.firm.append(storedFirm)
        return storedAttorney

    def store_contact(self, js, text, category, a):
        storedContact = db.Contact(
            text = safeVal(text),
            category = safeVal(category),
            attorney_id = safeVal(a.id)
        )
        js.add(storedContact)
        # js.commit()
        return storedContact

    def write_contact(self, js, contact_text, contact_category, attorney):
        storedContact = js.query(db.Contact)\
                        .filter_by(category=contact_category)\
                        .filter_by(text=contact_text)\
                        .filter_by(attorney_id=attorney.id)\
                        .first()
        if not storedContact:
            storedContact = self.store_contact(js, contact_text, contact_category, attorney)
        return storedContact

    def store_firm(self, js, name, addr, zip, city):
        storedFirm = db.AttorneyOrg(
                name = safeVal(name),
                street_address = safeVal(addr),
                zipcode = safeVal(zip),
                city_id = safeVal(self.safe_val_id(city))
        )
        js.add(storedFirm)
        # js.commit()
        return storedFirm

    def write_firm(self, js, f):
        city = js.query(db.City)\
                    .filter_by(city=f.get("city"))\
                    .filter_by(state=f.get("state"))\
                    .first()
        if city:
            storedFirm = js.query(db.AttorneyOrg)\
                        .filter_by(name=f.get("name"))\
                        .filter_by(street_address=f.get("street_address"))\
                        .filter_by(city_id=city.id)\
                        .first()
        else:
            storedFirm = js.query(db.AttorneyOrg)\
                            .filter_by(name=f.get("name"))\
                            .filter_by(street_address=f.get("street_address"))\
                            .first()
            if f.get("city") != None:
                print(f.get("city"))
        if not storedFirm:
            storedFirm = self.store_firm(js, f.get("name"), f.get("street_address"), f.get("zip"), city)
            if city:
                storedFirm.city.append(city)
                city.firms.append(storedFirm)
        return storedFirm

    def store_charge(self, js, c, charge_label, party, case):
        storedCharge = db.Charges(
            party_id = safeVal(party.id),
            case_id = safeVal(case.case_id),
            category = safeVal(c.get("type")),
            label_id = safeVal(charge_label.id),
            counts = safeVal(c.get("counts")),
            status = safeVal(c.get("status")),
            disposition_text = safeVal(c.get("disposition_text"))
        )
        js.add(storedCharge)
        # js.commit()
        return storedCharge

    def write_charge(self, js, c, party, case, skip_querying):
        if not c:
            return None
        #charge_label = js.query(db.ChargeLabel).filter_by(label=c.get("label")).first()
        storedCharge = ''
        clabel = ''.join(['%',c.get("label"),'%'])
        charge_label = js.query(db.ChargeLabel).filter(db.ChargeLabel.label.ilike(clabel)).first()
        if not charge_label:
            print('"{u}": "{l}"'.format(u=c.get("label",'').lower(),l=c.get("label")))
        if not skip_querying:
            storedCharge = js.query(db.Charges)\
                        .filter_by(case_id=case.case_id)\
                        .filter_by(party_id=party.id)\
                        .filter_by(counts=c.get("counts"))\
                        .filter_by(label_id=charge_label.id)\
                        .first()
        if not storedCharge:
            storedCharge = self.store_charge(js, c, charge_label, party, case)
            if c.get("disposition_details"):
                for disposition in c["disposition_details"].values():
                    skip_query = False
                    if not storedCharge.disposition:
                        skip_query = True
                    storedDisposition = self.write_disposition_detail(js, disposition, storedCharge, case, skip_query)
                    storedCharge.disposition.append(storedDisposition)
            storedCharge.label.append(charge_label)
            charge_label.cases.append(case)
            case.charge_labels.append(charge_label)
        return storedCharge

    def store_disposition(self, js, value, units, charge, disposition_label):
        storedDisposition = db.DispositionDetail(
            charge_id = safeVal(charge.id),
            label_id = safeVal(disposition_label.id),
            value = safeVal(value),
            units = safeVal(units)
        )
        js.add(storedDisposition)
        # js.commit()
        return storedDisposition

    def get_disposition_val_and_unit(self, value, unit):
        if not value:
            return None, unit
        elif isinstance(value, float):
            return value, unit
        elif value == 'Time Served' or value == 'Terminated' or value == 'Life':
            return None, value
        elif isinstance(value, int):
            return value, unit
        else:
            print("NO DISPO {v} {u}".format(v=value, u=unit))
            return value, unit

    def write_disposition_detail(self, js, d, charge, case, skip_query):
        if not d or not d.get("type"):
            return None
        disposition_label = js.query(db.DispositionLabel).filter_by(label=d.get("type")).first()
        storedDisposition = ''
        if disposition_label:
            if not skip_query:
                storedDisposition = js.query(db.DispositionDetail)\
                                .filter_by(charge_id=charge.id)\
                                .filter_by(label_id=disposition_label.id)\
                                .first()
        else:
            print("\n\nNO DISP FOUND: {c}".format(c=d))
            return None
        if not storedDisposition:
            value, units = self.get_disposition_val_and_unit(d.get("value"), d.get("units"))
            storedDisposition = self.store_disposition(js, value, units, charge, disposition_label)
            storedDisposition.label.append(disposition_label)
            disposition_label.cases.append(case)
            case.disposition_labels.append(disposition_label)
        return storedDisposition

def writeDockets():
    meta = input("Should I write the docket metadata? (y/n) ")
    if meta.lower() == 'y':
        print("========\nLoading up all the metadata tables...\n========")
        tables = DocketMetadataTables(os.environ.get("noacri_metadata_tables"))
        #tables = DocketMetadataTables("./table_json")
        tables.load()
        tables.write()
        print("========\nDone with the metadata...\n========")
    else:
        print("Skipping the docket metadata...")
    print("========\nLoading up all the docket data...\n========")
    dockets = DocketTables(os.environ.get("noacri_dockets"))
    # dockets = DocketTables("./noacri_dockets")
    dockets.load_completes("./done.json")
    dockets.load()
    try:
        dockets.write()
    except Exception as e:
        print("Failed with {e}...".format(e=e))
        dockets.save_completes("./done.json")
    print("========\nDone with all the dockets...\n========")
    print("\nfin")

# And here's the part where we do everything...
# These checks can/should be blown out to provide more options as the data complexity grows
shortcut = input("Skip to the docket ingestion? ")
if shortcut != "y":
    demolishDB = input("Should I discard all our hard work and remake the DB from scratch? (y/n) ")
    fullWipe = False
    if demolishDB.lower() == "y":
        certainty = input("Are you sure you want to ERASE the database?  This is irrevocable. (y/n) ")
        if certainty.lower() == "y":
            print("Your wish is my command.  Out with the old...")
            db.dropall()
            db.build()
            fullWipe = True
    if not fullWipe:
        addMissingTables = input("Should I add in missing tables? This won't clear any existing data. (y/n) ")
        if addMissingTables.lower() == "y":
            db.build()
        else:
            print("Skipping the missing tables build...")
    writeJudgeCheck = input("Should I write the judge data? (y/n) ")
    if writeJudgeCheck.lower() == "y":
        writeJudges()
    else:
        print("Skipping the judge import/write...")
    writeDocketsCheck = input("Should I write the docket data? (y/n) ")
    if writeDocketsCheck.lower() == "y":
        writeDockets()
    else:
        print("Skipping the dockets...")
elif shortcut == "y":
    writeDockets()
