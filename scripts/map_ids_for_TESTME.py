#!/usr/bin/env python3
"""
map_ids_for_TESTME.py

Reads TESTME.xlsx, Accounts.csv, contacts.csv and maps AccountId & ContactId into the TESTME sheet.
Outputs TESTME_with_ids.xlsx and ambiguous_matches.csv.

Drop this file in the same folder as TESTME.xlsx, Accounts.csv, contacts.csv (default: Downloads).
"""
from pathlib import Path
import pandas as pd
import json, re, sys, csv
from rapidfuzz import process, fuzz

# -------- CONFIG: adjust if your files are in different locations --------
BASE_DIR = Path.home() / "Downloads"
TESTME_XLSX = BASE_DIR / "TESTME.xlsx"
ACCOUNTS_CSV = BASE_DIR / "Accounts.csv"
CONTACTS_CSV = BASE_DIR / "contacts.csv"
OUTPUT_XLSX = BASE_DIR / "TESTME_with_ids.xlsx"
AMBIGUOUS_CSV = BASE_DIR / "ambiguous_matches.csv"

FUZZY_THRESHOLD = 85  # raise for stricter matching, lower to match more aggressively

# -------- utilities --------
COMMON_COMPANY_SUFFIXES = {"inc","inc.","llc","l.l.c","ltd","co","co.","corp","corporation","company","incorporated","plc","llp"}

def normalize_company(name):
    if not isinstance(name, str): return ""
    s = name.strip()
    s = s.lower()
    s = re.sub(r'[\u2018\u2019\u201c\u201d]', "'", s)
    s = re.sub(r'[^a-z0-9\s]', ' ', s)
    # remove common suffix tokens to help match Micronetbd vs Micronetbd Inc
    toks = [t for t in s.split() if t not in COMMON_COMPANY_SUFFIXES]
    return " ".join(toks).strip()

def normalize_person(name):
    if not isinstance(name, str): return ""
    s = name.strip()
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        if len(parts) >= 2:
            s = parts[1] + " " + parts[0]
    s = s.lower()
    s = re.sub(r'[^a-z0-9\s\-]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def find_first_col(df_cols, candidates):
    for c in candidates:
        if c in df_cols:
            return c
    return None

# -------- load data --------
print("Loading files...")
if not TESTME_XLSX.exists():
    print(f"ERROR: TESTME.xlsx not found at {TESTME_XLSX}")
    sys.exit(1)
if not ACCOUNTS_CSV.exists():
    print(f"ERROR: Accounts.csv not found at {ACCOUNTS_CSV}")
    sys.exit(1)
if not CONTACTS_CSV.exists():
    print(f"ERROR: contacts.csv not found at {CONTACTS_CSV}")
    sys.exit(1)

# read all sheets to preserve workbook and to update the proper sheet later
all_sheets = pd.read_excel(TESTME_XLSX, sheet_name=None, engine="openpyxl", dtype=str)
# choose sheet: Full Acc and Contact if exists else first sheet
sheet_name = "Full Acc and Contact" if "Full Acc and Contact" in all_sheets else list(all_sheets.keys())[0]
cases_df = all_sheets[sheet_name].fillna("")

accounts_df = pd.read_csv(ACCOUNTS_CSV, dtype=str).fillna("")
contacts_df = pd.read_csv(CONTACTS_CSV, dtype=str).fillna("")

print(f"Using sheet: {sheet_name}. Cases rows: {len(cases_df)}")
print(f"Accounts rows: {len(accounts_df)}; Contacts rows: {len(contacts_df)}")

# -------- build indexes --------
# find id/name cols in accounts
acc_id_col = find_first_col(accounts_df.columns, ["Id","ID","AccountId","Account Id","accountid"])
acc_name_col = find_first_col(accounts_df.columns, ["Name","Account Name","AccountName","name"])
if acc_id_col is None or acc_name_col is None:
    print("ERROR: Could not find Id or Name column in Accounts.csv. Columns:", accounts_df.columns.tolist())
    sys.exit(1)

# build normalized map for accounts
account_norm_map = {}   # norm -> list of (acc_id, raw_name)
account_choices = []
for _, r in accounts_df.iterrows():
    raw = str(r.get(acc_name_col,"")).strip()
    aid = str(r.get(acc_id_col,"")).strip()
    norm = normalize_company(raw)
    if not norm:
        continue
    account_norm_map.setdefault(norm, []).append((aid, raw))
    if norm not in account_choices:
        account_choices.append(norm)

# contacts: try to find Id, FirstName, LastName, AccountId (on contact)
con_id_col = find_first_col(contacts_df.columns, ["Id","ID","ContactId","Contact Id","contactid"])
con_first_col = find_first_col(contacts_df.columns, ["FirstName","First Name","First"])
con_last_col = find_first_col(contacts_df.columns, ["LastName","Last Name","Last"])
con_full_col = find_first_col(contacts_df.columns, ["FullName","Name","ContactName","Contact Name"])
con_accid_col = find_first_col(contacts_df.columns, ["AccountId","Account Id","Account_Id","AccountID"])

if con_id_col is None:
    print("ERROR: Could not find Contact Id column in contacts.csv. Columns:", contacts_df.columns.tolist())
    sys.exit(1)

contact_norm_map = {}  # norm -> list of (contact_id, raw_fullname, accountid_if_any)
contact_choices = []
for _, r in contacts_df.iterrows():
    cid = str(r.get(con_id_col,"")).strip()
    if con_full_col:
        raw_full = str(r.get(con_full_col,"")).strip()
    else:
        raw_full = (str(r.get(con_first_col,"")).strip() + " " + str(r.get(con_last_col,"")).strip()).strip()
    accid = str(r.get(con_accid_col,"")).strip() if con_accid_col else ""
    norm = normalize_person(raw_full)
    if not norm:
        continue
    contact_norm_map.setdefault(norm, []).append((cid, raw_full, accid))
    if norm not in contact_choices:
        contact_choices.append(norm)

# helper: find columns in cases for account/contact names & existing ids
cases_cols = cases_df.columns.tolist()
acct_name_col = find_first_col(cases_cols, ["Account Name","AccountName","Account","AccountName"])
contact_name_col = find_first_col(cases_cols, ["Contact Name","ContactName","Contact","Contact FullName"])
acct_id_out_col = find_first_col(cases_cols, ["AccountId","Account Id","Account_Id"]) or "AccountId"
con_id_out_col = find_first_col(cases_cols, ["ContactId","Contact Id","Contact_Id"]) or "ContactId"

print("Detected case columns -> account name:", acct_name_col, "contact name:", contact_name_col)
print("Will write AccountId column:", acct_id_out_col, "ContactId column:", con_id_out_col)

# ensure output id columns exist
if acct_id_out_col not in cases_df.columns:
    cases_df[acct_id_out_col] = ""
if con_id_out_col not in cases_df.columns:
    cases_df[con_id_out_col] = ""

# prepare ambiguous log
ambiguous_rows = []
processed=0
filled_acc=0
filled_con=0

# caches
acc_cache = {}
con_cache = {}

# ---------- matching functions ----------
def match_account_for_row(acc_name_raw, contact_name_raw):
    key = (acc_name_raw or "", contact_name_raw or "")
    if key in acc_cache:
        return acc_cache[key]
    # prefer account via contact if contact is exact and has accid
    contact_norm = normalize_person(contact_name_raw) if contact_name_raw else ""
    if contact_norm and contact_norm in contact_norm_map:
        for cid, raw_full, accid in contact_norm_map[contact_norm]:
            if accid:
                acc_cache[key] = (accid, raw_full, "contact->account")
                return acc_cache[key]
    # exact normalized account name
    if acc_name_raw:
        acc_norm = normalize_company(acc_name_raw)
        if acc_norm in account_norm_map:
            # if multiple candidates, prefer one that has at least one contact mapped to its accid
            cand = account_norm_map[acc_norm]
            if contact_norm:
                for aid, raw in cand:
                    # check if any contact has this accid
                    for _, _, c_accid in [x for sub in contact_norm_map.values() for x in sub]:
                        if c_accid and c_accid == aid:
                            acc_cache[key] = (aid, raw, "exact_with_contact_preference")
                            return acc_cache[key]
            aid, raw = cand[0]
            acc_cache[key] = (aid, raw, "exact")
            return acc_cache[key]
    # fuzzy match account name against choices
    if acc_name_raw:
        acc_norm = normalize_company(acc_name_raw)
        best = process.extractOne(acc_norm, account_choices, scorer=fuzz.token_sort_ratio)
        if best:
            match_norm, score, _ = best
            if score >= FUZZY_THRESHOLD:
                cand = account_norm_map.get(match_norm)
                aid, raw = cand[0]
                acc_cache[key] = (aid, raw, f"fuzzy:{score}")
                return acc_cache[key]
            else:
                # log ambiguous candidate
                ambiguous_rows.append({
                    "type":"account_low_score", "account_name": acc_name_raw, "best_match": match_norm, "score": score
                })
    # try derive account from contact record (if any contact matched has accid)
    if contact_norm and contact_norm in contact_norm_map:
        for cid, raw_full, accid in contact_norm_map[contact_norm]:
            if accid:
                acc_cache[key] = (accid, None, "contact_record_accid")
                return acc_cache[key]
    acc_cache[key] = (None, None, "no_match")
    return acc_cache[key]

def match_contact_for_row(contact_name_raw, account_id_hint=None):
    key = (contact_name_raw or "", account_id_hint or "")
    if key in con_cache:
        return con_cache[key]
    contact_norm = normalize_person(contact_name_raw) if contact_name_raw else ""
    if not contact_norm:
        con_cache[key] = (None, None, "no_contact_name")
        return con_cache[key]
    # exact normalized contact match
    if contact_norm in contact_norm_map:
        cands = contact_norm_map[contact_norm]
        # prefer contact that belongs to account_id_hint if provided
        if account_id_hint:
            for cid, raw_full, accid in cands:
                if accid and accid == account_id_hint:
                    con_cache[key] = (cid, raw_full, "exact_with_account_pref")
                    return con_cache[key]
        cid, raw_full, accid = cands[0]
        con_cache[key] = (cid, raw_full, "exact")
        return con_cache[key]
    # fuzzy contact match
    best = process.extractOne(contact_norm, contact_choices, scorer=fuzz.token_sort_ratio)
    if best:
        match_norm, score, _ = best
        if score >= FUZZY_THRESHOLD:
            cands = contact_norm_map.get(match_norm, [])
            cid, raw_full, accid = cands[0]
            con_cache[key] = (cid, raw_full, f"fuzzy:{score}")
            return con_cache[key]
        else:
            ambiguous_rows.append({"type":"contact_low_score","contact_name":contact_name_raw,"best_match":match_norm,"score":score})
    con_cache[key] = (None, None, "no_match")
    return con_cache[key]

# -------- iterate cases_df and fill ids --------
for idx, row in cases_df.iterrows():
    processed += 1
    acct_name = str(row.get(acct_name_col,"")).strip() if acct_name_col else ""
    contact_name = str(row.get(contact_name_col,"")).strip() if contact_name_col else ""
    existing_accid = str(row.get(acct_id_out_col,"")).strip()
    existing_conid = str(row.get(con_id_out_col,"")).strip()

    # If both already present skip
    if existing_accid and existing_conid:
        continue

    # Try account match
    acc_id, matched_acc_name, acc_method = match_account_for_row(acct_name, contact_name)
    if acc_id and not existing_accid:
        cases_df.at[idx, acct_id_out_col] = acc_id
        filled_acc += 1

    # Try contact match, prefer contact that belongs to the matched account
    con_id, matched_con_name, con_method = match_contact_for_row(contact_name, account_id_hint=acc_id)
    if con_id and not existing_conid:
        cases_df.at[idx, con_id_out_col] = con_id
        filled_con += 1

    # If account still missing but contact matched and contact record has accid, set account
    if (not cases_df.at[idx, acct_id_out_col]) and con_id:
        # find contact entry for this id to get its accid
        found_accid = None
        for cand_list in contact_norm_map.values():
            for cid, raw_full, accid in cand_list:
                if cid == con_id and accid:
                    found_accid = accid
                    break
            if found_accid:
                break
        if found_accid:
            cases_df.at[idx, acct_id_out_col] = found_accid
            filled_acc += 1

# -------- write outputs: preserve other sheets --------
all_sheets[sheet_name] = cases_df  # replace updated sheet

with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    for sname, df in all_sheets.items():
        df.to_excel(writer, sheet_name=sname, index=False)

# write ambiguous log if anything
if ambiguous_rows:
    amb_df = pd.DataFrame(ambiguous_rows)
    amb_df.to_csv(AMBIGUOUS_CSV, index=False)
    print(f"Ambiguous matches logged to: {AMBIGUOUS_CSV}")

print(f"Done. Processed rows: {processed}. AccountId filled: {filled_acc}. ContactId filled: {filled_con}")
print(f"Output written to: {OUTPUT_XLSX}")
