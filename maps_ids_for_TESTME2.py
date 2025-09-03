#!/usr/bin/env python3

from pathlib import Path
import pandas as pd
from rapidfuzz import process, fuzz
import re, sys

BASE_DIR = Path(__file__).parent

TESTME_PATH     = BASE_DIR / "TESTME_with_ids2.xlsx"
ACCOUNTS_PATH   = BASE_DIR / "Accounts.csv"
CONTACTS_PATH   = BASE_DIR / "Contacts.csv"
ACCOUNTS2_PATH  = BASE_DIR / "Accounts2.csv"
CONTACTS2_PATH  = BASE_DIR / "Contacts2.csv"

OUTPUT_XLSX     = BASE_DIR / "TESTME_with_ids2.xlsx"
AMBIG_CSV       = BASE_DIR / "ambiguous_matches.csv"

FUZZY_THRESHOLD       = 85          #EDIT VALUES FOR STRICTER OR LOOSER MATCHING (HIGHER = MORE STRICT) lowest I found effective was 80
FALLBACK_FUZZY_THRESH = 75         

# ---------------- Allowed Values ----------------
ALLOWED_TYPES = ['Administrative','App Development','Client Project','Configuration','Configuration Change','CPQ Issues','CSM Issues','Feature Request','Marketing','Miscellaneous Type','New Feature','Problem','Question','Sales Issues','Sales Non-CPQ Related Issues']
ALLOWED_SUBTYPES = ['ServiceDesk+ App','Credit App','Email Template','SpringCM Project','Salesforce Project','Miscellaneous SubType']
ALLOWED_CATEGORIES = ['Client Training','System Access','Data Extraction','Planning','Integration','Reporting','Case Management','Stakeholder Management','Client Research','Project Scope','File Management','Data']

COMMON_COMPANY_SUFFIXES = {"inc","inc.","llc","l.l.c","ltd","co","co.","corp","corporation","company","incorporated","plc","llp"}

# ---------------- Helpers ----------------
def normalize_text(s: str) -> str:
    if not isinstance(s, str): return ""
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_company(name: str) -> str:
    toks = [t for t in normalize_text(name).split() if t not in COMMON_COMPANY_SUFFIXES]
    return " ".join(toks).strip()

def normalize_person(name: str) -> str:
    if not isinstance(name, str): return ""
    s = name.strip()
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        if len(parts) >= 2:
            s = parts[1] + " " + parts[0]
    return normalize_text(s)

def load_table(path: Path) -> pd.DataFrame:
    if not path.exists(): return pd.DataFrame()
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, dtype=str).fillna("")
    return pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")

# ---------------- Classification Rules ----------------
def classify_type_subtype_category(text: str) -> tuple[str,str,str]:
    t = text.lower()

    # Type
    if "cpq" in t:
        type_value = "CPQ Issues"
    elif "salesforce" in t:
        type_value = "Configuration"
    elif "email template" in t:
        type_value = "Client Project"
    elif any(w in t for w in ["bug","issue","error"]):
        type_value = "Problem"
    elif "feature request" in t:
        type_value = "Feature Request"
    else:
        type_value = "Miscellaneous Type"

    # Sub-Type
    if "servicedesk" in t:
        subtype_value = "ServiceDesk+ App"
    elif "springcm" in t:
        subtype_value = "SpringCM Project"
    elif "salesforce" in t:
        subtype_value = "Salesforce Project"
    elif "credit app" in t:
        subtype_value = "Credit App"
    elif "email template" in t:
        subtype_value = "Email Template"
    else:
        subtype_value = "Miscellaneous SubType"

    # Category
    if "training" in t:
        category_value = "Client Training"
    elif any(w in t for w in ["login","sso","access","permission"]):
        category_value = "System Access"
    elif "report" in t or "dashboard" in t:
        category_value = "Reporting"
    elif "data" in t or "etl" in t or "extract" in t:
        category_value = "Data Extraction"
    elif "plan" in t or "scope" in t or "roadmap" in t:
        category_value = "Planning"
    elif "integrat" in t or "api" in t:
        category_value = "Integration"
    else:
        category_value = "Case Management"

    # snap to allowed lists
    if type_value not in ALLOWED_TYPES: type_value = "Miscellaneous Type"
    if subtype_value not in ALLOWED_SUBTYPES: subtype_value = "Miscellaneous SubType"
    if category_value not in ALLOWED_CATEGORIES: category_value = "Case Management"
    return type_value, subtype_value, category_value

# ---------------- Main ----------------
def main():
    if not TESTME_PATH.exists():
        print(f"ERROR: TESTME.xlsx not found at {TESTME_PATH}"); sys.exit(1)

    accounts_df  = load_table(ACCOUNTS_PATH)
    contacts_df  = load_table(CONTACTS_PATH)
    accounts2_df = load_table(ACCOUNTS2_PATH)
    contacts2_df = load_table(CONTACTS2_PATH)
    accounts_df = pd.concat([accounts_df, accounts2_df], ignore_index=True).drop_duplicates().fillna("")
    contacts_df = pd.concat([contacts_df, contacts2_df], ignore_index=True).drop_duplicates().fillna("")
    all_sheets = pd.read_excel(TESTME_PATH, sheet_name=None, dtype=str, engine="openpyxl")
    sheet_name = "Full Acc and Contact" if "Full Acc and Contact" in all_sheets else list(all_sheets.keys())[0]
    cases_df = all_sheets[sheet_name].fillna("")
    acc_id_col = "Id" if "Id" in accounts_df.columns else accounts_df.columns[0]
    acc_name_col = "Name" if "Name" in accounts_df.columns else accounts_df.columns[1]
    con_id_col = "Id" if "Id" in contacts_df.columns else contacts_df.columns[0]
    con_name_col = "Name" if "Name" in contacts_df.columns else contacts_df.columns[1]
    account_map = {normalize_company(r[acc_name_col]): (r[acc_id_col], r[acc_name_col]) for _,r in accounts_df.iterrows()}
    contact_map = {normalize_person(r[con_name_col]): (r[con_id_col], r[con_name_col]) for _,r in contacts_df.iterrows()}

    for c in ["AccountId","ContactId","Type","Sub-Type","Category"]:
        if c not in cases_df.columns:
            cases_df[c] = ""

    ambiguous = []
    for idx,row in cases_df.iterrows():
        summary = str(row.get("Email Summary","")) + " " + str(row.get("Description",""))
        norm_text = normalize_text(summary)

        # Account info
        if not row.get("AccountId"):
            best = process.extractOne(norm_text, list(account_map.keys()), scorer=fuzz.token_sort_ratio)
            if best and best[1] >= FUZZY_THRESHOLD:
                aid, aname = account_map[best[0]]
                cases_df.at[idx,"AccountId"] = aid
            elif best:
                ambiguous.append({"row":idx,"account_guess":best[0],"score":best[1]})

        # Contact info
        if not row.get("ContactId"):
            best = process.extractOne(norm_text, list(contact_map.keys()), scorer=fuzz.token_sort_ratio)
            if best and best[1] >= FUZZY_THRESHOLD:
                cid, cname = contact_map[best[0]]
                cases_df.at[idx,"ContactId"] = cid
            elif best:
                ambiguous.append({"row":idx,"contact_guess":best[0],"score":best[1]})

    
        t,st,c = classify_type_subtype_category(summary)
        if not row.get("Type"): cases_df.at[idx,"Type"] = t
        if not row.get("Sub-Type"): cases_df.at[idx,"Sub-Type"] = st
        if not row.get("Category"): cases_df.at[idx,"Category"] = c
        
    

    drop_cols = []
    if "Sub-Type" in cases_df.columns:
        drop_cols.append("Sub-Type")
    if "Category" in cases_df.columns:
        drop_cols.append("Category")
    if drop_cols:
        print(f"Dropping duplicate columns: {drop_cols}")
        cases_df = cases_df.drop(columns=drop_cols)

    for col in cases_df.columns:
        if cases_df[col].dtype == "object":
            cases_df[col] = cases_df[col].str.replace("_x000D_", " ", regex=False)
            cases_df[col] = cases_df[col].str.replace("\n", " ", regex=False)
            cases_df[col] = cases_df[col].str.strip()

    cases_df.rename(columns=lambda c: c.strip(), inplace=True)
    CLEAN_OUTPUT_XLSX = TESTME_PATH.with_name("TESTME_with_ids_clean.xlsx")
    with pd.ExcelWriter(CLEAN_OUTPUT_XLSX, engine="openpyxl") as writer:
        for sname, df in all_sheets.items():
            df.to_excel(writer, sheet_name=sname, index=False)
    print(f"Clean Excel written to: {CLEAN_OUTPUT_XLSX}")
    CLEAN_OUTPUT_CSV = TESTME_PATH.with_name("TESTME_with_ids_clean.csv")
    cases_df.to_csv(CLEAN_OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Clean CSV written to: {CLEAN_OUTPUT_CSV}")


if __name__ == "__main__":
    main()
