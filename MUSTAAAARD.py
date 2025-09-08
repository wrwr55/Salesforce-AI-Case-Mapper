# #!/usr/bin/env python3
# """
# map_ids_for_TESTME_fixed.py
# 
# Fixed, robust version of your mapping + hybrid classifier script.
# Drop it in the same folder as TESTME.xlsx, Accounts2.csv, Contacts2.csv and click Run.
# """
# 
# from pathlib import Path
# from typing import Dict, List, Optional, Tuple
# import re
# import sys
# import traceback
# 
# import pandas as pd
# import numpy as np
# from rapidfuzz import process, fuzz
# 
# # Try to import sentence-transformers for semantic matching (optional)
# USE_EMBEDDINGS = True
# try:
#     from sentence_transformers import SentenceTransformer, util
# except Exception:
#     USE_EMBEDDINGS = False
#     SentenceTransformer = None
#     util = None
# 
# # ---------------- Paths ----------------
# BASE_DIR = Path(__file__).parent
# 
# TESTME_XLSX = BASE_DIR / "TESTME2.xlsx"
# ACCOUNTS_CSV = BASE_DIR / "Accounts2.csv"
# CONTACTS_CSV = BASE_DIR / "Contacts2.csv"
# 
# OUTPUT_XLSX = BASE_DIR / "TESTME_with_ids.xlsx"
# CLEAN_OUTPUT_XLSX = BASE_DIR / "TESTME_with_ids_clean.xlsx"
# CLEAN_OUTPUT_CSV = BASE_DIR / "TESTME_with_ids_clean.csv"
# AMBIGUOUS_CSV = BASE_DIR / "ambiguous_matches.csv"
# 
# # ---------------- Thresholds ----------------
# NAME_FUZZY_STRICT = 90
# NAME_FUZZY_FROM_TEXT = 85
# SIMILARITY_THRESHOLD_ACCOUNT_CONTACT = 0.80  # semantic cos sim threshold for account/contact
# SIMILARITY_THRESHOLD_LABEL = 0.55            # semantic threshold for labels (lower)
# FUZZY_THRESHOLD_LABEL = 75                   # fallback fuzzy for labels
# 
# # ---------------- Allowed lists ----------------
# ALLOWED_TYPES = [
#     'Administrative','App Development','Client Project','Configuration','Configuration Change',
#     'CPQ Issues','CSM Issues','Feature Request','Marketing','Miscellaneous Type',
#     'New Feature','Problem','Question','Sales Issues','Sales Non-CPQ Related Issues'
# ]
# ALLOWED_SUBTYPES = [
#     'ServiceDesk+ App','Credit App','Email Template','SpringCM Project','Salesforce Project','Miscellaneous SubType'
# ]
# ALLOWED_CATEGORIES = [
#     'Client Training','System Access','Data Extraction','Planning','Integration','Reporting',
#     'Case Management','Stakeholder Management','Client Research','Project Scope','File Management','Data'
# ]
# 
# COMMON_COMPANY_SUFFIXES = {"inc","inc.","llc","l.l.c","ltd","co","co.","corp","corporation","company","incorporated","plc","llp"}
# 
# # ------------------ Helpers ------------------
# def normalize_text(val: object) -> str:
#     """Lowercase, remove Excel artifacts, keep letters/numbers/spaces and - + signs."""
#     if val is None:
#         return ""
#     s = str(val)
#     s = s.replace("_x000D_", " ").replace("\r", " ").replace("\n", " ").replace("\t", " ")
#     s = s.lower()
#     s = re.sub(r"[^a-z0-9\s\-\+]", " ", s)
#     s = re.sub(r"\s+", " ", s).strip()
#     return s
# 
# def normalize_company(name: object) -> str:
#     s = normalize_text(name)
#     toks = [t for t in s.split() if t not in COMMON_COMPANY_SUFFIXES]
#     return " ".join(toks).strip()
# 
# def normalize_person(name: object) -> str:
#     if name is None:
#         return ""
#     s = str(name).strip()
#     if "," in s:
#         parts = [p.strip() for p in s.split(",")]
#         if len(parts) >= 2:
#             s = parts[1] + " " + parts[0]
#     return normalize_text(s)
# 
# def find_first_col(df_cols: List[str], candidates: List[str]) -> Optional[str]:
#     """Return the first column name from df_cols that matches any candidate exactly (case-sensitive)."""
#     for c in candidates:
#         if c in df_cols:
#             return c
#     # fallback: case-insensitive match
#     lower_map = {col.lower(): col for col in df_cols}
#     for c in candidates:
#         if c.lower() in lower_map:
#             return lower_map[c.lower()]
#     return None
# 
# def load_table(path: Path) -> pd.DataFrame:
#     if not path.exists():
#         return pd.DataFrame()
#     if path.suffix.lower() == ".csv":
#         return pd.read_csv(path, dtype=str).fillna("")
#     return pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")
# 
# # ------------------ Rule sets ------------------
# TYPE_RULES: Dict[str, List[str]] = {
#     "CPQ Issues": ["cpq","sbqq","quote line","quote object"],
#     "Problem": ["bug","issue","error","not working","fails","failure","exception"],
#     "Configuration": ["salesforce","validation rule","workflow","page layout","permission set","field level","apex","trigger","flow","process builder"],
#     "Feature Request": ["feature request","enhancement","would like","request feature","add feature"],
#     "Client Project": ["implementation","go live","project plan","sprint","implementation"],
#     "Administrative": ["create user","reset password","license","deactivate user","profile change","permission"],
#     "Reporting": ["report","dashboard","analytics","kpi"]
# }
# 
# SUBTYPE_RULES: Dict[str, List[str]] = {
#     "ServiceDesk+ App": ["servicedesk","service desk","service-desk"],
#     "Email Template": ["email template","template","email template update"],
#     "SpringCM Project": ["springcm","docu","docusign","document generation"],
#     "Credit App": ["credit app","credit application"],
#     "Salesforce Project": ["salesforce project","deployment","release"]
# }
# 
# CATEGORY_RULES: Dict[str, List[str]] = {
#     "Client Training": ["training","walkthrough","demo","onboarding","enablement"],
#     "System Access": ["login","sso","access","permission","mfa","lockout","account locked"],
#     "Reporting": ["report","dashboard","report type","analytics"],
#     "Data Extraction": ["extract","export","etl","data export","loader"],
#     "Integration": ["integrat","api","webhook","endpoint","middleware","boomi","mule","workato"],
#     "File Management": ["file","document","attachment","library","springcm"],
#     "Case Management": ["case management","sla","case status"],
#     "Data": ["data quality","duplicate","dedupe","data import","cleanse"]
# }
# 
# def rule_score(text_lc: str, rules: Dict[str, List[str]]) -> Dict[str, int]:
#     scores = {k: 0 for k in rules.keys()}
#     for label, kws in rules.items():
#         for kw in kws:
#             if kw in text_lc:
#                 scores[label] += max(1, len(kw) // 4)
#     return scores
# 
# # ------------------ Semantic helpers ------------------
# model = None
# if USE_EMBEDDINGS:
#     try:
#         print("Loading sentence-transformers model (may take a minute)...")
#         model = SentenceTransformer('all-MiniLM-L6-v2')
#     except Exception:
#         print("Warning: sentence-transformers model failed to load. Falling back to fuzzy-only mode.")
#         model = None
#         USE_EMBEDDINGS = False
# 
# def embed_texts(texts: List[str]):
#     if not USE_EMBEDDINGS or model is None:
#         return None
#     return model.encode(texts, convert_to_tensor=True)
# 
# # ------------------ Matching helpers ------------------
# def fuzzy_choice_from_text(text: str, choices: List[str], threshold: int) -> Optional[str]:
#     if not text or not choices:
#         return None
#     match = process.extractOne(text, choices, scorer=fuzz.partial_token_set_ratio)
#     if match and match[1] >= threshold:
#         return match[0]
#     return None
# 
# def semantic_choice_from_text(text: str, choices: List[str], choices_embeddings, threshold: float) -> Optional[str]:
#     if not USE_EMBEDDINGS or model is None or choices_embeddings is None:
#         return None
#     try:
#         emb = model.encode(text, convert_to_tensor=True)
#         sims = util.cos_sim(emb, choices_embeddings)[0].cpu().numpy()
#         idx = int(np.argmax(sims))
#         score = float(sims[idx])
#         if score >= threshold:
#             return choices[idx]
#     except Exception:
#         return None
#     return None
# 
# def fuzzy_label_match(text: str, allowed: List[str], threshold: int = FUZZY_THRESHOLD_LABEL) -> Optional[str]:
#     if not text or not allowed:
#         return None
#     best = process.extractOne(text, allowed, scorer=fuzz.token_sort_ratio)
#     if best and best[1] >= threshold:
#         return best[0]
#     return None
# 
# def infer_label(text_raw: str, rules: Dict[str, List[str]], allowed: List[str], semantic_emb=None, semantic_thresh=SIMILARITY_THRESHOLD_LABEL) -> str:
#     t = normalize_text(text_raw)
#     if not t:
#         # return a reasonable default if available
#         for cand in allowed:
#             if cand:
#                 return cand
#         return allowed[0] if allowed else ""
#     # rules
#     scores = rule_score(t, rules)
#     best_label = max(scores, key=lambda k: scores[k])
#     if scores[best_label] > 0:
#         return best_label
#     # semantic
#     if semantic_emb is not None and USE_EMBEDDINGS and model is not None:
#         try:
#             emb = model.encode(t, convert_to_tensor=True)
#             sims = util.cos_sim(emb, semantic_emb)[0].cpu().numpy()
#             idx = int(np.argmax(sims))
#             best_score = float(sims[idx])
#             if best_score >= semantic_thresh:
#                 return allowed[idx]
#         except Exception:
#             pass
#     # fuzzy fallback
#     fuzzy = fuzzy_label_match(t, allowed, FUZZY_THRESHOLD_LABEL)
#     if fuzzy:
#         return fuzzy
#     # default fallback
#     for fallback in ["Miscellaneous Type", "Miscellaneous SubType", "Case Management"]:
#         if fallback in allowed:
#             return fallback
#     return allowed[0] if allowed else ""
# 
# # ------------------ Build maps ------------------
# def build_account_maps(df: pd.DataFrame) -> Tuple[Dict[str, List[Tuple[str,str]]], List[str]]:
#     if df.empty:
#         return {}, []
#     id_col = find_first_col(df.columns.tolist(), ["Id","ID","AccountId","Account Id","accountid"]) or df.columns[0]
#     name_col = find_first_col(df.columns.tolist(), ["Name","Account Name","AccountName","name"]) or (df.columns[1] if len(df.columns)>1 else df.columns[0])
#     norm_map: Dict[str, List[Tuple[str,str]]] = {}
#     choices: List[str] = []
#     for _, r in df.iterrows():
#         raw = str(r.get(name_col,"")).strip()
#         aid = str(r.get(id_col,"")).strip()
#         norm = normalize_company(raw)
#         if not norm:
#             continue
#         norm_map.setdefault(norm, []).append((aid, raw))
#         if norm not in choices:
#             choices.append(norm)
#     return norm_map, choices
# 
# def build_contact_maps(df: pd.DataFrame) -> Tuple[Dict[str, List[Tuple[str,str,str]]], List[str]]:
#     if df.empty:
#         return {}, []
#     id_col = find_first_col(df.columns.tolist(), ["Id","ID","ContactId","Contact Id","contactid"]) or df.columns[0]
#     full_col = find_first_col(df.columns.tolist(), ["FullName","Name","ContactName","Contact Name"])
#     first_c = find_first_col(df.columns.tolist(), ["FirstName","First Name","First"])
#     last_c = find_first_col(df.columns.tolist(), ["LastName","Last Name","Last"])
#     accid_c = find_first_col(df.columns.tolist(), ["AccountId","Account Id","Account_Id","AccountID"])
#     norm_map: Dict[str, List[Tuple[str,str,str]]] = {}
#     choices: List[str] = []
#     for _, r in df.iterrows():
#         cid = str(r.get(id_col,"")).strip()
#         if full_col:
#             raw_full = str(r.get(full_col,"")).strip()
#         else:
#             raw_full = (str(r.get(first_c,"")).strip() + " " + str(r.get(last_c,"")).strip()).strip()
#         accid = str(r.get(accid_c,"")).strip() if accid_c else ""
#         norm = normalize_person(raw_full)
#         if not norm:
#             continue
#         norm_map.setdefault(norm, []).append((cid, raw_full, accid))
#         if norm not in choices:
#             choices.append(norm)
#     return norm_map, choices
# 
# # ------------------ Main ------------------
# def main():
#     try:
#         if not TESTME_XLSX.exists():
#             print(f"ERROR: TESTME.xlsx not found at {TESTME_XLSX}")
#             return
# 
#         # load lookup tables (accounts/contacts optional)
#         accounts_df = pd.read_csv(ACCOUNTS_CSV, dtype=str).fillna("") if ACCOUNTS_CSV.exists() else pd.DataFrame()
#         contacts_df = pd.read_csv(CONTACTS_CSV, dtype=str).fillna("") if CONTACTS_CSV.exists() else pd.DataFrame()
# 
#         acc_map, acc_choices = build_account_maps(accounts_df)
#         con_map, con_choices = build_contact_maps(contacts_df)
# 
#         # optionally build embeddings for acc/contacts (semantic)
#         acc_emb = None
#         con_emb = None
#         if USE_EMBEDDINGS and model is not None and acc_choices:
#             try:
#                 acc_emb = embed_texts(acc_choices)
#             except Exception:
#                 acc_emb = None
#         if USE_EMBEDDINGS and model is not None and con_choices:
#             try:
#                 con_emb = embed_texts(con_choices)
#             except Exception:
#                 con_emb = None
# 
#         # precompute label embeddings
#         types_emb = subtypes_emb = cats_emb = None
#         if USE_EMBEDDINGS and model is not None:
#             try:
#                 types_emb = embed_texts(ALLOWED_TYPES)
#                 subtypes_emb = embed_texts(ALLOWED_SUBTYPES)
#                 cats_emb = embed_texts(ALLOWED_CATEGORIES)
#             except Exception:
#                 types_emb = subtypes_emb = cats_emb = None
# 
#         # load TESTME workbook
#         all_sheets = pd.read_excel(TESTME_XLSX, sheet_name=None, dtype=str, engine="openpyxl")
#         sheet_name = "Full Acc and Contact" if "Full Acc and Contact" in all_sheets else list(all_sheets.keys())[0]
#         cases_df = all_sheets[sheet_name].fillna("")
# 
#         cols = cases_df.columns.tolist()
#         acct_name_col = find_first_col(cols, ["Account Name","AccountName","Account","_Account_Name__c","Account_Name__c"])
#         contact_name_col = find_first_col(cols, ["Contact Name","ContactName","Contact","Contact FullName","_Contact_Name__c"])
#         summary_col = find_first_col(cols, ["Email Summary","_Email_Summary__c","Email_Summary__c","Email Summary","Subject"])
#         desc_col = find_first_col(cols, ["Description","_Description","Description__c","Body"])
# 
#         acct_id_out_col = find_first_col(cols, ["AccountId","Account Id","Account_Id"]) or "AccountId"
#         con_id_out_col = find_first_col(cols, ["ContactId","Contact Id","Contact_Id"]) or "ContactId"
# 
#         # ensure output columns exist (preserve both __c and plain if present)
#         for c in [acct_id_out_col, con_id_out_col, "Type", "Sub_Type__c", "Category__c", "Sub-Type", "Category"]:
#             if c not in cases_df.columns:
#                 cases_df[c] = ""
# 
#         ambiguous_rows: List[Dict[str,str]] = []
#         processed = 0
#         filled_acc = filled_con = 0
# 
#         total = len(cases_df)
#         print(f"Processing {total} rows...")
# 
#         for idx, row in cases_df.iterrows():
#             processed += 1
# 
#             summary_val = str(row.get(summary_col, "")) if summary_col else ""
#             desc_val = str(row.get(desc_col, "")) if desc_col else ""
#             combined_text = f"{summary_val} {desc_val}".strip()
#             combined_norm = normalize_text(combined_text)
# 
#             # clean visible fields for readability
#             if summary_col:
#                 cases_df.at[idx, summary_col] = summary_val.replace("_x000D_"," ").replace("\n"," ").strip()
#             if desc_col:
#                 cases_df.at[idx, desc_col] = desc_val.replace("_x000D_"," ").replace("\n"," ").strip()
# 
#             # ---------- Account matching ----------
#             existing_acc = str(row.get(acct_id_out_col, "")).strip()
#             matched_acc_id: Optional[str] = None
# 
#             acct_name_val = str(row.get(acct_name_col, "")).strip() if acct_name_col else ""
#             if acct_name_val:
#                 acct_norm = normalize_company(acct_name_val)
#                 if acct_norm and acct_norm in acc_map:
#                     matched_acc_id = acc_map[acct_norm][0][0]
#                 else:
#                     approx = fuzzy_choice_from_text(acct_name_val, acc_choices, NAME_FUZZY_STRICT)
#                     if approx:
#                         matched_acc_id = acc_map.get(approx)[0][0]
#                     else:
#                         if USE_EMBEDDINGS and acc_emb is not None:
#                             sem = semantic_choice_from_text(acct_name_val, acc_choices, acc_emb, SIMILARITY_THRESHOLD_ACCOUNT_CONTACT)
#                             if sem:
#                                 matched_acc_id = acc_map.get(sem)[0][0]
# 
#             if not matched_acc_id and combined_norm:
#                 approx2 = fuzzy_choice_from_text(combined_norm, acc_choices, NAME_FUZZY_FROM_TEXT)
#                 if approx2:
#                     matched_acc_id = acc_map.get(approx2)[0][0]
#                 else:
#                     if USE_EMBEDDINGS and acc_emb is not None:
#                         sem2 = semantic_choice_from_text(combined_norm, acc_choices, acc_emb, SIMILARITY_THRESHOLD_ACCOUNT_CONTACT)
#                         if sem2:
#                             matched_acc_id = acc_map.get(sem2)[0][0]
# 
#             if matched_acc_id and not existing_acc:
#                 cases_df.at[idx, acct_id_out_col] = matched_acc_id
#                 filled_acc += 1
# 
#             # ---------- Contact matching ----------
#             existing_con = str(row.get(con_id_out_col, "")).strip()
#             matched_con_id: Optional[str] = None
#             matched_con_acc: Optional[str] = None
# 
#             contact_name_val = str(row.get(contact_name_col, "")).strip() if contact_name_col else ""
#             if contact_name_val:
#                 c_norm = normalize_person(contact_name_val)
#                 if c_norm and c_norm in con_map:
#                     matched_con_id, raw_full, matched_con_acc = con_map[c_norm][0]
#                 else:
#                     approxc = fuzzy_choice_from_text(contact_name_val, con_choices, NAME_FUZZY_STRICT)
#                     if approxc:
#                         matched_con_id, raw_full, matched_con_acc = con_map.get(approxc)[0]
#                     else:
#                         if USE_EMBEDDINGS and con_emb is not None:
#                             semc = semantic_choice_from_text(contact_name_val, con_choices, con_emb, SIMILARITY_THRESHOLD_ACCOUNT_CONTACT)
#                             if semc:
#                                 matched_con_id, raw_full, matched_con_acc = con_map.get(semc)[0]
# 
#             if not matched_con_id and combined_norm:
#                 approxc2 = fuzzy_choice_from_text(combined_norm, con_choices, NAME_FUZZY_FROM_TEXT)
#                 if approxc2:
#                     matched_con_id, raw_full, matched_con_acc = con_map.get(approxc2)[0]
#                 else:
#                     if USE_EMBEDDINGS and con_emb is not None:
#                         semc2 = semantic_choice_from_text(combined_norm, con_choices, con_emb, SIMILARITY_THRESHOLD_ACCOUNT_CONTACT)
#                         if semc2:
#                             matched_con_id, raw_full, matched_con_acc = con_map.get(semc2)[0]
# 
#             if matched_con_id and not existing_con:
#                 cases_df.at[idx, con_id_out_col] = matched_con_id
#                 filled_con += 1
#                 # if account missing and contact row has accid, set it
#                 if not cases_df.at[idx, acct_id_out_col] and matched_con_acc:
#                     cases_df.at[idx, acct_id_out_col] = matched_con_acc
#                     filled_acc += 1
# 
#             # ---------- Classification (override) ----------
#             if combined_norm:
#                 # infer using rules -> semantic -> fuzzy
#                 chosen_type = infer_label(combined_text, TYPE_RULES, ALLOWED_TYPES, semantic_emb=types_emb if USE_EMBEDDINGS else None, semantic_thresh=SIMILARITY_THRESHOLD_LABEL)
#                 chosen_sub = infer_label(combined_text, SUBTYPE_RULES, ALLOWED_SUBTYPES, semantic_emb=subtypes_emb if USE_EMBEDDINGS else None, semantic_thresh=SIMILARITY_THRESHOLD_LABEL)
#                 chosen_cat = infer_label(combined_text, CATEGORY_RULES, ALLOWED_CATEGORIES, semantic_emb=cats_emb if USE_EMBEDDINGS else None, semantic_thresh=SIMILARITY_THRESHOLD_LABEL)
# 
#                 # override the fields as requested
#                 cases_df.at[idx, "Type"] = chosen_type
#                 cases_df.at[idx, "Sub_Type__c"] = chosen_sub
#                 cases_df.at[idx, "Category__c"] = chosen_cat
#                 if "Sub-Type" in cases_df.columns:
#                     cases_df.at[idx, "Sub-Type"] = chosen_sub
#                 if "Category" in cases_df.columns:
#                     cases_df.at[idx, "Category"] = chosen_cat
# 
#         # end for rows
# 
#         # write outputs
#         all_sheets[sheet_name] = cases_df
#         with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
#             for sname, df in all_sheets.items():
#                 df.to_excel(writer, sheet_name=sname, index=False)
#         print("Raw output written to:", OUTPUT_XLSX)
#         print(f"Rows processed: {processed}, AccountId filled: {filled_acc}, ContactId filled: {filled_con}")
# 
#         # cleaned Excel
#         cleaned = {}
#         for sname, df in all_sheets.items():
#             df2 = df.copy()
#             df2.rename(columns=lambda c: str(c).strip(), inplace=True)
#             for col in df2.columns:
#                 if df2[col].dtype == "object":
#                     df2[col] = (df2[col].astype(str)
#                                 .str.replace("_x000D_", " ", regex=False)
#                                 .str.replace("\r", " ", regex=False)
#                                 .str.replace("\n", " ", regex=False)
#                                 .str.replace("\t", " ", regex=False)
#                                 .str.strip())
#             cleaned[sname] = df2
#         with pd.ExcelWriter(CLEAN_OUTPUT_XLSX, engine="openpyxl") as writer:
#             for sname, df2 in cleaned.items():
#                 df2.to_excel(writer, sheet_name=sname, index=False)
#         print("Clean Excel written to:", CLEAN_OUTPUT_XLSX)
# 
#         # CSV for Salesforce
#         cleaned[sheet_name].to_csv(CLEAN_OUTPUT_CSV, index=False, encoding="utf-8")
#         print("Clean CSV written to:", CLEAN_OUTPUT_CSV)
# 
#         # ambiguous log
#         if ambiguous_rows:
#             pd.DataFrame(ambiguous_rows).to_csv(AMBIGUOUS_CSV, index=False)
#             print("Ambiguous matches written to:", AMBIGUOUS_CSV)
# 
#     except Exception as e:
#         print("Fatal error during processing:", e)
#         traceback.print_exc()
# 
# if __name__ == "__main__":
#     main()
#



#!/usr/bin/env python3
"""
map_ids_for_TESTME.py

Hybrid mapping + classifier with weighted Subject / Email Summary / Description prioritization.

Drop this file in the same folder as:
 - TESTME.xlsx
 - Accounts2.csv
 - Contacts2.csv

Click Run (no CLI args). Outputs:
 - TESTME_with_ids.xlsx
 - TESTME_with_ids_clean.xlsx
 - TESTME_with_ids_clean.csv
 - ambiguous_matches.csv
"""
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import re
import sys
import traceback

import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz

# optional semantic imports
USE_EMBEDDINGS = True
try:
    from sentence_transformers import SentenceTransformer, util
except Exception:
    USE_EMBEDDINGS = False
    SentenceTransformer = None
    util = None

# ---------------- Paths ----------------
BASE_DIR = Path(__file__).parent

TESTME_XLSX = BASE_DIR / "TESTME2.xlsx"
ACCOUNTS_CSV = BASE_DIR / "Accounts2.csv"
CONTACTS_CSV = BASE_DIR / "Contacts2.csv"

OUTPUT_XLSX = BASE_DIR / "TESTME_with_ids.xlsx"
CLEAN_OUTPUT_XLSX = BASE_DIR / "TESTME_with_ids_clean.xlsx"
CLEAN_OUTPUT_CSV = BASE_DIR / "TESTME_with_ids_clean.csv"
AMBIGUOUS_CSV = BASE_DIR / "ambiguous_matches.csv"

# ---------------- Thresholds ----------------
NAME_FUZZY_STRICT = 90
NAME_FUZZY_FROM_TEXT = 85
SIMILARITY_THRESHOLD_ACCOUNT_CONTACT = 0.80
SIMILARITY_THRESHOLD_LABEL = 0.55
FUZZY_THRESHOLD_LABEL = 75

# ---------------- Allowed lists ----------------
# ALLOWED_TYPES = [
#     'Administrative','App Development','Client Project','Configuration','Configuration Change',
#     'CPQ Issues','CSM Issues','Feature Request','Marketing','Miscellaneous Type',
#     'New Feature','Problem','Question','Sales Issues','Sales Non-CPQ Related Issues'
# ]
ALLOWED_TYPES = [
    "Administrative",
    "App Development",
    "Client Project",
    "Configuration",
    "Configuration Change",
    "CPQ Issues",
    "CSM Issues",
    "Feature Request",
    "Marketing",
    "Miscellaneous Type",
    "New Feature",
    "Problem",
    "Question",
    "Sales Issues",
    "Sales Non-CPQ Related Issues"
]
# ALLOWED_SUBTYPES = [
#     'ServiceDesk+ App','Credit App','Email Template','SpringCM Project','Salesforce Project','Miscellaneous SubType'
# ]

ALLOWED_SUBTYPES = [
    "ServiceDesk+ App",
    "Credit App",
    "Email Template",
    "SpringCM Project",
    "Salesforce Project",
    "Miscellaneous SubType"
]
# ALLOWED_CATEGORIES = [
#     'Client Training','System Access','Data Extraction','Planning','Integration','Reporting',
#     'Case Management','Stakeholder Management','Client Research','Project Scope','File Management','Data'
# ]
ALLOWED_CATEGORIES = [
    "Client Training",
    "System Access",
    "Data Extraction",
    "Planning",
    "Integration",
    "Reporting",
    "Case Management",
    "Stakeholder Management",
    "Client Research",
    "Project Scope",
    "File Management",
    "Data Mapping",
    "Sales Coordination",
    "System Setup",
    "Miscellaneous Category",
    "Administrative",
    "Documentation (BRD, SOW, etc.)",
    "Implementation and Recommendations",
    "Issue Resolution"
]
COMMON_COMPANY_SUFFIXES = {"inc","inc.","llc","l.l.c","ltd","co","co.","corp","corporation","company","incorporated","plc","llp"}

# ---------------- Normalizers ----------------
def normalize_text(val: object) -> str:
    if val is None:
        return ""
    s = str(val)
    s = s.replace("_x000D_", " ").replace("\r", " ").replace("\n", " ").replace("\t", " ")
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s\-\+]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_company(name: object) -> str:
    s = normalize_text(name)
    toks = [t for t in s.split() if t not in COMMON_COMPANY_SUFFIXES]
    return " ".join(toks)

def normalize_person(name: object) -> str:
    if name is None:
        return ""
    s = str(name).strip()
    if "," in s:
        parts = [p.strip() for p in s.split(",")]
        if len(parts) >= 2:
            s = parts[1] + " " + parts[0]
    return normalize_text(s)

def find_first_col(df_cols: List[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df_cols:
            return c
    lower_map = {col.lower(): col for col in df_cols}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None

def load_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, dtype=str).fillna("")
    return pd.read_excel(path, dtype=str, engine="openpyxl").fillna("")

# ---------------- Rules (keyword lists) ----------------
# TYPE_RULES: Dict[str, List[str]] = {
#     "CPQ Issues": ["cpq","sbqq","quote line","quote object"],
#     "Problem": ["bug","issue","error","not working","fails","failure","exception"],
#     "Configuration": ["salesforce","validation rule","workflow","page layout","permission set","field level","apex","trigger","flow","process builder"],
#     "Feature Request": ["feature request","enhancement","would like","request feature","add feature"],
#     "Client Project": ["implementation","go live","project plan","sprint","implementation"],
#     "Administrative": ["create user","reset password","license","deactivate user","profile change","permission"],
#     "Reporting": ["report","dashboard","analytics","kpi"]
# }
TYPE_RULES: Dict[str, List[str]] = {
    "CPQ Issues": [
        "cpq","sbqq","steelbrick","quote","quotes","pricing","price","pricebook","price book",
        "product configuration","configure product","product config","catalog","bundle","option",
        "quote line","quote-line","pricing error","price error","pricebook entry","quote adjustment","configure"
    ],
    "Problem": [
        "bug","issue","error","not working","fails","failure","exception","unable to","can't","cannot",
        "503","500 error","stack trace","crash","timeout","unexpected"
    ],
    "Configuration": [
        "salesforce","validation rule","workflow","page layout","permission set","permission","profile",
        "field level","apex","trigger","flow","process builder","metadata","custom field","record type",
        "object configuration","picklist","layout","permission set group","sharing rule"
    ],
    "Configuration Change": [
        "rename field","add field","change picklist","deploy","update layout","change page layout",
        "modify field","update record type","schema change","config change","configuration change"
    ],
    "Feature Request": [
        "feature request","enhancement","would like","request feature","add feature","enhancement request",
        "wish list","improve","improvement","new capability"
    ],
    "Client Project": [
        "implementation","go live","project plan","sprint","deployment","onboarding project","project kickoff",
        "statement of work","sow","project scope","transition plan"
    ],
    "App Development": [
        "integration","api","endpoint","webhook","sdk","developer","code","deploy","build","ci/cd","microservice",
        "app","application","service"
    ],
    "Administrative": [
        "create user","reset password","license","deactivate user","profile change","permission",
        "role change","org wide","org change","admin task","administrative","admin"
    ],
    "CSM Issues": [
        "renewal","success plan","health score","csm","customer success","renew","churn","billing dispute"
    ],
    "Sales Issues": [
        "lead","opportunity","pipeline","forecast","sales process","close date","deal","opp","opportunity"
    ],
    "Sales Non-CPQ Related Issues": [
        "pricing approval","discount approval","quote approval","contract approval","sales agreement","payment term"
    ],
    "Marketing": [
        "campaign","pardot","marketing cloud","utm","mailchimp","email campaign","lead source","marketing"
    ],
    "New Feature": [
        "new feature","new module","add module","introduce feature","launch feature"
    ],
    "Question": [
        "how do i","can we","is it possible","clarify","question","what is the","how to","help me","need help"
    ],
    "Miscellaneous Type": [
        # fallback, no explicit keywords
    ]
}

# SUBTYPE_RULES: Dict[str, List[str]] = {
#     "ServiceDesk+ App": ["servicedesk","service desk","service-desk"],
#     "Email Template": ["email template","template","email template update"],
#     "SpringCM Project": ["springcm","docu","docusign","document generation"],
#     "Credit App": ["credit app","credit application"],
#     "Salesforce Project": ["salesforce project","deployment","release"]
# }
SUBTYPE_RULES: Dict[str, List[str]] = {
    "ServiceDesk+ App": ["servicedesk","service desk","service-desk","sd+","servicedesk+"],
    "Credit App": ["credit app","credit application","credit-application","creditapp"],
    "Email Template": ["email template","template","email template update","html template","email body","email template change","email template update"],
    "SpringCM Project": ["springcm","spring cm","docu","docusign","document generation","content library"],
    "Salesforce Project": ["salesforce project","deployment","release","migration","rollback","salesforce project deploy"],
    "Miscellaneous SubType": []
}

# CATEGORY_RULES: Dict[str, List[str]] = {
#     "Client Training": ["training","walkthrough","demo","onboarding","enablement"],
#     "System Access": ["login","sso","access","permission","mfa","lockout","account locked"],
#     "Reporting": ["report","dashboard","report type","analytics"],
#     "Data Extraction": ["extract","export","etl","data export","loader"],
#     "Integration": ["integrat","api","webhook","endpoint","middleware","boomi","mule","workato"],
#     "File Management": ["file","document","attachment","library","springcm"],
#     "Case Management": ["case management","sla","case status"],
#     "Data": ["data quality","duplicate","dedupe","data import","cleanse"]
# }
CATEGORY_RULES: Dict[str, List[str]] = {
    "Client Training": ["training","walkthrough","demo","onboarding","enablement","training session","train"],
    "System Access": ["login","sso","access","permission","mfa","lockout","account locked","password reset","access denied"],
    "Data Extraction": ["extract","export","etl","data export","loader","data dump","data extract","data pull"],
    "Planning": ["plan","planning","roadmap","timeline","milestone","planning session"],
    "Integration": ["integrat","integration","api","webhook","endpoint","middleware","boomi","mule","workato","connector"],
    "Reporting": ["report","dashboard","report type","analytics","kpi","reporting","tableau","power bi","powerbi"],
    "Case Management": ["case management","sla","case status","case owner","case escalation"],
    "Stakeholder Management": ["stakeholder","raic","steering","communication plan","stakeholder update"],
    "Client Research": ["research","discovery","analysis","investigate","assessment","discovery call"],
    "Project Scope": ["scope","out of scope","change request","cr","scope change","requirements"],
    "File Management": ["file","document","attachment","library","springcm","content","document repository"],
    "Data Mapping": ["mapping","map fields","field map","data map","transform map","map import"],
    "Sales Coordination": ["sales coordination","sales ops","sales coordination","sales support","quote handoff"],
    "System Setup": ["setup","configure org","instance setup","initial setup","environment setup","sandbox setup"],
    "Miscellaneous Category": [],
    "Administrative": ["admin","administrative task","organizational","org admin"],
    "Documentation (BRD, SOW, etc.)": ["brd","sow","documentation","requirements doc","specification","design doc","proposal"],
    "Implementation and Recommendations": ["implementation","recommendation","recommendations","best practice","advice","suggested approach"],
    "Issue Resolution": ["resolve","resolution","fix","workaround","patch","hotfix","issue resolution"]
}

def rule_score_weighted(summary: str, subject: str, description: str, rules: Dict[str,List[str]], weights=(3,2,1)) -> Dict[str,int]:
    """
    Weighted rule scoring: keywords in summary count weights[0], subject weights[1], description weights[2].
    """
    s_summary = normalize_text(summary)
    s_subject = normalize_text(subject)
    s_desc = normalize_text(description)
    scores = {k:0 for k in rules.keys()}
    for label, kws in rules.items():
        for kw in kws:
            kw_l = kw.lower()
            if kw_l in s_summary:
                scores[label] += weights[0] * max(1, len(kw_l)//4)
            if kw_l in s_subject:
                scores[label] += weights[1] * max(1, len(kw_l)//4)
            if kw_l in s_desc:
                scores[label] += weights[2] * max(1, len(kw_l)//4)
    return scores

# ---------------- Semantic helpers (optional) ----------------
model = None
if USE_EMBEDDINGS:
    try:
        print("Loading sentence-transformers model (this may take a minute)...")
        model = SentenceTransformer('all-MiniLM-L6-v2')
    except Exception:
        print("Warning: failed to load sentence-transformers; continuing with fuzzy-only mode.")
        USE_EMBEDDINGS = False
        model = None

def embed_texts(texts: List[str]):
    if not USE_EMBEDDINGS or model is None:
        return None
    return model.encode(texts, convert_to_tensor=True)

# ---------------- Label inference with weighted text importance ----------------
def infer_label_weighted(summary: str, subject: str, description: str, rules: Dict[str,List[str]], allowed: List[str], semantic_emb=None, semantic_thresh=SIMILARITY_THRESHOLD_LABEL) -> str:
    """
    Priority: rules (weighted by summary/subject/desc) -> semantic on weighted combined text -> fuzzy fallback
    """
    # 1) rules weighted
    scores = rule_score_weighted(summary, subject, description, rules)
    best_label = max(scores, key=lambda k: scores[k])
    if scores[best_label] > 0:
        return best_label

    # 2) semantic on weighted combined text (weight summary higher by repeating)
    combined_weighted = " ".join([summary]*3 + [subject]*2 + [description])
    t_norm = normalize_text(combined_weighted)
    if semantic_emb is not None and USE_EMBEDDINGS and model is not None:
        try:
            emb = model.encode(t_norm, convert_to_tensor=True)
            sims = util.cos_sim(emb, semantic_emb)[0].cpu().numpy()
            idx = int(np.argmax(sims))
            best_score = float(sims[idx])
            if best_score >= semantic_thresh:
                return allowed[idx]
        except Exception:
            pass

    # 3) fuzzy fallback on weighted combined
    fuzzy = fuzzy_label_match(t_norm, allowed, FUZZY_THRESHOLD_LABEL)
    if fuzzy:
        return fuzzy

    # 4) final fallback
    # Try to pick a reasonable "Miscellaneous" / default label
    for fallback in ["Miscellaneous Type","Miscellaneous SubType","Case Management"]:
        if fallback in allowed:
            return fallback
    return allowed[0] if allowed else ""

# ---------------- fuzzy helper for labels ----------------
def fuzzy_label_match(text: str, allowed: List[str], threshold: int = FUZZY_THRESHOLD_LABEL) -> Optional[str]:
    if not text or not allowed:
        return None
    best = process.extractOne(text, allowed, scorer=fuzz.token_sort_ratio)
    if best and best[1] >= threshold:
        return best[0]
    return None

# ---------------- Account/contact mapping helpers ----------------
def build_account_maps(df: pd.DataFrame) -> Tuple[Dict[str, List[Tuple[str,str]]], List[str]]:
    if df.empty:
        return {}, []
    id_col = find_first_col(df.columns.tolist(), ["Id","ID","AccountId","Account Id","accountid"]) or df.columns[0]
    name_col = find_first_col(df.columns.tolist(), ["Name","Account Name","AccountName","name"]) or (df.columns[1] if len(df.columns)>1 else df.columns[0])
    norm_map: Dict[str, List[Tuple[str,str]]] = {}
    choices: List[str] = []
    for _, r in df.iterrows():
        raw = str(r.get(name_col,"")).strip()
        aid = str(r.get(id_col,"")).strip()
        norm = normalize_company(raw)
        if not norm:
            continue
        norm_map.setdefault(norm, []).append((aid, raw))
        if norm not in choices:
            choices.append(norm)
    return norm_map, choices

def build_contact_maps(df: pd.DataFrame) -> Tuple[Dict[str, List[Tuple[str,str,str]]], List[str]]:
    if df.empty:
        return {}, []
    id_col = find_first_col(df.columns.tolist(), ["Id","ID","ContactId","Contact Id","contactid"]) or df.columns[0]
    full_col = find_first_col(df.columns.tolist(), ["FullName","Name","ContactName","Contact Name"])
    first_c = find_first_col(df.columns.tolist(), ["FirstName","First Name","First"])
    last_c = find_first_col(df.columns.tolist(), ["LastName","Last Name","Last"])
    accid_c = find_first_col(df.columns.tolist(), ["AccountId","Account Id","Account_Id","AccountID"])
    norm_map: Dict[str, List[Tuple[str,str,str]]] = {}
    choices: List[str] = []
    for _, r in df.iterrows():
        cid = str(r.get(id_col,"")).strip()
        if full_col:
            raw_full = str(r.get(full_col,"")).strip()
        else:
            raw_full = (str(r.get(first_c,"")).strip() + " " + str(r.get(last_c,"")).strip()).strip()
        accid = str(r.get(accid_c,"")).strip() if accid_c else ""
        norm = normalize_person(raw_full)
        if not norm:
            continue
        norm_map.setdefault(norm, []).append((cid, raw_full, accid))
        if norm not in choices:
            choices.append(norm)
    return norm_map, choices

def fuzzy_choice_from_text(text: str, choices: List[str], threshold: int) -> Optional[str]:
    if not text or not choices:
        return None
    best = process.extractOne(text, choices, scorer=fuzz.partial_token_set_ratio)
    if best and best[1] >= threshold:
        return best[0]
    return None

def semantic_choice_from_text(text: str, choices: List[str], choices_embeddings, threshold: float) -> Optional[str]:
    if not USE_EMBEDDINGS or model is None or choices_embeddings is None:
        return None
    try:
        emb = model.encode(text, convert_to_tensor=True)
        sims = util.cos_sim(emb, choices_embeddings)[0].cpu().numpy()
        idx = int(np.argmax(sims))
        score = float(sims[idx])
        if score >= threshold:
            return choices[idx]
    except Exception:
        return None
    return None

# ---------------- Main ----------------
def main():
    try:
        if not TESTME_XLSX.exists():
            print(f"ERROR: TESTME.xlsx not found at {TESTME_XLSX}")
            return

        accounts_df = load_table(ACCOUNTS_CSV) if ACCOUNTS_CSV.exists() else pd.DataFrame()
        contacts_df = load_table(CONTACTS_CSV) if CONTACTS_CSV.exists() else pd.DataFrame()

        account_norm_map, account_choices = build_account_maps(accounts_df)
        contact_norm_map, contact_choices = build_contact_maps(contacts_df)

        # optional embeddings for account/contact names
        account_embeddings = embed_texts(account_choices) if USE_EMBEDDINGS and model is not None and account_choices else None
        contact_embeddings = embed_texts(contact_choices) if USE_EMBEDDINGS and model is not None and contact_choices else None

        # label embeddings
        types_emb = embed_texts(ALLOWED_TYPES) if USE_EMBEDDINGS and model is not None else None
        subtypes_emb = embed_texts(ALLOWED_SUBTYPES) if USE_EMBEDDINGS and model is not None else None
        cats_emb = embed_texts(ALLOWED_CATEGORIES) if USE_EMBEDDINGS and model is not None else None

        # load TESTME workbook
        all_sheets = pd.read_excel(TESTME_XLSX, sheet_name=None, dtype=str, engine="openpyxl")
        sheet_name = "Full Acc and Contact" if "Full Acc and Contact" in all_sheets else list(all_sheets.keys())[0]
        cases_df = all_sheets[sheet_name].fillna("")

        cols = cases_df.columns.tolist()
        acct_name_col = find_first_col(cols, ["Account Name","AccountName","Account","_Account_Name__c","Account_Name__c"])
        contact_name_col = find_first_col(cols, ["Contact Name","ContactName","Contact","Contact FullName","_Contact_Name__c"])
        summary_col = find_first_col(cols, ["Email Summary","_Email_Summary__c","Email_Summary__c","Email Summary","Summary","Email Subject"])
        subject_col = find_first_col(cols, ["Subject","Case Subject","Email_Subject__c"])
        desc_col = find_first_col(cols, ["Description","_Description","Description__c","Body","Email Body"])

        acct_id_out_col = find_first_col(cols, ["AccountId","Account Id","Account_Id"]) or "AccountId"
        con_id_out_col = find_first_col(cols, ["ContactId","Contact Id","Contact_Id"]) or "ContactId"

        for c in [acct_id_out_col, con_id_out_col, "Type", "Sub_Type__c", "Category__c", "Sub-Type", "Category"]:
            if c not in cases_df.columns:
                cases_df[c] = ""

        ambiguous_rows: List[Dict[str,str]] = []
        processed = 0
        filled_acc = filled_con = 0

        print(f"Processing {len(cases_df)} rows...")

        # iterate rows
        for idx, row in cases_df.iterrows():
            processed += 1

            summary_val = str(row.get(summary_col, "")) if summary_col else ""
            subject_val = str(row.get(subject_col, "")) if subject_col else ""
            desc_val = str(row.get(desc_col, "")) if desc_col else ""
            combined_text = f"{summary_val} {subject_val} {desc_val}".strip()

            # clean visible fields
            if summary_col:
                cases_df.at[idx, summary_col] = summary_val.replace("_x000D_", " ").replace("\n", " ").strip()
            if subject_col:
                cases_df.at[idx, subject_col] = subject_val.replace("_x000D_", " ").replace("\n", " ").strip()
            if desc_col:
                cases_df.at[idx, desc_col] = desc_val.replace("_x000D_", " ").replace("\n", " ").strip()

            combined_norm = normalize_text(combined_text)

            # ---------- Account matching ----------
            existing_acc = str(row.get(acct_id_out_col, "")).strip()
            matched_acc_id: Optional[str] = None

            acct_name_val = str(row.get(acct_name_col, "")).strip() if acct_name_col else ""
            if acct_name_val:
                acct_norm = normalize_company(acct_name_val)
                if acct_norm and acct_norm in account_norm_map:
                    matched_acc_id = account_norm_map[acct_norm][0][0]
                else:
                    approx = fuzzy_choice_from_text(acct_name_val, account_choices, NAME_FUZZY_STRICT)
                    if approx:
                        matched_acc_id = account_norm_map.get(approx)[0][0]
                    else:
                        if USE_EMBEDDINGS and account_embeddings is not None:
                            sem = semantic_choice_from_text(acct_name_val, account_choices, account_embeddings, SIMILARITY_THRESHOLD_ACCOUNT_CONTACT)
                            if sem:
                                matched_acc_id = account_norm_map.get(sem)[0][0]

            if not matched_acc_id and combined_norm:
                approx2 = fuzzy_choice_from_text(combined_norm, account_choices, NAME_FUZZY_FROM_TEXT)
                if approx2:
                    matched_acc_id = account_norm_map.get(approx2)[0][0]
                else:
                    if USE_EMBEDDINGS and account_embeddings is not None:
                        sem2 = semantic_choice_from_text(combined_norm, account_choices, account_embeddings, SIMILARITY_THRESHOLD_ACCOUNT_CONTACT)
                        if sem2:
                            matched_acc_id = account_norm_map.get(sem2)[0][0]

            if matched_acc_id and not existing_acc:
                cases_df.at[idx, acct_id_out_col] = matched_acc_id
                filled_acc += 1

            # ---------- Contact matching ----------
            existing_con = str(row.get(con_id_out_col, "")).strip()
            matched_con_id: Optional[str] = None
            matched_con_acc: Optional[str] = None

            contact_name_val = str(row.get(contact_name_col, "")).strip() if contact_name_col else ""
            if contact_name_val:
                c_norm = normalize_person(contact_name_val)
                if c_norm and c_norm in contact_norm_map:
                    matched_con_id, _raw, matched_con_acc = contact_norm_map[c_norm][0]
                else:
                    approxc = fuzzy_choice_from_text(contact_name_val, contact_choices, NAME_FUZZY_STRICT)
                    if approxc:
                        matched_con_id, _raw, matched_con_acc = contact_norm_map.get(approxc)[0]
                    else:
                        if USE_EMBEDDINGS and contact_embeddings is not None:
                            semc = semantic_choice_from_text(contact_name_val, contact_choices, contact_embeddings, SIMILARITY_THRESHOLD_ACCOUNT_CONTACT)
                            if semc:
                                matched_con_id, _raw, matched_con_acc = contact_norm_map.get(semc)[0]

            if not matched_con_id and combined_norm:
                approxc2 = fuzzy_choice_from_text(combined_norm, contact_choices, NAME_FUZZY_FROM_TEXT)
                if approxc2:
                    matched_con_id, _raw, matched_con_acc = contact_norm_map.get(approxc2)[0]
                else:
                    if USE_EMBEDDINGS and contact_embeddings is not None:
                        semc2 = semantic_choice_from_text(combined_norm, contact_choices, contact_embeddings, SIMILARITY_THRESHOLD_ACCOUNT_CONTACT)
                        if semc2:
                            matched_con_id, _raw, matched_con_acc = contact_norm_map.get(semc2)[0]

            if matched_con_id and not existing_con:
                cases_df.at[idx, con_id_out_col] = matched_con_id
                filled_con += 1
                if not cases_df.at[idx, acct_id_out_col] and matched_con_acc:
                    cases_df.at[idx, acct_id_out_col] = matched_con_acc
                    filled_acc += 1

            # ---------- Classification: PRIORITIZE Email Summary > Subject > Description ----------
            # use weighted rules and weighted combined text (summary repeated)
            if any([summary_val.strip(), subject_val.strip(), desc_val.strip()]):
                chosen_type = infer_label_weighted(summary_val, subject_val, desc_val, TYPE_RULES, ALLOWED_TYPES, semantic_emb=types_emb if USE_EMBEDDINGS else None, semantic_thresh=SIMILARITY_THRESHOLD_LABEL)
                chosen_sub  = infer_label_weighted(summary_val, subject_val, desc_val, SUBTYPE_RULES, ALLOWED_SUBTYPES, semantic_emb=subtypes_emb if USE_EMBEDDINGS else None, semantic_thresh=SIMILARITY_THRESHOLD_LABEL)
                chosen_cat  = infer_label_weighted(summary_val, subject_val, desc_val, CATEGORY_RULES, ALLOWED_CATEGORIES, semantic_emb=cats_emb if USE_EMBEDDINGS else None, semantic_thresh=SIMILARITY_THRESHOLD_LABEL)

                # override the fields
                cases_df.at[idx, "Type"] = chosen_type
                cases_df.at[idx, "Sub_Type__c"] = chosen_sub
                cases_df.at[idx, "Category__c"] = chosen_cat
                if "Sub-Type" in cases_df.columns:
                    cases_df.at[idx, "Sub-Type"] = chosen_sub
                if "Category" in cases_df.columns:
                    cases_df.at[idx, "Category"] = chosen_cat

        # end loop rows

        # save outputs
        all_sheets[sheet_name] = cases_df
        with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
            for sname, df in all_sheets.items():
                df.to_excel(writer, sheet_name=sname, index=False)
        print("Raw Excel written to:", OUTPUT_XLSX)

        # cleaned version
        cleaned = {}
        for sname, df in all_sheets.items():
            df2 = df.copy()
            df2.rename(columns=lambda c: str(c).strip(), inplace=True)
            for col in df2.columns:
                if df2[col].dtype == "object":
                    df2[col] = (df2[col].astype(str)
                                .str.replace("_x000D_", " ", regex=False)
                                .str.replace("\r", " ", regex=False)
                                .str.replace("\n", " ", regex=False)
                                .str.replace("\t", " ", regex=False)
                                .str.strip())
            cleaned[sname] = df2
        with pd.ExcelWriter(CLEAN_OUTPUT_XLSX, engine="openpyxl") as writer:
            for sname, df2 in cleaned.items():
                df2.to_excel(writer, sheet_name=sname, index=False)
        print("Clean Excel written to:", CLEAN_OUTPUT_XLSX)

        # CSV for Salesforce
        cleaned[sheet_name].to_csv(CLEAN_OUTPUT_CSV, index=False, encoding="utf-8")
        print("Clean CSV written to:", CLEAN_OUTPUT_CSV)

        # ambiguous log (if any)
        if ambiguous_rows:
            pd.DataFrame(ambiguous_rows).to_csv(AMBIGUOUS_CSV, index=False)
            print("Ambiguous matches written to:", AMBIGUOUS_CSV)

    except Exception as e:
        print("Fatal error:", e)
        traceback.print_exc()

if __name__ == "__main__":
    main()
