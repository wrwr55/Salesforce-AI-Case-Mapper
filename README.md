BEFORE STARTING MAKE SURE THAT AUTO CREATED CASES ARE DELETED
IF YOU ALREADY DELETED ALL OF THE AUTO CREATED CASES YOU CAN SKIP THIS PART
To do this navigate deleteAutoCases.txt file and copy the contents
Then navigate to the Salesforce inspector and select Data Export
There, delete any other text and paste the contents of that file and run the query
Select the button that says "Copy (Excel)"
Then go to Data Import in the Salesforce inspector and MAKE SURE THE OBJECT IS SET TO CASE and change the Action from the default to Delete
Then paste the data into the box that says "Paste data here" and then select "Run Insert"
ONCE FINISHED YOU CAN PROCEED TO THE NEXT STEPS

Create a working folder for files
In the Salesforce enviroment go into the Salesforce Inspector and go to Export Data

Inside of the accountQuery.txt, contactQuery.txt, and caseInfoQuery.txt you will see queries to run
For each file copy the contained text and paste in the query box and make sure to delete any text that was there before
Then select "Run Export" and select the "Copy (CSV)" button
Paste the copied CSV content into the respective file from this repo (ie. contactQuert.txt would go into Contacts.csv)
Once finished all three make sure that these files are all saved to the same folder and run the main file









































````markdown
# Salesforce ID Mapper

A lightweight toolkit to populate missing `AccountId` and `ContactId` values in Case records by matching Case-level account/contact names to authoritative Accounts and Contacts exports. Designed for Windows-friendly workflows and large datasets — exact-normalized matching first, then fast fuzzy fallback (RapidFuzz). Outputs are ready for Salesforce upsert (Data Loader / CLI).

---

## Features

- Read Cases from **Excel** (`.xlsx`) or **NDJSON** (newline-delimited JSON) / JSON array.  
- Read Accounts and Contacts from **CSV** or **Excel**.  
- Normalize names (strip punctuation, collapse common suffixes like `Inc`, `LLC`) for robust matching.  
- Exact match first → fuzzy match (RapidFuzz) fallback with configurable threshold.  
- Prefer matches that produce **both** `AccountId` and `ContactId` when possible.  
- Produces:
  - `TESTME_with_ids.xlsx` — updated workbook (preserves other sheets).
  - `ambiguous_matches.csv` — low-confidence matches for manual review.
  - Optional NDJSON output for streaming workflows.
- One-click `.bat` runner for Windows.

---

## Quick Start (Windows)

### 1. Prereqs
- Python 3.10+ recommended.  
- PowerShell (Windows) or a terminal.  
- Recommended working folder: `C:\Users\YOUR_USERNAME\Desktop\USER_FOLDER`

> **Note:** Wherever you see `YOUR_USERNAME` below, replace it with your actual system username, or use environment variables like `%USERPROFILE%` on Windows or `~` on macOS/Linux.

### 2. Install dependencies
Open PowerShell and run:

```powershell
cd "%USERPROFILE%\Desktop\USER_FOLDER"
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
````

`requirements.txt` should include:

```
pandas
openpyxl
rapidfuzz
```

If you have legacy `.xls` files:

```powershell
pip install xlrd
```

---

## Files you need (place in one folder)

* `TESTME.xlsx` — case workbook (sheet **Full Acc and Contact** preferred; otherwise first sheet used)
* `Accounts.csv` — accounts export (must include `Id` and `Name` columns)
  * To get this file go into the salesforce inspector and run a query that gets the Id and Name for the Account object and download the results
* `contacts.csv` — contacts export (must include `Id` and `FirstName`/`LastName` or `FullName`; optional `AccountId` column is helpful)
* `scripts/map_ids_for_TESTME.py` — convenience script (provided)
* `scripts/map_ids_to_cases.py` — streaming / NDJSON-capable script (optional)
* `scripts/convert_excel_to_ndjson.py` — helper to convert Excel to NDJSON (optional)

Place these files in the same folder (e.g. `C:\Users\YOUR_USERNAME\Desktop\USER_FOLDER`) or edit the scripts to point at whatever folder you prefer.

---

## One-click (recommended)

Create `run_map_TESTME.bat` in the same folder (example content below). Double-clicking this file will run the convenience mapping script and pause so you can see the output.

```bat
@echo off
REM Change to your downloads folder or where the files are stored
cd /d "%USERPROFILE%\Desktop\USER_FOLDER"

REM Activate venv (optional)
REM call "%~dp0\.venv\Scripts\activate.bat"

REM Run the convenience mapping script
python "scripts\map_ids_for_TESTME.py"

echo.
echo ✅ Done - check TESTME_with_ids.xlsx and ambiguous_matches.csv
pause
```

If you prefer a macOS / Linux one-liner, run the Python script directly in your terminal:

```bash
python3 scripts/map_ids_for_TESTME.py
```

---

## Manual (explicit commands)

Open PowerShell and go to your folder:

```powershell
cd "%USERPROFILE%\Desktop\USER_FOLDER"
```

Run the convenience script (reads `TESTME.xlsx`, `Accounts.csv`, `contacts.csv`):

```powershell
python scripts\map_ids_for_TESTME.py
```

### Optional: Convert Excel → NDJSON for streaming (recommended for very large files)

1. Convert the Excel sheet to NDJSON:

```powershell
python scripts\convert_excel_to_ndjson.py "TESTME.xlsx" "TESTME_ndjson.json" "Full Acc and Contact"
```

2. Run the streaming mapper:

```powershell
python scripts\map_ids_to_cases.py --input "TESTME_ndjson.json" --accounts "Accounts.csv" --contacts "contacts.csv" --output "TESTME_with_ids.ndjson" --fuzzy-threshold 85
```

* Use `--input-format array` if your input is a single JSON array file.
* Add `--write-array` if you want a single JSON array output instead of NDJSON.

---

## Important options & config

* **Fuzzy threshold**: default `85`. Increase to be stricter (90–95), lower to be more permissive (70–80).

  * For `map_ids_to_cases.py` pass `--fuzzy-threshold <int>`.
  * For `map_ids_for_TESTME.py` edit `FUZZY_THRESHOLD` at the top of that script.
* **Sheet name**: `map_ids_for_TESTME.py` uses sheet `Full Acc and Contact` if present; otherwise the first sheet is used. Rename your sheet or edit the script if needed.
* **Column names**: scripts detect common header names (`Id`, `Name`, `FirstName`, `LastName`, `FullName`). If your CSV/Excel uses different headers, either rename the columns or edit the script’s header candidate lists.

---

## How matching works (brief)

1. **Normalize names**:

   * Company: remove punctuation, lowercase, remove common suffixes (`Inc`, `LLC`, etc.).
   * Person: convert `"Last, First"` → `"First Last"`, strip punctuation and lowercase.

2. **Priority**:

   * If a contact record in `contacts.csv` contains an `AccountId`, prefer that Account for matching cases where a similar contact is found.
   * Exact normalized match → use the matching ID.
   * If exact fails → fuzzy match (RapidFuzz token sort ratio) with the configured threshold.
   * Ambiguous/low-score matches are logged to `ambiguous_matches.csv` for review.

---

## Outputs

* `TESTME_with_ids.xlsx` — updated workbook with `AccountId` and `ContactId` populated (preserves other sheets).
* `ambiguous_matches.csv` — log of low-score fuzzy matches you should review.
* NDJSON/JSON variants if you use the streaming mapper.

---

## Troubleshooting

* **`ValueError: Excel file format cannot be determined`** — ensure the file extension matches the actual format. If your file is `.xls`, either convert it to `.xlsx` or install/force `xlrd` in the script (and `pip install xlrd`).
* **Pandas engine errors** — ensure `openpyxl` is installed for `.xlsx` files: `pip install openpyxl`.
* **Missing columns** — open the CSV/Excel and verify exact header names; edit the script if your headers differ.
* **Fuzzy matches too aggressive** — increase `--fuzzy-threshold` (e.g., 90).
* **Too few fuzzy matches** — lower the threshold (e.g., 80) but review `ambiguous_matches.csv`.

---

## Tests & CI

* A basic `tests/` folder with unit tests for normalization helpers is included.
* A sample GitHub Actions workflow (`.github/workflows/python-app.yml`) is provided to run tests on push / PR.

---

## Security & privacy

* These scripts are local-only — no data is sent externally.
* Do **not** store Salesforce credentials in these scripts.
* Always validate `ambiguous_matches.csv` before performing any upsert into Salesforce.

---

## Contributing

1. Fork the repo.
2. Create a feature branch.
3. Add tests for new behavior.
4. Submit a pull request with a clear description.

