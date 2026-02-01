import streamlit as st
import pandas as pd
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import urllib.request
import smtplib
from email.message import EmailMessage
import time
import re
import io
import base64

import requests

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# streamlit_app.py
# Streamlit app: search StudyID across multiple Google Sheets tabs (read-only via API key)
#
# Expected Google Sheet tabs:
#   cocaine, cannabis, HIV-neg-nondrug, HIV+nondrug, boxNumber
#
# Secrets needed in Streamlit Cloud:
#   GOOGLE_API_KEY = "AIza..."
#   SPREADSHEET_ID = "1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
#
# Notes:
# - API key access works if the sheet is public or shared appropriately for API-key read access.
# - If your sheet is private and API key fails (403), you need a Service Account instead.

import re
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

st.set_page_config(page_title="StudyID Lookup", layout="wide")
st.title("🔎 StudyID Lookup (Google Sheets)")

# ---------- Config ----------
TABS_TO_SEARCH = ["cocaine", "cannabis", "HIV-neg-nondrug", "HIV+nondrug"]
BOX_TAB = "boxNumber"

FIELDS_TO_SHOW = [
    "BoxNumber",
    "StudyID",
    "Date collected",
    "Samples Received",
    "Missing Samples",
    "Group",
    "Urine results",
    "All collected by AH",
]

# ---------- Helpers ----------
def _normalize_header(s: str) -> str:
    """Normalize headers to reduce mismatch from extra spaces/casing."""
    if s is None:
        return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _normalize_studyid(s: str) -> str:
    return re.sub(r"\s+", "", str(s or "")).strip().upper()

@st.cache_resource(show_spinner=False)
def get_sheets_service(api_key: str):
    return build("sheets", "v4", developerKey=api_key, cache_discovery=False)

@st.cache_data(ttl=300, show_spinner=False)
def read_tab_as_df(spreadsheet_id: str, tab_name: str, api_key: str) -> pd.DataFrame:
    """
    Read a whole tab into a DataFrame (assumes first row is header).
    Uses a wide range to accommodate columns without knowing exact width.
    """
    service = get_sheets_service(api_key)
    # Wide range; adjust if you know your max column
    rng = f"'{tab_name}'!A1:ZZ"

    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=rng,
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()

    values = resp.get("values", [])
    if not values:
        return pd.DataFrame()

    header = [_normalize_header(h) for h in values[0]]
    rows = values[1:]

    # Make rows same length as header
    max_len = len(header)
    fixed_rows = []
    for r in rows:
        r = list(r)
        if len(r) < max_len:
            r += [""] * (max_len - len(r))
        elif len(r) > max_len:
            r = r[:max_len]
        fixed_rows.append(r)

    df = pd.DataFrame(fixed_rows, columns=header)
    return df

def build_box_map(box_df: pd.DataFrame) -> dict:
    """
    Build StudyID -> BoxNumber mapping from the 'boxNumber' tab.
    Tries common column header names; falls back to best-effort.
    """
    if box_df.empty:
        return {}

    # Try exact headers first
    possible_study_cols = ["StudyID", "Study Id", "Study ID", "ID"]
    possible_box_cols = ["BoxNumber", "Box Number", "Box#", "Box #", "Box"]

    study_col = next((c for c in possible_study_cols if c in box_df.columns), None)
    box_col = next((c for c in possible_box_cols if c in box_df.columns), None)

    # If not found, attempt fuzzy by normalized header
    if study_col is None or box_col is None:
        norm_to_col = {re.sub(r"[^a-z0-9]+", "", c.lower()): c for c in box_df.columns}
        if study_col is None:
            for k in ["studyid", "studyid#", "studyidid", "id"]:
                if k in norm_to_col:
                    study_col = norm_to_col[k]
                    break
        if box_col is None:
            for k in ["boxnumber", "box", "boxno", "boxnum", "box#"]:
                if k in norm_to_col:
                    box_col = norm_to_col[k]
                    break

    if study_col is None or box_col is None:
        return {}

    m = {}
    for _, row in box_df.iterrows():
        sid = _normalize_studyid(row.get(study_col, ""))
        bx = row.get(box_col, "")
        if sid:
            m[sid] = bx
    return m

def extract_fields(row: pd.Series, box_map: dict) -> dict:
    """
    Create an output dict with requested fields.
    Works even if some columns are missing in the source tab.
    """
    out = {}

    # BoxNumber from mapping (preferred) else from row if exists
    sid_norm = _normalize_studyid(row.get("StudyID", ""))
    box_from_map = box_map.get(sid_norm, "")
    out["BoxNumber"] = box_from_map if box_from_map != "" else row.get("BoxNumber", "")

    # Remaining fields from row
    for f in FIELDS_TO_SHOW:
        if f == "BoxNumber":
            continue
        out[f] = row.get(f, "")

    return out

def find_studyid_in_tabs(studyid: str, tabs: list[str], spreadsheet_id: str, api_key: str, box_map: dict) -> pd.DataFrame:
    sid_norm = _normalize_studyid(studyid)
    hits = []

    for tab in tabs:
        df = read_tab_as_df(spreadsheet_id, tab, api_key)
        if df.empty:
            continue

        # Ensure StudyID exists
        if "StudyID" not in df.columns:
            continue

        df["_StudyID_norm"] = df["StudyID"].apply(_normalize_studyid)
        sub = df[df["_StudyID_norm"] == sid_norm].copy()
        if sub.empty:
            continue

        # Create outputs
        for _, r in sub.iterrows():
            rec = extract_fields(r, box_map)
            rec["SourceTab"] = tab  # helpful context
            hits.append(rec)

    if not hits:
        return pd.DataFrame()

    out_df = pd.DataFrame(hits)

    # Ensure output columns order
    ordered_cols = ["SourceTab"] + FIELDS_TO_SHOW
    for c in ordered_cols:
        if c not in out_df.columns:
            out_df[c] = ""
    out_df = out_df[ordered_cols]

    return out_df

# ---------- Secrets ----------
api_key = st.secrets.get("GOOGLE_API_KEY", "")
spreadsheet_id = st.secrets.get("SPREADSHEET_ID", "")

if not api_key or not spreadsheet_id:
    st.error("Missing secrets. Add GOOGLE_API_KEY and SPREADSHEET_ID in Streamlit Secrets.")
    st.stop()

# ---------- UI ----------
with st.sidebar:
    st.subheader("Settings")
    st.write("Searching tabs:")
    st.write(TABS_TO_SEARCH)
    st.write("Box mapping tab:")
    st.write(BOX_TAB)

studyid = st.text_input("Enter StudyID", placeholder="e.g., S1234").strip()

col1, col2 = st.columns([1, 3])
with col1:
    do_search = st.button("Search", type="primary", use_container_width=True)
with col2:
    st.caption("Tip: StudyID matching ignores spaces and is case-insensitive.")

# ---------- Search ----------
if do_search:
    if not studyid:
        st.warning("Please enter a StudyID.")
        st.stop()

    try:
        with st.spinner("Loading boxNumber mapping..."):
            box_df = read_tab_as_df(spreadsheet_id, BOX_TAB, api_key)
            box_map = build_box_map(box_df)

        with st.spinner("Searching StudyID across tabs..."):
            results = find_studyid_in_tabs(
                studyid=studyid,
                tabs=TABS_TO_SEARCH,
                spreadsheet_id=spreadsheet_id,
                api_key=api_key,
                box_map=box_map,
            )

        if results.empty:
            st.info(f"No results found for StudyID = {studyid}")
        else:
            st.success(f"Found {len(results)} record(s).")
            st.dataframe(results, use_container_width=True, hide_index=True)

    except HttpError as e:
        st.error("Google Sheets API error.")
        st.code(str(e), language="text")
        st.info(
            "If you see 403/permission errors, your sheet is likely not accessible via API key. "
            "In that case, switch to a Service Account (recommended for private sheets)."
        )
    except Exception as e:
        st.error("Unexpected error.")
        st.code(str(e), language="text")
