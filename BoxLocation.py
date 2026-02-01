# BoxLocation.py (revised, clean, no duplicate imports)
# Streamlit: user enters StudyID -> show BoxNumber + requested fields
# Reads from Google Sheets via API key (developerKey).
#
# Tabs: cocaine, cannabis, HIV-neg-nondrug, HIV+nondrug, boxNumber
#
# Streamlit Secrets:
spreadsheet = "1ATbGvDHey-0sGeEP12TBE9bRaPemII-E92-GQpOJ3kc"
GOOGLE_API_KEY = "AIzaSyCPhrEngtBqF3-jVjfme2gGhR5XcAe9DU0"
st.write("Secrets loaded:", bool(GOOGLE_API_KEY), bool(spreadsheet_id))


import re
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError




st.set_page_config(page_title="StudyID Lookup", layout="wide")
st.title("🔎 StudyID Lookup (Google Sheets)")

# -------------------- Config --------------------
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

# -------------------- Helpers --------------------
def norm_header(x: str) -> str:
    x = "" if x is None else str(x)
    x = re.sub(r"\s+", " ", x.strip())
    return x

def norm_studyid(x: str) -> str:
    x = "" if x is None else str(x)
    return re.sub(r"\s+", "", x.strip()).upper()

@st.cache_resource(show_spinner=False)
def sheets_service(api_key: str):
    return build("sheets", "v4", developerKey=api_key, cache_discovery=False)

@st.cache_data(ttl=300, show_spinner=False)
def read_tab(spreadsheet_id: str, tab_name: str, api_key: str) -> pd.DataFrame:
    """
    Read a whole tab (A1:ZZ) into DataFrame. Assumes first row is header.
    """
    svc = sheets_service(api_key)
    rng = f"'{tab_name}'!A1:ZZ"

    resp = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=rng,
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()

    values = resp.get("values", [])
    if not values:
        return pd.DataFrame()

    header = [norm_header(h) for h in values[0]]
    rows = values[1:]

    # pad/truncate each row to header length
    n = len(header)
    fixed = []
    for r in rows:
        r = list(r)
        if len(r) < n:
            r += [""] * (n - len(r))
        elif len(r) > n:
            r = r[:n]
        fixed.append(r)

    return pd.DataFrame(fixed, columns=header)

def build_box_map(box_df: pd.DataFrame) -> dict:
    """
    Build mapping: normalized StudyID -> BoxNumber
    Accepts a variety of column header spellings.
    """
    if box_df.empty:
        return {}

    study_candidates = ["StudyID", "Study ID", "Study Id", "ID"]
    box_candidates = ["BoxNumber", "Box Number", "Box#", "Box #", "Box"]

    study_col = next((c for c in study_candidates if c in box_df.columns), None)
    box_col = next((c for c in box_candidates if c in box_df.columns), None)

    # Fuzzy fallback
    if study_col is None or box_col is None:
        norm_map = {re.sub(r"[^a-z0-9]+", "", c.lower()): c for c in box_df.columns}
        if study_col is None:
            for key in ["studyid", "id"]:
                if key in norm_map:
                    study_col = norm_map[key]
                    break
        if box_col is None:
            for key in ["boxnumber", "boxno", "boxnum", "box"]:
                if key in norm_map:
                    box_col = norm_map[key]
                    break

    if study_col is None or box_col is None:
        return {}

    m = {}
    for _, r in box_df.iterrows():
        sid = norm_studyid(r.get(study_col, ""))
        bx = r.get(box_col, "")
        if sid:
            m[sid] = bx
    return m

def row_to_output(row: pd.Series, box_map: dict) -> dict:
    """
    Create one output record with requested fields.
    BoxNumber: prefer mapping from boxNumber tab; fallback to row's BoxNumber.
    """
    sid = norm_studyid(row.get("StudyID", ""))
    out = {"BoxNumber": box_map.get(sid, row.get("BoxNumber", ""))}

    # Remaining requested fields
    for f in FIELDS_TO_SHOW:
        if f == "BoxNumber":
            continue
        out[f] = row.get(f, "")

    return out

def search_studyid(studyid: str, spreadsheet_id: str, api_key: str) -> pd.DataFrame:
    """
    Search StudyID across TABS_TO_SEARCH and return results DF.
    """
    sid_norm = norm_studyid(studyid)

    # Load box mapping
    box_df = read_tab(spreadsheet_id, BOX_TAB, api_key)
    box_map = build_box_map(box_df)

    hits = []
    for tab in TABS_TO_SEARCH:
        df = read_tab(spreadsheet_id, tab, api_key)
        if df.empty or "StudyID" not in df.columns:
            continue

        df["_sid"] = df["StudyID"].apply(norm_studyid)
        sub = df[df["_sid"] == sid_norm]
        if sub.empty:
            continue

        for _, r in sub.iterrows():
            rec = row_to_output(r, box_map)
            rec["SourceTab"] = tab
            hits.append(rec)

    if not hits:
        return pd.DataFrame()

    out_df = pd.DataFrame(hits)
    ordered = ["SourceTab"] + FIELDS_TO_SHOW
    for c in ordered:
        if c not in out_df.columns:
            out_df[c] = ""
    return out_df[ordered]

# -------------------- Secrets --------------------
api_key = st.secrets.get("GOOGLE_API_KEY", "")
spreadsheet_id = st.secrets.get("SPREADSHEET_ID", "")

if not api_key or not spreadsheet_id:
    st.error("Missing secrets. Please set GOOGLE_API_KEY and SPREADSHEET_ID in Streamlit Secrets.")
    st.stop()

# -------------------- UI --------------------
with st.sidebar:
    st.subheader("Tabs")
    st.write("Search:", TABS_TO_SEARCH)
    st.write("Box mapping:", BOX_TAB)

studyid = st.text_input("Enter StudyID", placeholder="e.g., S1234").strip()
do_search = st.button("Search", type="primary")

if do_search:
    if not studyid:
        st.warning("Please enter a StudyID.")
        st.stop()

    try:
        with st.spinner("Searching..."):
            results = search_studyid(studyid, spreadsheet_id, api_key)

        if results.empty:
            st.info(f"No results found for StudyID = {studyid}")
        else:
            st.success(f"Found {len(results)} record(s).")
            st.dataframe(results, use_container_width=True, hide_index=True)

    except HttpError as e:
        st.error("Google Sheets API error")
        st.code(str(e), language="text")
        st.info(
            "If you see 403 PERMISSION_DENIED, your sheet is likely private and not accessible via API key. "
            "Use a Service Account for private data."
        )
    except Exception as e:
        st.error("Unexpected error")
        st.code(str(e), language="text")
