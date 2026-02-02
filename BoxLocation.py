import re
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
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

# -------------------- Secrets / Spreadsheet ID --------------------
def get_spreadsheet_id() -> str:
    # Recommended: use Streamlit connection secret
    try:
        return st.secrets["connections"]["gsheets"]["spreadsheet"]
    except Exception:
        # Optional fallback if you choose to add it:
        return st.secrets.get("SPREADSHEET_ID", "")

SPREADSHEET_ID = get_spreadsheet_id()
if not SPREADSHEET_ID:
    st.error('Missing spreadsheet id. Add this to Secrets:\n\n[connections.gsheets]\nspreadsheet = "YOUR_SHEET_ID"')
    st.stop()

# -------------------- Helpers --------------------
def format_mmddyyyy(x):
    """Display as MM/DD/YYYY. Handles strings, datetimes, and Google Sheets serial numbers."""
    if x in ("", None):
        return ""

    try:
        # Already datetime
        if isinstance(x, (datetime, pd.Timestamp)):
            return x.strftime("%m/%d/%Y")

        # Google Sheets serial number (days since 1899-12-30)
        if isinstance(x, (int, float)) and not pd.isna(x):
            base = datetime(1899, 12, 30)
            dt = base + timedelta(days=float(x))
            return dt.strftime("%m/%d/%Y")

        # Parse string
        dt = pd.to_datetime(x, errors="coerce")
        if pd.isna(dt):
            return str(x)

        return dt.strftime("%m/%d/%Y")

    except Exception:
        return str(x)

def norm_header(x: str) -> str:
    x = "" if x is None else str(x)
    return re.sub(r"\s+", " ", x.strip())

def norm_studyid(x: str) -> str:
    x = "" if x is None else str(x)
    return re.sub(r"\s+", "", x.strip()).upper()

@st.cache_resource(show_spinner=False)
def sheets_service():
    if "google_service_account" not in st.secrets:
        raise KeyError('Missing [google_service_account] in secrets.toml')

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(
        dict(st.secrets["google_service_account"]),
        scopes=scopes
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

@st.cache_data(ttl=300, show_spinner=False)
def read_tab(tab_name: str) -> pd.DataFrame:
    """Read a whole tab into DataFrame; first row is header."""
    svc = sheets_service()
    rng = f"'{tab_name}'!A1:ZZ"

    resp = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=rng,
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()

    values = resp.get("values", [])
    if not values:
        return pd.DataFrame()

    header = [norm_header(h) for h in values[0]]
    rows = values[1:]

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
    """Map normalized StudyID -> BoxNumber from boxNumber tab."""
    if box_df.empty:
        return {}

    study_candidates = ["StudyID", "Study ID", "Study Id", "ID"]
    box_candidates = ["BoxNumber", "Box Number", "Box#", "Box #", "Box"]

    study_col = next((c for c in study_candidates if c in box_df.columns), None)
    box_col = next((c for c in box_candidates if c in box_df.columns), None)

    if study_col is None or box_col is None:
        return {}

    m = {}
    for _, r in box_df.iterrows():
        sid = norm_studyid(r.get(study_col, ""))
        bx = r.get(box_col, "")
        if sid and str(bx).strip() != "":
            m[sid] = bx
    return m

def row_to_output(row: pd.Series, box_map: dict) -> dict:
    """BoxNumber ONLY from boxNumber tab; Date collected formatted."""
    sid = norm_studyid(row.get("StudyID", ""))
    out = {"BoxNumber": box_map.get(sid, "")}  # ONLY from boxNumber tab

    for f in FIELDS_TO_SHOW:
        if f == "BoxNumber":
            continue
        elif f == "Date collected":
            out[f] = format_mmddyyyy(row.get(f, ""))
        else:
            out[f] = row.get(f, "")

    return out

def search_studyid(studyid: str) -> pd.DataFrame:
    """Gatekeeper: StudyID must exist in boxNumber tab; otherwise return empty."""
    sid_norm = norm_studyid(studyid)

    box_df = read_tab(BOX_TAB)
    box_map = build_box_map(box_df)

    # GATEKEEPER
    if sid_norm not in box_map:
        return pd.DataFrame()

    hits = []
    for tab in TABS_TO_SEARCH:
        df = read_tab(tab)
        if df.empty or "StudyID" not in df.columns:
            continue

        df["_sid"] = df["StudyID"].apply(norm_studyid)
        sub = df[df["_sid"] == sid_norm]

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

# -------------------- UI --------------------
with st.sidebar:
    st.subheader("Tabs")
    st.write("Search:", TABS_TO_SEARCH)
    st.write("Box mapping:", BOX_TAB)
    st.caption(f"Spreadsheet ID: {SPREADSHEET_ID[:10]}...")

studyid = st.text_input("Enter StudyID", placeholder="e.g., S1234").strip()
do_search = st.button("Search", type="primary")

if do_search:
    if not studyid:
        st.warning("Please enter a StudyID.")
        st.stop()

    try:
        with st.spinner("Searching..."):
            results = search_studyid(studyid)

        if results.empty:
            st.info(f"StudyID {studyid} is not in 'boxNumber' tab (or no BoxNumber). Nothing to display.")
        else:
            st.success(f"Found {len(results)} record(s).")
            st.dataframe(results, use_container_width=True, hide_index=True)

    except HttpError as e:
        st.error("Google Sheets API error")
        st.code(str(e), language="text")
    except Exception as e:
        st.error("Unexpected error")
        st.code(str(e), language="text")
