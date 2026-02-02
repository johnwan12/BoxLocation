import re
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

st.set_page_config(page_title="StudyID Lookup", layout="wide")
st.title("🧊 Box Location Viewer (Google Sheets)")

# -------------------- Tabs --------------------
DISPLAY_TABS = ["Cocaine", "Cannabis", "HIV-neg-nondrug", "HIV+nondrug"]
TAB_MAP = {
    "Cocaine": "cocaine",
    "Cannabis": "cannabis",
    "HIV-neg-nondrug": "HIV-neg-nondrug",
    "HIV+nondrug": "HIV+nondrug",
}
BOX_TAB = "boxNumber"

# -------------------- Secrets / Spreadsheet ID --------------------
def get_spreadsheet_id() -> str:
    try:
        return st.secrets["connections"]["gsheets"]["spreadsheet"]
    except Exception:
        return st.secrets.get("SPREADSHEET_ID", "")

SPREADSHEET_ID = get_spreadsheet_id()
if not SPREADSHEET_ID:
    st.error('Missing spreadsheet id. Add this to Secrets:\n\n[connections.gsheets]\nspreadsheet = "YOUR_SHEET_ID"')
    st.stop()

# -------------------- Helpers --------------------
def norm_header(x: str) -> str:
    x = "" if x is None else str(x)
    return re.sub(r"\s+", " ", x.strip())

def norm_studyid(x: str) -> str:
    x = "" if x is None else str(x)
    return re.sub(r"\s+", "", x.strip()).upper()

def format_mmddyyyy(x):
    """Display as MM/DD/YYYY. Handles strings, datetimes, and Google Sheets serial numbers."""
    if x in ("", None):
        return ""

    try:
        if isinstance(x, (datetime, pd.Timestamp)):
            return x.strftime("%m/%d/%Y")

        if isinstance(x, (int, float)) and not pd.isna(x):
            base = datetime(1899, 12, 30)
            dt = base + timedelta(days=float(x))
            return dt.strftime("%m/%d/%Y")

        dt = pd.to_datetime(x, errors="coerce")
        if pd.isna(dt):
            return str(x)

        return dt.strftime("%m/%d/%Y")
    except Exception:
        return str(x)

def maybe_format_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    """If the tab has 'Date collected', format it for display."""
    if "Date collected" in df.columns:
        df = df.copy()
        df["Date collected"] = df["Date collected"].apply(format_mmddyyyy)
    return df

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

@st.cache_data(ttl=300, show_spinner=False)
def build_box_map() -> dict:
    """
    Build mapping: normalized StudyID -> BoxNumber from boxNumber tab.
    If headers don't match expected names, returns empty mapping.
    """
    df = read_tab(BOX_TAB)
    if df.empty:
        return {}

    study_candidates = ["StudyID", "Study ID", "Study Id", "ID"]
    box_candidates = ["BoxNumber", "Box Number", "Box#", "Box #", "Box"]

    study_col = next((c for c in study_candidates if c in df.columns), None)
    box_col = next((c for c in box_candidates if c in df.columns), None)

    if study_col is None or box_col is None:
        return {}

    m = {}
    for _, r in df.iterrows():
        sid = norm_studyid(r.get(study_col, ""))
        if sid:
            m[sid] = r.get(box_col, "")
    return m

# -------------------- UI --------------------
with st.sidebar:
    st.subheader("Select Group Tab")
    selected_display_tab = st.selectbox("Tab", DISPLAY_TABS, index=0)
    st.caption(f"Spreadsheet: {SPREADSHEET_ID[:10]}...")

sheet_tab = TAB_MAP[selected_display_tab]

try:
    with st.spinner(f"Loading tab: {selected_display_tab} ..."):
        df = read_tab(sheet_tab)

    if df.empty:
        st.warning(f"No data found in tab: {selected_display_tab}")
        st.stop()

    df = maybe_format_date_columns(df)

    st.subheader(f"All data in: {selected_display_tab}")
    st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Lookup BoxNumber by StudyID")

    if "StudyID" not in df.columns:
        st.warning("This tab does not have a 'StudyID' column, so StudyID lookup is not available.")
        st.stop()

    # Build dropdown options for StudyID (unique, non-empty)
    studyids = (
        df["StudyID"]
        .dropna()
        .astype(str)
        .map(lambda x: x.strip())
    )
    studyids = [s for s in studyids.unique().tolist() if s != ""]

    selected_studyid = st.selectbox("StudyID", ["(select)"] + sorted(studyids))
    if selected_studyid != "(select)":
        box_map = build_box_map()
        box = box_map.get(norm_studyid(selected_studyid), "")

        st.markdown("**BoxNumber**")
        if str(box).strip() == "":
            st.error("Not Found")
        else:
            st.success(str(box))

except HttpError as e:
    st.error("Google Sheets API error")
    st.code(str(e), language="text")
except Exception as e:
    st.error("Unexpected error")
    st.code(str(e), language="text")
