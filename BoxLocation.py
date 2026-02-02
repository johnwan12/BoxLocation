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

#add new into liquid nitrogen tank 3
from datetime import datetime
import pandas as pd

LN3_TAB = "LN3"  # tab name in Google Sheets

def utc_now_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

def get_sheet_id_by_title(spreadsheet_metadata: dict, title: str):
    for sh in spreadsheet_metadata.get("sheets", []):
        props = sh.get("properties", {})
        if props.get("title") == title:
            return props.get("sheetId")
    return None

def ensure_ln3_header(service, spreadsheet_id: str, tab_name: str, header: list[str]):
    """
    Ensures LN3 has the header row exactly once.
    If tab doesn't exist, it creates it.
    If tab exists but empty, writes header in row 1.
    """
    # 1) Fetch spreadsheet metadata (to check tab existence)
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = get_sheet_id_by_title(meta, tab_name)

    # 2) Create tab if missing
    if sheet_id is None:
        req = {
            "requests": [
                {"addSheet": {"properties": {"title": tab_name}}}
            ]
        }
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body=req
        ).execute()

    # 3) Check if first row has header
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab_name}'!A1:Z1",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    values = resp.get("values", [])

    if not values or not values[0] or all(str(x).strip() == "" for x in values[0]):
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{tab_name}'!A1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()

def append_ln3_row(service, spreadsheet_id: str, tab_name: str, row_values: list):
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab_name}'!A:Z",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row_values]},
    ).execute()


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

st.divider()
st.header("🧊 LN3 — Liquid Nitrogen Tank")

# Load LN3 data (if tab doesn't exist yet, this returns empty df)
try:
    ln3_df = read_tab(LN3_TAB)  # your existing read_tab(tab_name) that reads from SPREADSHEET_ID
except Exception:
    ln3_df = pd.DataFrame()

# ---------- Add New Data UI ----------
st.subheader("➕ Add New LN3 Record")

with st.form("ln3_add_form", clear_on_submit=True):
    rack = st.selectbox("RackNumber", [1, 2, 3, 4, 5, 6], index=0)

    colA, colB, colC = st.columns([1, 1, 1])
    with colA:
        hiv_status = st.selectbox("HIV Status", ["HIV+", "HIV-"], index=0)
    with colB:
        drug_group = st.selectbox("Drug Group", ["Cocaine", "Cannabis", "Poly"], index=0)
    with colC:
        box_suffix = st.text_input("Box # (your label/number)", placeholder="e.g., 12 or A12").strip()

    # Standardized BoxNumber string stored in LN3
    # Example: HIV+-Cocaine-12
    box_number = f"{hiv_status}-{drug_group}-{box_suffix}" if box_suffix else f"{hiv_status}-{drug_group}"

    colD, colE = st.columns([1, 2])
    with colD:
        tube_prefix = st.selectbox("Tube Prefix", ["GICU", "HCCU"], index=0)
    with colE:
        tube_suffix = st.text_input("Tube Suffix (enter)", placeholder="e.g., 00123").strip()

    tube_number = f"{tube_prefix}{tube_suffix}" if tube_suffix else tube_prefix

    tube_amount = st.number_input("TubeAmount", min_value=0, step=1, value=1)
    memo = st.text_area("Memo", placeholder="Optional notes")

    submitted = st.form_submit_button("Submit to LN3", type="primary")

    if submitted:
        if not box_suffix:
            st.error("Please enter Box # (your label/number).")
            st.stop()
        if not tube_suffix:
            st.error("Please enter Tube Suffix.")
            st.stop()

        # Ensure header exists, then append
        service = sheets_service()
        header = ["Timestamp", "RackNumber", "BoxNumber", "TubeNumber", "TubeAmount", "Memo"]

        try:
            ensure_ln3_header(service, SPREADSHEET_ID, LN3_TAB, header)

            row = [
                utc_now_str(),
                str(rack),
                box_number,
                tube_number,
                int(tube_amount),
                memo,
            ]
            append_ln3_row(service, SPREADSHEET_ID, LN3_TAB, row)
            st.success(f"Saved to LN3: {box_number} / {tube_number}")

            # Refresh view
            ln3_df = read_tab(LN3_TAB)

        except HttpError as e:
            st.error("Google Sheets API error while writing to LN3.")
            st.code(str(e), language="text")
        except Exception as e:
            st.error("Unexpected error while writing to LN3.")
            st.code(str(e), language="text")

# Show current LN3 table
st.subheader("📋 LN3 Inventory Table")
if ln3_df is None or ln3_df.empty:
    st.info("LN3 tab has no data yet.")
else:
    st.dataframe(ln3_df, use_container_width=True, hide_index=True)

# ---------- Search block ----------
st.subheader("🔎 Search LN3 by BoxNumber (shows TubeAmount)")

if ln3_df is None or ln3_df.empty or "BoxNumber" not in ln3_df.columns:
    st.info("No searchable BoxNumber data in LN3 yet.")
else:
    # Clean list of BoxNumber options
    bn = (
        ln3_df["BoxNumber"]
        .dropna()
        .astype(str)
        .map(lambda x: x.strip())
    )
    bn_options = sorted([x for x in bn.unique().tolist() if x])

    selected_bn = st.selectbox("Choose BoxNumber", ["(select)"] + bn_options)

    if selected_bn != "(select)":
        # Filter and display all columns (including TubeAmount)
        sub = ln3_df[ln3_df["BoxNumber"].astype(str).str.strip() == selected_bn].copy()

        # Optional: make TubeAmount numeric for consistent display/sorting
        if "TubeAmount" in sub.columns:
            sub["TubeAmount"] = pd.to_numeric(sub["TubeAmount"], errors="coerce")

        st.write(f"Records for **{selected_bn}**:")
        st.dataframe(sub, use_container_width=True, hide_index=True)

