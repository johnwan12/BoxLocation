# BoxLocation.py
# Complete Streamlit app including:
# 1) Box Location viewer:
#    - User selects a tab: Cocaine / Cannabis / HIV-neg-nondrug / HIV+nondrug
#    - Display ALL rows/columns for that selected tab
#    - StudyID dropdown from the selected tab
#    - Lookup BoxNumber for that StudyID in the "boxNumber" tab
#      -> If not found, display "Not Found"
#
# 2) LN3 Liquid Nitrogen Tank:
#    - Add new LN3 record:
#        RackNumber: dropdown 1..6
#        BoxNumber: HIV status / Drug Group
#        TubeNumber: TubePrefix + one space + TubeInput
#        TubeAmount: user enters
#        Memo: user enters
#      -> Save (append) to LN3 tab
#    - Search LN3 by BoxNumber:
#        Choose BoxNumber -> show all matching rows including TubeAmount
#
# Streamlit Secrets required:
#   [google_service_account]  (service account json fields)
#   [connections.gsheets]
#   spreadsheet = "YOUR_SPREADSHEET_ID"
#
# The Google Sheet must be shared with the service account email:
#   - Viewer is enough for reading tabs
#   - Editor is required for writing to LN3

import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ============================================================
# Page setup
# ============================================================
st.set_page_config(page_title="Box Location + LN3", layout="wide")
st.title("📦 Box Location + 🧊 LN3 Liquid Nitrogen Tank")

# ============================================================
# Constants
# ============================================================
DISPLAY_TABS = ["Cocaine", "Cannabis", "HIV-neg-nondrug", "HIV+nondrug"]
TAB_MAP = {
    "Cocaine": "cocaine",
    "Cannabis": "cannabis",
    "HIV-neg-nondrug": "HIV-neg-nondrug",
    "HIV+nondrug": "HIV+nondrug",
}
BOX_TAB = "boxNumber"
LN3_TAB = "LN3"

# ============================================================
# Spreadsheet ID (from Streamlit secrets)
# ============================================================
SPREADSHEET_ID = st.secrets["connections"]["gsheets"]["spreadsheet"]

# ============================================================
# Google Sheets service (READ + WRITE) - service account
# ============================================================
@st.cache_resource(show_spinner=False)
def sheets_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(
        dict(st.secrets["google_service_account"]),
        scopes=scopes,
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

# ============================================================
# Helpers
# ============================================================
def safe_strip(x):
    return "" if x is None else str(x).strip()

def read_tab(tab_name: str) -> pd.DataFrame:
    """Read a whole tab into DataFrame; first row is header."""
    svc = sheets_service()
    resp = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{tab_name}'!A1:ZZ",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()

    values = resp.get("values", [])
    if not values:
        return pd.DataFrame()

    header = [safe_strip(h) for h in values[0]]
    rows = values[1:]

    # Normalize row lengths to header length
    n = len(header)
    fixed = []
    for r in rows:
        r = list(r)
        if len(r) < n:
            r += [""] * (n - len(r))
        elif len(r) > n:
            r = r[:n]
        fixed.append(r)

    df = pd.DataFrame(fixed, columns=header)
    return df

def build_box_map() -> dict:
    """
    Build mapping: StudyID -> BoxNumber from boxNumber tab.
    Looks for columns:
      StudyID / Study ID / Study Id / ID
      BoxNumber / Box Number / Box / Box#
    """
    df = read_tab(BOX_TAB)
    if df.empty:
        return {}

    study_candidates = ["StudyID", "Study ID", "Study Id", "ID"]
    box_candidates = ["BoxNumber", "Box Number", "Box", "Box#", "Box #"]

    study_col = next((c for c in study_candidates if c in df.columns), None)
    box_col = next((c for c in box_candidates if c in df.columns), None)
    if study_col is None or box_col is None:
        return {}

    m = {}
    for _, r in df.iterrows():
        sid = safe_strip(r.get(study_col, "")).upper()
        bx = safe_strip(r.get(box_col, ""))
        if sid:
            m[sid] = bx
    return m

def ensure_sheet_exists(service, tab_name: str):
    """Create the sheet tab if missing."""
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    titles = [s["properties"]["title"] for s in meta.get("sheets", [])]

    if tab_name not in titles:
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        ).execute()

def ensure_ln3_header(service):
    """
    Ensure LN3 has header row. If LN3 is empty, write header to row 1.
    Does NOT overwrite an existing non-empty header row.
    """
    ensure_sheet_exists(service, LN3_TAB)

    # Check first row
    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{LN3_TAB}'!A1:Z1",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    first_row = (resp.get("values", [[]]) or [[]])[0]
    if first_row and any(safe_strip(x) for x in first_row):
        return  # header already exists (or at least row 1 isn't blank)

    header = ["RackNumber", "BoxNumber", "TubeNumber", "TubeAmount", "Memo"]
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{LN3_TAB}'!A1",
        valueInputOption="RAW",
        body={"values": [header]},
    ).execute()

def append_ln3_row(service, row):
    """Append a row to LN3."""
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{LN3_TAB}'!A:Z",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()

# ============================================================
# Sidebar
# ============================================================
with st.sidebar:
    st.subheader("Box Location")
    selected_display_tab = st.selectbox("Select a tab", DISPLAY_TABS, index=0)
    st.caption(f"Spreadsheet: {SPREADSHEET_ID[:10]}...")

# ============================================================
# 1) BOX LOCATION SECTION
# ============================================================
st.header("📦 Box Location")

sheet_tab = TAB_MAP[selected_display_tab]

try:
    with st.spinner(f"Loading tab: {selected_display_tab} ..."):
        df = read_tab(sheet_tab)

    if df.empty:
        st.warning(f"No data found in tab: {selected_display_tab}")
    else:
        st.subheader(f"📋 All data in: {selected_display_tab}")
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.subheader("🔎 StudyID → BoxNumber (from boxNumber tab)")
        if "StudyID" not in df.columns:
            st.info("This tab does not have a 'StudyID' column, so lookup is unavailable.")
        else:
            studyids = (
                df["StudyID"]
                .dropna()
                .astype(str)
                .map(lambda x: safe_strip(x))
            )
            options = sorted([s for s in studyids.unique().tolist() if s])

            selected_studyid = st.selectbox("Select StudyID", ["(select)"] + options)
            if selected_studyid != "(select)":
                box_map = build_box_map()
                box = box_map.get(safe_strip(selected_studyid).upper(), "")
                st.markdown("**BoxNumber:**")
                if safe_strip(box) == "":
                    st.error("Not Found")
                else:
                    st.success(box)

except HttpError as e:
    st.error("Google Sheets API error (Box Location)")
    st.code(str(e), language="text")
except Exception as e:
    st.error("Unexpected error (Box Location)")
    st.code(str(e), language="text")

# ============================================================
# 2) LN3 SECTION
# ============================================================
st.divider()
st.header("🧊 LN3 Liquid Nitrogen Tank")

# Load LN3 data
try:
    ln3_df = read_tab(LN3_TAB)
except Exception:
    ln3_df = pd.DataFrame()

# ---------- Add New LN3 Record ----------
st.subheader("➕ Add LN3 Record")

with st.form("add_ln3", clear_on_submit=True):
    rack = st.selectbox("RackNumber", [1, 2, 3, 4, 5, 6], index=0)

    c1, c2 = st.columns(2)
    with c1:
        hiv_status = st.selectbox("HIV Status", ["HIV+", "HIV-"], index=0)
    with c2:
        drug_group = st.selectbox("Drug Group", ["Cocaine", "Cannabis", "Poly"], index=0)

    # BoxNumber format: HIV status / Drug Group
    box_number = f"{hiv_status} / {drug_group}"

    c3, c4 = st.columns(2)
    with c3:
        tube_prefix = st.selectbox("Tube Prefix", ["GICU", "HCCU"], index=0)
    with c4:
        tube_input = st.text_input("Tube Input", placeholder="e.g., 00123").strip()

    # TubeNumber = TubePrefix + one space + TubeInput
    tube_number = f"{tube_prefix} {tube_input}" if tube_input else ""

    tube_amount = st.number_input("TubeAmount", min_value=0, step=1, value=1)
    memo = st.text_area("Memo", placeholder="Optional notes")

    submitted = st.form_submit_button("Save to LN3", type="primary")

    if submitted:
        if not tube_input:
            st.error("Tube Input is required.")
            st.stop()

        try:
            service = sheets_service()
            ensure_ln3_header(service)

            row = [
                int(rack),
                box_number,
                tube_number,
                int(tube_amount),
                memo,
            ]
            append_ln3_row(service, row)
            st.success(f"Saved: {box_number} | {tube_number}")

            # Refresh LN3 data after write
            ln3_df = read_tab(LN3_TAB)

        except HttpError as e:
            st.error("Google Sheets API error while writing to LN3.")
            st.code(str(e), language="text")
        except Exception as e:
            st.error("Failed to save LN3 record")
            st.code(str(e), language="text")

# ---------- Show LN3 Table ----------
st.subheader("📋 LN3 Inventory Table")
if ln3_df.empty:
    st.info("LN3 tab is empty.")
else:
    st.dataframe(ln3_df, use_container_width=True, hide_index=True)

# ---------- Search LN3 by BoxNumber ----------
st.subheader("🔎 Search LN3 by BoxNumber")

if (not ln3_df.empty) and ("BoxNumber" in ln3_df.columns):
    bn = ln3_df["BoxNumber"].dropna().astype(str).map(safe_strip)
    box_options = sorted([x for x in bn.unique().tolist() if x])

    selected_box = st.selectbox("BoxNumber (LN3)", ["(select)"] + box_options)

    if selected_box != "(select)":
        result = ln3_df[ln3_df["BoxNumber"].astype(str).map(safe_strip) == selected_box].copy()

        # Ensure TubeAmount displays as numeric when possible
        if "TubeAmount" in result.columns:
            result["TubeAmount"] = pd.to_numeric(result["TubeAmount"], errors="coerce")

        st.dataframe(result, use_container_width=True, hide_index=True)
else:
    st.info("No BoxNumber data available yet (LN3 empty or missing BoxNumber column).")
