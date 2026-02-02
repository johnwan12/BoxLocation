# BoxLocation.py
# Streamlit app for:
# 1) View all data from a selected tab: Cocaine / Cannabis / HIV-neg-nondrug / HIV+nondrug
# 2) StudyID -> BoxNumber lookup from boxNumber tab (if missing => "Not Found")
# 3) LN3 (liquid nitrogen tank) inventory:
#    - Add new record (RackNumber, BoxNumber=HIV status + Drug Group, TubeNumber, TubeAmount, Memo)
#    - Search LN3 by BoxNumber (shows TubeAmount and all columns)
#
# Secrets required (Streamlit Cloud -> App -> Settings -> Secrets):
# [google_service_account]  (full service account json fields)
# [connections.gsheets]
# spreadsheet = "YOUR_SPREADSHEET_ID"
#
# The Google Sheet must be shared with the service account email (Editor needed for LN3 writes).

import re
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# -------------------- Page --------------------
st.set_page_config(page_title="Box Location + LN3", layout="wide")
st.title("📦 Box Location + 🧊 LN3 Liquid Nitrogen Tank")

# -------------------- Tabs --------------------
DISPLAY_TABS = ["Cocaine", "Cannabis", "HIV-neg-nondrug", "HIV+nondrug"]
TAB_MAP = {
    "Cocaine": "cocaine",
    "Cannabis": "cannabis",
    "HIV-neg-nondrug": "HIV-neg-nondrug",
    "HIV+nondrug": "HIV+nondrug",
}
BOX_TAB = "boxNumber"
LN3_TAB = "LN3"

# -------------------- Spreadsheet ID --------------------
def get_spreadsheet_id() -> str:
    try:
        return st.secrets["connections"]["gsheets"]["spreadsheet"]
    except Exception:
        return st.secrets.get("SPREADSHEET_ID", "")

SPREADSHEET_ID = get_spreadsheet_id()
if not SPREADSHEET_ID:
    st.error(
        'Missing spreadsheet id in Secrets.\n\nAdd:\n'
        '[connections.gsheets]\n'
        'spreadsheet = "YOUR_SPREADSHEET_ID"\n'
    )
    st.stop()

# -------------------- Helpers --------------------
def norm_header(x: str) -> str:
    x = "" if x is None else str(x)
    return re.sub(r"\s+", " ", x.strip())

def norm_studyid(x: str) -> str:
    x = "" if x is None else str(x)
    return re.sub(r"\s+", "", x.strip()).upper()

def format_mmddyyyy(x):
    """Display as MM/DD/YYYY; handles strings, datetimes, and Google Sheets serial numbers."""
    if x in ("", None):
        return ""
    try:
        if isinstance(x, (datetime, pd.Timestamp)):
            return x.strftime("%m/%d/%Y")

        # Google Sheets serial date (days since 1899-12-30)
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

def utc_now_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

@st.cache_resource(show_spinner=False)
def sheets_service():
    if "google_service_account" not in st.secrets:
        raise KeyError('Missing [google_service_account] in secrets.toml')

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]  # read + write (needed for LN3 append)
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

def maybe_format_date_col(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "Date collected" in df.columns:
        df = df.copy()
        df["Date collected"] = df["Date collected"].apply(format_mmddyyyy)
    return df

@st.cache_data(ttl=300, show_spinner=False)
def build_box_map() -> dict:
    """
    Build mapping: normalized StudyID -> BoxNumber from boxNumber tab.
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
        bx = r.get(box_col, "")
        if sid:
            m[sid] = bx
    return m

def get_sheet_id_by_title(spreadsheet_metadata: dict, title: str):
    for sh in spreadsheet_metadata.get("sheets", []):
        props = sh.get("properties", {})
        if props.get("title") == title:
            return props.get("sheetId")
    return None

def ensure_header_and_sheet(service, spreadsheet_id: str, tab_name: str, header: list[str]):
    """
    Ensure tab exists and has header row.
    - If tab missing: create it.
    - If A1 row empty: set header in row 1.
    """
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id = get_sheet_id_by_title(meta, tab_name)

    if sheet_id is None:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        ).execute()

    # Check first row
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab_name}'!A1:Z1",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    values = resp.get("values", [])
    first_row = values[0] if values else []

    if (not first_row) or all(str(x).strip() == "" for x in first_row):
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{tab_name}'!A1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()

def append_row(service, spreadsheet_id: str, tab_name: str, row_values: list):
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"'{tab_name}'!A:Z",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row_values]},
    ).execute()

# -------------------- Sidebar: pick a group tab --------------------
with st.sidebar:
    st.subheader("Group Tabs")
    selected_display_tab = st.selectbox("Select a tab", DISPLAY_TABS, index=0)
    st.caption(f"Spreadsheet: {SPREADSHEET_ID[:10]}...")

# -------------------- Section 1: show all data for selected tab --------------------
sheet_tab = TAB_MAP[selected_display_tab]

try:
    with st.spinner(f"Loading tab: {selected_display_tab} ..."):
        df = read_tab(sheet_tab)
    df = maybe_format_date_col(df)

    st.subheader(f"📋 All data in: {selected_display_tab}")
    if df.empty:
        st.warning(f"No data found in tab: {selected_display_tab}")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

    # -------------------- Section 2: StudyID -> BoxNumber lookup --------------------
    st.divider()
    st.subheader("🔎 StudyID → BoxNumber (from boxNumber tab)")

    if df.empty or "StudyID" not in df.columns:
        st.info("No StudyID column found in this tab, so StudyID lookup is unavailable.")
    else:
        # Build StudyID options from current selected tab
        studyids = (
            df["StudyID"]
            .dropna()
            .astype(str)
            .map(lambda x: x.strip())
        )
        studyid_options = sorted([s for s in studyids.unique().tolist() if s])

        selected_studyid = st.selectbox("Select StudyID", ["(select)"] + studyid_options)

        if selected_studyid != "(select)":
            box_map = build_box_map()
            box = box_map.get(norm_studyid(selected_studyid), "")

            if str(box).strip() == "":
                st.markdown("**BoxNumber:**")
                st.error("Not Found")
            else:
                st.markdown("**BoxNumber:**")
                st.success(str(box))

    # -------------------- Section 3: LN3 --------------------
    st.divider()
    st.header("🧊 LN3 — Liquid Nitrogen Tank")

    # Load LN3 table (may be empty if tab missing or no data)
    try:
        ln3_df = read_tab(LN3_TAB)
    except Exception:
        ln3_df = pd.DataFrame()

    # ---- Add New LN3 Record ----
    st.subheader("➕ Add New LN3 Record")

    with st.form("ln3_add_form", clear_on_submit=True):
        rack = st.selectbox("RackNumber", [1, 2, 3, 4, 5, 6], index=0)

        c1, c2 = st.columns(2)
        with c1:
            hiv_status = st.selectbox("HIV Status", ["HIV+", "HIV-"], index=0)
        with c2:
            drug_group = st.selectbox("Drug Group", ["Cocaine", "Cannabis", "Poly"], index=0)

        # BoxNumber is ONLY HIV status + Drug Group
        box_number = f"{hiv_status}-{drug_group}"

        c3, c4 = st.columns([1, 2])
        with c3:
            tube_prefix = st.selectbox("Tube Prefix", ["GICU", "HCCU"], index=0)
        with c4:
            tube_suffix = st.text_input("Tube Suffix (enter)", placeholder="e.g., 00123").strip()

        tube_number = f"{tube_prefix}{tube_suffix}" if tube_suffix else tube_prefix

        tube_amount = st.number_input("TubeAmount", min_value=0, step=1, value=1)
        memo = st.text_area("Memo", placeholder="Optional notes")

        submit_ln3 = st.form_submit_button("Submit to LN3", type="primary")

        if submit_ln3:
            if not tube_suffix:
                st.error("Please enter Tube Suffix.")
                st.stop()

            service = sheets_service()
            header = ["Timestamp", "RackNumber", "BoxNumber", "TubeNumber", "TubeAmount", "Memo"]

            try:
                ensure_header_and_sheet(service, SPREADSHEET_ID, LN3_TAB, header)
                row = [utc_now_str(), str(rack), box_number, tube_number, int(tube_amount), memo]
                append_row(service, SPREADSHEET_ID, LN3_TAB, row)
                st.success(f"Saved to LN3: {box_number} / {tube_number}")

                # refresh LN3
                ln3_df = read_tab(LN3_TAB)

            except HttpError as e:
                st.error("Google Sheets API error while writing to LN3.")
                st.code(str(e), language="text")
            except Exception as e:
                st.error("Unexpected error while writing to LN3.")
                st.code(str(e), language="text")

    # ---- Display LN3 table ----
    st.subheader("📋 LN3 Inventory Table")
    if ln3_df is None or ln3_df.empty:
        st.info("LN3 tab has no data yet.")
    else:
        st.dataframe(ln3_df, use_container_width=True, hide_index=True)

    # ---- Search LN3 by BoxNumber ----
    st.subheader("🔎 Search LN3 by BoxNumber (includes TubeAmount)")
    if ln3_df is None or ln3_df.empty or "BoxNumber" not in ln3_df.columns:
        st.info("No searchable BoxNumber data in LN3 yet.")
    else:
        bn = ln3_df["BoxNumber"].dropna().astype(str).map(lambda x: x.strip())
        bn_options = sorted([x for x in bn.unique().tolist() if x])

        selected_bn = st.selectbox("Choose BoxNumber (LN3)", ["(select)"] + bn_options)
        if selected_bn != "(select)":
            sub = ln3_df[ln3_df["BoxNumber"].astype(str).str.strip() == selected_bn].copy()

            # make TubeAmount numeric if present (for clean display/sorting)
            if "TubeAmount" in sub.columns:
                sub["TubeAmount"] = pd.to_numeric(sub["TubeAmount"], errors="coerce")

            st.write(f"Records for **{selected_bn}**:")
            st.dataframe(sub, use_container_width=True, hide_index=True)

except HttpError as e:
    st.error("Google Sheets API error")
    st.code(str(e), language="text")
except Exception as e:
    st.error("Unexpected error")
    st.code(str(e), language="text")
