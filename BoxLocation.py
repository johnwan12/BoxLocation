# BoxLocation.py
# Box Location + LN3 (Liquid Nitrogen Tank) with auto BoxUID column
#
# LN3 columns:
#   RackNumber | BoxNumber | BoxUID | TubeNumber | TubeAmount | Memo
#
# BoxNumber format: "HIV status / Drug Group"
# TubeNumber format: "Tube Prefix + one space + Tube Input"
# BoxUID auto: LN3-R{rack:02d}-{HIVCODE}-{DRUGCODE}-{NN}  (NN=01..99)
# Example: LN3-R02-HP-COC-01  => Tank LN3; Rack 02; HIV+; Cocaine; serial 01
#
# Streamlit Secrets:
# [google_service_account]
# ...
# [connections.gsheets]
# spreadsheet="YOUR_SPREADSHEET_ID"
#
# Share the Google Sheet with service account email (Editor needed for LN3 writes).

import re
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# -------------------- Page --------------------
st.set_page_config(page_title="Box Location + LN3", layout="wide")
st.title("📦 Box Location + 🧊 LN3 Liquid Nitrogen Tank")

# -------------------- Constants --------------------
DISPLAY_TABS = ["Cocaine", "Cannabis", "HIV-neg-nondrug", "HIV+nondrug"]
TAB_MAP = {
    "Cocaine": "cocaine",
    "Cannabis": "cannabis",
    "HIV-neg-nondrug": "HIV-neg-nondrug",
    "HIV+nondrug": "HIV+nondrug",
}
BOX_TAB = "boxNumber"
LN3_TAB = "LN3"

# BoxUID code maps (edit if you want different abbreviations)
HIV_CODE = {"HIV+": "HP", "HIV-": "HN"}  # Example uses HP for HIV+
DRUG_CODE = {"Cocaine": "COC", "Cannabis": "CAN", "Poly": "POL"}

# -------------------- Spreadsheet ID --------------------
SPREADSHEET_ID = st.secrets["connections"]["gsheets"]["spreadsheet"]

# -------------------- Google Sheets service (READ + WRITE) --------------------
@st.cache_resource(show_spinner=False)
def sheets_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(
        dict(st.secrets["google_service_account"]),
        scopes=scopes,
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

# -------------------- Helpers --------------------
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

    return pd.DataFrame(fixed, columns=header)

def build_box_map() -> dict:
    """StudyID -> BoxNumber map from boxNumber tab."""
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
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    titles = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if tab_name not in titles:
        service.spreadsheets().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
        ).execute()

def ensure_ln3_header(service):
    """
    Ensure LN3 exists and contains the BoxUID column in header.
    If header row is empty, write full header.
    If header exists but missing BoxUID, we do NOT auto-rewrite (avoid shifting existing data).
    """
    ensure_sheet_exists(service, LN3_TAB)

    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{LN3_TAB}'!A1:Z1",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    row1 = (resp.get("values", [[]]) or [[]])[0]
    row1_clean = [safe_strip(x) for x in row1]

    required = ["RackNumber", "BoxNumber", "BoxUID", "TubeNumber", "TubeAmount", "Memo"]

    # If row1 is blank -> set header
    if (not row1_clean) or all(x == "" for x in row1_clean):
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{LN3_TAB}'!A1",
            valueInputOption="RAW",
            body={"values": [required]},
        ).execute()
        return

    # If LN3 exists already, we assume user already set the header.
    # We only warn if BoxUID is missing.
    if "BoxUID" not in row1_clean:
        st.warning(
            "LN3 header exists but missing column 'BoxUID'. "
            "Please add a new column named 'BoxUID' in LN3 header row (row 1) to store BoxUID."
        )

def append_ln3_row(service, row_values: list):
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{LN3_TAB}'!A:Z",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row_values]},
    ).execute()

def compute_next_boxuid(ln3_df: pd.DataFrame, rack: int, hiv_status: str, drug_group: str) -> str:
    """
    BoxUID format:
      LN3-R{rack:02d}-{HIVCODE}-{DRUGCODE}-{NN}
    NN: 01..99, sequence within same Rack + HIVCODE + DRUGCODE.
    """
    rack2 = f"{int(rack):02d}"
    hiv_code = HIV_CODE.get(hiv_status, "HX")
    drug_code = DRUG_CODE.get(drug_group, "XXX")
    prefix = f"LN3-R{rack2}-{hiv_code}-{drug_code}-"

    max_n = 0
    if ln3_df is not None and not ln3_df.empty and "BoxUID" in ln3_df.columns:
        for v in ln3_df["BoxUID"].dropna().astype(str):
            s = v.strip()
            if s.startswith(prefix):
                m = re.search(r"-(\d{2})$", s)
                if m:
                    try:
                        n = int(m.group(1))
                        max_n = max(max_n, n)
                    except ValueError:
                        pass

    next_n = max_n + 1
    if next_n > 99:
        raise ValueError(f"BoxUID sequence exceeded 99 for {prefix}**")
    return f"{prefix}{next_n:02d}"

# ============================================================
# Sidebar
# ============================================================
with st.sidebar:
    st.subheader("Box Location")
    selected_display_tab = st.selectbox("Select a tab", DISPLAY_TABS, index=0)
    st.caption(f"Spreadsheet: {SPREADSHEET_ID[:10]}...")

# ============================================================
# 1) BOX LOCATION
# ============================================================
st.header("📦 Box Location")

sheet_tab = TAB_MAP[selected_display_tab]

try:
    df = read_tab(sheet_tab)
    if df.empty:
        st.warning(f"No data found in tab: {selected_display_tab}")
    else:
        st.subheader(f"📋 All data in: {selected_display_tab}")
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.subheader("🔎 StudyID → BoxNumber (from boxNumber tab)")
        if "StudyID" not in df.columns:
            st.info("This tab does not have a 'StudyID' column.")
        else:
            studyids = df["StudyID"].dropna().astype(str).map(lambda x: safe_strip(x))
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
# 2) LN3 LIQUID NITROGEN TANK
# ============================================================
st.divider()
st.header("🧊 LN3 Liquid Nitrogen Tank")

# Load LN3 data (for viewing + BoxUID sequence)
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

    # TubeNumber stays unchanged: Tube Prefix + Tube Input (one space)
    tube_number = f"{tube_prefix} {tube_input}" if tube_input else ""

    tube_amount = st.number_input("TubeAmount", min_value=0, step=1, value=1)
    memo = st.text_area("Memo", placeholder="Optional notes")

    # Preview BoxUID
    preview_boxuid = ""
    preview_err = ""
    try:
        preview_boxuid = compute_next_boxuid(ln3_df, rack, hiv_status, drug_group)
    except Exception as e:
        preview_err = str(e)

    st.markdown("**BoxUID (auto):**")
    if preview_err:
        st.error(preview_err)
    else:
        st.info(preview_boxuid)

    submitted = st.form_submit_button("Save to LN3", type="primary")

    if submitted:
        if not tube_input:
            st.error("Tube Input is required.")
            st.stop()

        try:
            service = sheets_service()
            ensure_ln3_header(service)

            # Recompute at save time
            box_uid = compute_next_boxuid(ln3_df, rack, hiv_status, drug_group)

            # Append row with NEW BoxUID column, TubeNumber unchanged
            row = [
                int(rack),
                box_number,
                box_uid,         # <-- new column
                tube_number,     # unchanged rule
                int(tube_amount),
                memo,
            ]
            append_ln3_row(service, row)
            st.success(f"Saved: {box_uid} | {box_number} | {tube_number}")

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

        # Ensure TubeAmount numeric if possible
        if "TubeAmount" in result.columns:
            result["TubeAmount"] = pd.to_numeric(result["TubeAmount"], errors="coerce")

        st.dataframe(result, use_container_width=True, hide_index=True)
else:
    st.info("No BoxNumber data available yet (LN3 empty or missing BoxNumber column).")
