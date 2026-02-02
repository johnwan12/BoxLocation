# BoxLocation.py
# Complete Streamlit app (Box Location + LN3 Liquid Nitrogen Tank)
#
# ✅ Box Location:
#   - User selects tab: Cocaine / Cannabis / HIV-neg-nondrug / HIV+nondrug
#   - Display all data in selected tab
#   - Select StudyID -> look up BoxNumber in boxNumber tab
#     - If not found => "Not Found"
#
# ✅ LN3 Liquid Nitrogen Tank (tab "LN3"):
#   - Add record fields:
#       RackNumber: dropdown 1..6
#       BoxNumber: code = (HP/HN)-(COC/CAN/POL)
#       BoxUID: auto = LN3-R{rack:02d}-{HP/HN}-{COC/CAN/POL}-{01..99}
#       TubeNumber: "TubePrefix TubeInput" (one space)
#       TubeAmount: user input
#       Memo: user input
#       QRCodeLink: auto-generated (Google Chart QR URL) and written to sheet
#   - Search by BoxNumber: shows all matching rows (incl TubeAmount, BoxUID, QRCodeLink)
#   - Also shows QR preview + download button on submit
#
# Assumptions:
#   - You already added column "QRCodeLink" in LN3 header row.
#
# Streamlit Secrets required:
#   [google_service_account]  (service account json fields)
#   [connections.gsheets]
#   spreadsheet = "YOUR_SPREADSHEET_ID"
#
# Share the Google Sheet with the service account email (Editor required for LN3 writes).

import re
import urllib.parse
import urllib.request
from io import BytesIO

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

# Codes
HIV_CODE = {"HIV+": "HP", "HIV-": "HN"}
DRUG_CODE = {"Cocaine": "COC", "Cannabis": "CAN", "Poly": "POL"}

BOXUID_RE = re.compile(r"^LN3-R\d{2}-(HP|HN)-(COC|CAN|POL)-\d{2}$")

# 1cm x 1cm approximate pixels at 300 DPI: ~118 px (1 inch=2.54cm, 300dpi => 118px/cm)
QR_PX = 118

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
    """Read whole tab into DataFrame; row 1 is header."""
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

def ensure_ln3_header_exists_and_has_qrcol(service):
    """
    If LN3 header row is blank, write expected header.
    If LN3 exists but missing QRCodeLink or BoxUID columns, warn user (do not auto-shift columns).
    """
    # Fetch row 1
    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{LN3_TAB}'!A1:Z1",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    row1 = (resp.get("values", [[]]) or [[]])[0]
    row1 = [safe_strip(x) for x in row1]

    expected = ["RackNumber", "BoxNumber", "BoxUID", "TubeNumber", "TubeAmount", "Memo", "QRCodeLink"]

    # If blank header, set it
    if (not row1) or all(x == "" for x in row1):
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{LN3_TAB}'!A1",
            valueInputOption="RAW",
            body={"values": [expected]},
        ).execute()
        return

    missing = [c for c in expected if c not in row1]
    if missing:
        st.warning(
            "LN3 header row exists but is missing columns: "
            + ", ".join(missing)
            + ". Please add them to LN3 header row (row 1) to prevent misalignment."
        )

def compute_next_boxuid(ln3_df: pd.DataFrame, rack: int, hp_hn: str, drug_code: str) -> str:
    """
    BoxUID: LN3-R{rack:02d}-{HP/HN}-{COC/CAN/POL}-{NN}
    NN increments within same (rack + HP/HN + drug_code), 01..99
    """
    prefix = f"LN3-R{int(rack):02d}-{hp_hn}-{drug_code}-"
    max_n = 0

    if ln3_df is not None and (not ln3_df.empty) and ("BoxUID" in ln3_df.columns):
        for v in ln3_df["BoxUID"].dropna().astype(str):
            s = v.strip()
            if s.startswith(prefix) and re.search(r"-(\d{2})$", s):
                try:
                    n = int(s.split("-")[-1])
                    max_n = max(max_n, n)
                except ValueError:
                    pass

    nxt = max_n + 1
    if nxt > 99:
        raise ValueError(f"BoxUID sequence exceeded 99 for {prefix}**")
    return f"{prefix}{nxt:02d}"

def qr_link_for_boxuid(box_uid: str, px: int = QR_PX) -> str:
    """
    Use Google Chart API to generate QR image (no hosting required).
    Clicking the URL opens a PNG that can be downloaded.
    """
    # URL-encode payload
    payload = urllib.parse.quote(box_uid, safe="")
    return f"https://chart.googleapis.com/chart?cht=qr&chs={px}x{px}&chld=M|1&chl={payload}"

def fetch_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url) as resp:
        return resp.read()

def append_ln3_row(service, row_values: list):
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{LN3_TAB}'!A:Z",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row_values]},
    ).execute()

# ============================================================
# Sidebar (Box Location tab selector)
# ============================================================
with st.sidebar:
    st.subheader("Box Location")
    selected_display_tab = st.selectbox("Select a tab", DISPLAY_TABS, index=0)
    st.caption(f"Spreadsheet: {SPREADSHEET_ID[:10]}...")

# ============================================================
# 1) BOX LOCATION
# ============================================================
st.header("📦 Box Location")
tab_name = TAB_MAP[selected_display_tab]

try:
    df = read_tab(tab_name)

    if df.empty:
        st.warning(f"No data found in tab: {selected_display_tab}")
    else:
        st.subheader(f"📋 All data in: {selected_display_tab}")
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.subheader("🔎 StudyID → BoxNumber (from boxNumber tab)")
        if "StudyID" not in df.columns:
            st.info("This tab does not have a 'StudyID' column.")
        else:
            studyids = df["StudyID"].dropna().astype(str).map(safe_strip)
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

service = sheets_service()

# Load LN3 data
try:
    ln3_df = read_tab(LN3_TAB)
except Exception:
    ln3_df = pd.DataFrame()

# Ensure header contains QRCodeLink (will warn if missing)
ensure_ln3_header_exists_and_has_qrcol(service)

# ---------- Add New LN3 Record ----------
st.subheader("➕ Add LN3 Record")

with st.form("ln3_add", clear_on_submit=True):
    rack = st.selectbox("RackNumber", [1, 2, 3, 4, 5, 6], index=0)

    c1, c2 = st.columns(2)
    with c1:
        hiv_status = st.selectbox("HIV Status", ["HIV+", "HIV-"], index=0)
    with c2:
        drug_group = st.selectbox("Drug Group", ["Cocaine", "Cannabis", "Poly"], index=0)

    hp_hn = HIV_CODE[hiv_status]
    drug_code = DRUG_CODE[drug_group]

    # BoxNumber should be code like: HP-CAN
    box_number = f"{hp_hn}-{drug_code}"

    c3, c4 = st.columns(2)
    with c3:
        tube_prefix = st.selectbox("Tube Prefix", ["GICU", "HCCU"], index=0)
    with c4:
        tube_input = st.text_input("Tube Input", placeholder="e.g., 01 005").strip()

    # TubeNumber stays unchanged: Tube Prefix + one space + Tube Input
    tube_number = f"{tube_prefix} {tube_input}" if tube_input else ""

    tube_amount = st.number_input("TubeAmount", min_value=0, step=1, value=1)
    memo = st.text_area("Memo (optional)")

    # Preview BoxUID and QR
    preview_uid = ""
    preview_qr = ""
    preview_err = ""
    try:
        preview_uid = compute_next_boxuid(ln3_df, rack, hp_hn, drug_code)
        preview_qr = qr_link_for_boxuid(preview_uid)
    except Exception as e:
        preview_err = str(e)

    st.markdown("**BoxUID (auto):**")
    if preview_err:
        st.error(preview_err)
    else:
        st.info(preview_uid)

    st.markdown("**QRCode (auto, ~1cm x 1cm):**")
    if preview_err:
        st.info("QR preview unavailable until BoxUID can be generated.")
    else:
        st.image(preview_qr, width=QR_PX)

    submitted = st.form_submit_button("Save to LN3", type="primary")

    if submitted:
        if not tube_input:
            st.error("Tube Input is required.")
            st.stop()

        try:
            # Recompute at save time (avoid conflicts if others added rows)
            if ln3_df is None:
                ln3_df = pd.DataFrame()

            box_uid = compute_next_boxuid(ln3_df, rack, hp_hn, drug_code)
            qr_link = qr_link_for_boxuid(box_uid)

            # Append row order MUST match LN3 header order:
            # RackNumber | BoxNumber | BoxUID | TubeNumber | TubeAmount | Memo | QRCodeLink
            row = [
                int(rack),
                box_number,
                box_uid,
                tube_number,
                int(tube_amount),
                memo,
                qr_link,
            ]
            append_ln3_row(service, row)
            st.success(f"Saved ✅ {box_uid}")

            # Refresh LN3 data after write
            ln3_df = read_tab(LN3_TAB)

            # Provide immediate download button (PNG)
            try:
                png_bytes = fetch_bytes(qr_link)
                st.download_button(
                    label="⬇️ Download QR PNG",
                    data=png_bytes,
                    file_name=f"{box_uid}.png",
                    mime="image/png",
                )
                st.caption("You can also click the QRCodeLink in the sheet to open the PNG and download.")
            except Exception as e:
                st.warning(f"Saved, but QR download preview failed: {e}")

        except HttpError as e:
            st.error("Google Sheets API error while writing to LN3.")
            st.code(str(e), language="text")
        except Exception as e:
            st.error("Failed to save LN3 record")
            st.code(str(e), language="text")

# ---------- Show LN3 Table ----------
st.subheader("📋 LN3 Inventory Table")
if ln3_df is None or ln3_df.empty:
    st.info("LN3 is empty.")
else:
    st.dataframe(ln3_df, use_container_width=True, hide_index=True)

# ---------- Search LN3 by BoxNumber ----------
st.subheader("🔎 Search LN3 by BoxNumber (includes TubeAmount, BoxUID, QRCodeLink)")
if ln3_df is not None and (not ln3_df.empty) and ("BoxNumber" in ln3_df.columns):
    opts = sorted([safe_strip(x) for x in ln3_df["BoxNumber"].dropna().unique().tolist() if safe_strip(x)])
    chosen = st.selectbox("BoxNumber (LN3)", ["(select)"] + opts)

    if chosen != "(select)":
        res = ln3_df[ln3_df["BoxNumber"].astype(str).map(safe_strip) == chosen].copy()
        if "TubeAmount" in res.columns:
            res["TubeAmount"] = pd.to_numeric(res["TubeAmount"], errors="coerce")
        st.dataframe(res, use_container_width=True, hide_index=True)

        # Optional: quick QR preview for the first row in result
        if "BoxUID" in res.columns and "QRCodeLink" in res.columns and len(res) > 0:
            first_uid = safe_strip(res.iloc[0].get("BoxUID", ""))
            first_qr = safe_strip(res.iloc[0].get("QRCodeLink", ""))
            if first_uid and first_qr:
                st.markdown("**QR Preview (first matching record):**")
                st.image(first_qr, width=QR_PX)
                try:
                    png_bytes = fetch_bytes(first_qr)
                    st.download_button(
                        label="⬇️ Download this QR PNG",
                        data=png_bytes,
                        file_name=f"{first_uid}.png",
                        mime="image/png",
                    )
                except Exception:
                    pass
else:
    st.info("No BoxNumber data available yet (LN3 empty or missing BoxNumber column).")
