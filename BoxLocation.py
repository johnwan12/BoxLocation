# BoxLocation.py
# Complete Streamlit app (Box Location + LN3 Liquid Nitrogen Tank)
#
# ✅ Box Location:
#   - User selects a tab: Cocaine / Cannabis / HIV-neg-nondrug / HIV+nondrug
#   - Display all data in selected tab
#   - Select StudyID -> look up BoxNumber in boxNumber tab
#     - If not found => "Not Found"
#
# ✅ LN3 Liquid Nitrogen Tank (tab "LN3"):
#   - Add record fields:
#       RackNumber: dropdown 1..6
#       BoxNumber: code = (HP/HN)-(COC/CAN/POL/NON-DRUG)
#       BoxUID: auto = LN3-R{rack:02d}-{HP/HN}-{COC/CAN/POL/NON-DRUG}-{01..99}
#       TubeNumber: "TubePrefix TubeInput" (one space)
#       TubeAmount: user input
#       Memo: user input
#       QRCodeLink: auto-generated (QuickChart PNG URL) and written to sheet
#   - Search by BoxNumber: shows all matching rows (incl TubeAmount, BoxUID, QRCodeLink)
#   - Shows QR preview + download button on submit
#
# IMPORTANT:
#   - LN3 header row (row 1) should include these columns (in this order is best):
#       RackNumber | BoxNumber | BoxUID | TubeNumber | TubeAmount | Memo | QRCodeLink
#   - Sheet must be shared with the service account email (Editor required for LN3 writes).
#
# Streamlit Secrets:
#   [google_service_account]  (service account json fields)
#   [connections.gsheets]
#   spreadsheet = "YOUR_SPREADSHEET_ID"

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

# Codes for BoxNumber + BoxUID
HIV_CODE = {"HIV+": "HP", "HIV-": "HN"}
DRUG_CODE = {"Cocaine": "COC", "Cannabis": "CAN", "Poly": "POL"}

BOXUID_RE = re.compile(r"^LN3-R\d{2}-(HP|HN)-(COC|CAN|POL)-\d{2}$")

# 1 cm ~ 118 px at 300 DPI (approx). You can increase to 236 for higher DPI labels.
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

def ensure_ln3_header(service):
    """
    If LN3 header row is blank, write expected header.
    If LN3 exists but missing required columns, show warning (do not auto-shift existing columns).
    """
    expected = ["RackNumber", "BoxNumber", "BoxUID", "TubeNumber", "TubeAmount", "Memo", "QRCodeLink"]

    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{LN3_TAB}'!A1:Z1",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()

    row1 = (resp.get("values", [[]]) or [[]])[0]
    row1 = [safe_strip(x) for x in row1]

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
            "LN3 header exists but missing columns: "
            + ", ".join(missing)
            + ". Please add them to row 1 (header) to prevent data misalignment."
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
    QuickChart QR endpoint returns a PNG.
    Docs: https://quickchart.io/documentation/qr-codes/
    """
    text = urllib.parse.quote(box_uid, safe="")
    # ecLevel=Q gives stronger error correction; margin=1 keeps some quiet zone
    return f"https://quickchart.io/qr?text={text}&size={px}&ecLevel=Q&margin=1"

def fetch_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url) as resp:
        return resp.read()

def append_ln3_row(service, row_values: list):
    """
    Append row values in this order:
      RackNumber | BoxNumber | BoxUID | TubeNumber | TubeAmount | Memo | QRCodeLink
    """
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

# Load LN3 data (for sequencing)
try:
    ln3_df = read_tab(LN3_TAB)
except Exception:
    ln3_df = pd.DataFrame()

# Ensure LN3 header (won't overwrite existing non-empty header)
ensure_ln3_header(service)

# ---------- Add New LN3 Record ----------
st.subheader("➕ Add LN3 Record")

with st.form("ln3_add", clear_on_submit=True):
    rack = st.selectbox("RackNumber", [1, 2, 3, 4, 5, 6], index=0)

    c1, c2 = st.columns(2)
    with c1:
        hiv_status = st.selectbox("HIV Status", ["HIV+", "HIV-"], index=0)
    with c2:
        drug_group = st.selectbox("Drug Group", ["Cocaine", "Cannabis", "Poly","NON-DRUG"], index=0)

    hp_hn = HIV_CODE[hiv_status]
    drug_code = DRUG_CODE[drug_group]

    # BoxNumber code like: HP-CAN
    box_number = f"{hp_hn}-{drug_code}"

    c3, c4 = st.columns(2)
    with c3:
        tube_prefix = st.selectbox("Tube Prefix", ["GICU", "HCCU"], index=0)
    with c4:
        tube_input = st.text_input("Tube Input", placeholder="e.g., 01 005").strip()

    # TubeNumber: prefix + one space + input
    tube_number = f"{tube_prefix} {tube_input}" if tube_input else ""

    tube_amount = st.number_input("TubeAmount", min_value=0, step=1, value=1)
    memo = st.text_area("Memo (optional)")

    # Preview BoxUID and QRCodeLink
    preview_uid, preview_qr, preview_err = "", "", ""
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

    st.markdown("**QR Preview (~1cm x 1cm):**")
    if preview_qr:
        st.image(preview_qr, width=QR_PX)
    else:
        st.info("QR preview unavailable until BoxUID can be generated.")

    submitted = st.form_submit_button("Save to LN3", type="primary")

    if submitted:
        if not tube_input:
            st.error("Tube Input is required.")
            st.stop()

        try:
            # Recompute at save-time
            box_uid = compute_next_boxuid(ln3_df, rack, hp_hn, drug_code)
            qr_link = qr_link_for_boxuid(box_uid)

            # Append row order MUST match header order:
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

            # Refresh LN3 data
            ln3_df = read_tab(LN3_TAB)

            # Download QR PNG (from QRCodeLink)
            try:
                png_bytes = fetch_bytes(qr_link)
                st.download_button(
                    label="⬇️ Download QR PNG",
                    data=png_bytes,
                    file_name=f"{box_uid}.png",
                    mime="image/png",
                )
                st.caption("QRCodeLink is also stored in the sheet and is clickable.")
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

        # QR preview for first row in results (if available)
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
