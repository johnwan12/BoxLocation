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
# ✅ Log Usage (subtract from TubeAmount)
#   - Select BoxNumber (dropdown)
#   - Select TubeNumber (dropdown filtered by BoxNumber)
#   - (If duplicates) Select BoxUID
#   - Enter Use amount
#   - On submit: TubeAmount = TubeAmount - Use
#   - Report: shows UPDATED ROW ONLY (post-update)
#   - Download button is OUTSIDE st.form() (required by Streamlit)
#
# IMPORTANT:
#   - LN3 header row (row 1) should include these columns (order recommended):
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

import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# -------------------- Page --------------------
st.set_page_config(page_title="Box Location + LN3", layout="wide")
st.title("📦 Box Location + 🧊 LN3 Liquid Nitrogen Tank")

# -------------------- Session State --------------------
if "usage_report_df" not in st.session_state:
    st.session_state.usage_report_df = None
if "usage_report_name" not in st.session_state:
    st.session_state.usage_report_name = "LN3_usage_updated_row.csv"

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
DRUG_CODE = {
    "Cocaine": "COC",
    "Cannabis": "CAN",
    "Poly": "POL",
    "NON-DRUG": "NON-DRUG",
}

# Optional validation regex (not required for app function)
BOXUID_RE = re.compile(r"^LN3-R\d{2}-(HP|HN)-(COC|CAN|POL|NON-DRUG)-\d{2}$")

# 1 cm ~ 118 px at 300 DPI (approx).
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
    BoxUID: LN3-R{rack:02d}-{HP/HN}-{COC/CAN/POL/NON-DRUG}-{NN}
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
    """QuickChart QR endpoint returns a PNG."""
    text = urllib.parse.quote(box_uid, safe="")
    return f"https://quickchart.io/qr?text={text}&size={px}&ecLevel=Q&margin=1"

def fetch_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url) as resp:
        return resp.read()

def append_ln3_row(service, row_values: list):
    """Append row values in expected header order."""
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{LN3_TAB}'!A:Z",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row_values]},
    ).execute()

def col_to_a1(col_idx_0based: int) -> str:
    """0->A, 25->Z, 26->AA ..."""
    n = col_idx_0based + 1
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def get_ln3_header(service) -> list:
    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{LN3_TAB}'!A1:Z1",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    row1 = (resp.get("values", [[]]) or [[]])[0]
    return [safe_strip(x) for x in row1]

def to_int_amount(x, default=0) -> int:
    try:
        s = safe_strip(x)
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default

def find_ln3_row_index(ln3_df: pd.DataFrame, box_number: str, tube_number: str, box_uid: str = ""):
    """
    Find matching DataFrame index (0-based) for a row to update.
    Matches BoxNumber + TubeNumber; if box_uid provided, also match BoxUID.
    Returns (idx0, current_amount_int) or (None, None).
    """
    if ln3_df is None or ln3_df.empty:
        return None, None

    for col in ["BoxNumber", "TubeNumber", "TubeAmount"]:
        if col not in ln3_df.columns:
            return None, None

    df = ln3_df.copy()
    df["BoxNumber"] = df["BoxNumber"].astype(str).map(safe_strip)
    df["TubeNumber"] = df["TubeNumber"].astype(str).map(safe_strip)

    mask = (df["BoxNumber"] == safe_strip(box_number)) & (df["TubeNumber"] == safe_strip(tube_number))

    if box_uid and "BoxUID" in df.columns:
        df["BoxUID"] = df["BoxUID"].astype(str).map(safe_strip)
        mask = mask & (df["BoxUID"] == safe_strip(box_uid))

    hits = df[mask]
    if hits.empty:
        return None, None

    idx0 = int(hits.index[0])
    cur_amount = to_int_amount(hits.iloc[0].get("TubeAmount", 0), default=0)
    return idx0, cur_amount

def update_ln3_tubeamount_by_index(service, idx0: int, new_amount: int):
    """
    Update TubeAmount cell for a given DataFrame index (0-based).
    Sheet row = idx0 + 2 (header row is 1).
    """
    header = get_ln3_header(service)
    if "TubeAmount" not in header:
        raise ValueError("LN3 sheet header missing 'TubeAmount' column.")

    col_idx = header.index("TubeAmount")
    a1_col = col_to_a1(col_idx)
    sheet_row = idx0 + 2

    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{LN3_TAB}'!{a1_col}{sheet_row}",
        valueInputOption="RAW",
        body={"values": [[int(new_amount)]]},
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

# Ensure LN3 header (won't overwrite existing non-empty header)
ensure_ln3_header(service)

# Load LN3 data (for sequencing + display)
try:
    ln3_df = read_tab(LN3_TAB)
except Exception:
    ln3_df = pd.DataFrame()

# ---------- Add New LN3 Record ----------
st.subheader("➕ Add LN3 Record")

with st.form("ln3_add", clear_on_submit=True):
    rack = st.selectbox("RackNumber", [1, 2, 3, 4, 5, 6], index=0)

    c1, c2 = st.columns(2)
    with c1:
        hiv_status = st.selectbox("HIV Status", ["HIV+", "HIV-"], index=0)
    with c2:
        drug_group = st.selectbox("Drug Group", ["Cocaine", "Cannabis", "Poly", "NON-DRUG"], index=0)

    hp_hn = HIV_CODE[hiv_status]
    drug_code = DRUG_CODE.get(drug_group)
    if not drug_code:
        st.error(f"Unknown Drug Group: {drug_group}. Please update DRUG_CODE.")
        st.stop()

    box_number = f"{hp_hn}-{drug_code}"

    c3, c4 = st.columns(2)
    with c3:
        tube_prefix = st.selectbox("Tube Prefix", ["GICU", "HCCU"], index=0)
    with c4:
        tube_input = st.text_input("Tube Input", placeholder="e.g., 01 005").strip()

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
            box_uid = compute_next_boxuid(ln3_df, rack, hp_hn, drug_code)
            qr_link = qr_link_for_boxuid(box_uid)

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

            # QR PNG download is OK here (inside form)?? -> NO. So store it and show outside.
            # We'll store the last QR png link instead.
            st.session_state["last_qr_link"] = qr_link
            st.session_state["last_qr_uid"] = box_uid

        except HttpError as e:
            st.error("Google Sheets API error while writing to LN3.")
            st.code(str(e), language="text")
        except Exception as e:
            st.error("Failed to save LN3 record")
            st.code(str(e), language="text")

# ---------- Download QR outside the form (Streamlit requirement) ----------
if "last_qr_link" in st.session_state and st.session_state.get("last_qr_link"):
    try:
        png_bytes = fetch_bytes(st.session_state["last_qr_link"])
        st.download_button(
            label="⬇️ Download last saved QR PNG",
            data=png_bytes,
            file_name=f"{st.session_state.get('last_qr_uid','LN3')}.png",
            mime="image/png",
            key="download_last_qr_png",
        )
        st.caption("QRCodeLink is stored in the sheet and is clickable.")
    except Exception as e:
        st.warning(f"Saved, but QR download failed: {e}")

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
    chosen = st.selectbox("BoxNumber (LN3)", ["(select)"] + opts, key="search_boxnumber")

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
else:
    st.info("No BoxNumber data available yet (LN3 empty or missing BoxNumber column).")

# ============================================================
# 3) LOG USAGE (UPDATED ROW ONLY REPORT + DOWNLOAD OUTSIDE FORM)
# ============================================================
st.subheader("📉 Log Usage (subtract from TubeAmount)")

if ln3_df is None or ln3_df.empty:
    st.info("LN3 is empty — nothing to log.")
else:
    needed = {"BoxNumber", "TubeNumber", "TubeAmount"}
    if not needed.issubset(set(ln3_df.columns)):
        st.warning("LN3 sheet must include columns: BoxNumber, TubeNumber, TubeAmount.")
    else:
        # Choose BoxNumber
        box_opts = sorted(
            [safe_strip(x) for x in ln3_df["BoxNumber"].dropna().astype(str).tolist() if safe_strip(x)]
        )
        chosen_box = st.selectbox("Select BoxNumber", ["(select)"] + sorted(set(box_opts)), key="use_box")

        chosen_tube = "(select)"
        chosen_uid = ""

        if chosen_box != "(select)":
            sub = ln3_df.copy()
            sub["BoxNumber"] = sub["BoxNumber"].astype(str).map(safe_strip)
            sub["TubeNumber"] = sub["TubeNumber"].astype(str).map(safe_strip)
            sub = sub[sub["BoxNumber"] == safe_strip(chosen_box)].copy()

            tube_opts = sorted([safe_strip(x) for x in sub["TubeNumber"].dropna().astype(str).tolist() if safe_strip(x)])
            chosen_tube = st.selectbox("Select TubeNumber", ["(select)"] + sorted(set(tube_opts)), key="use_tube")

            # If duplicates exist, choose BoxUID
            if chosen_tube != "(select)" and "BoxUID" in sub.columns:
                sub2 = sub[sub["TubeNumber"] == safe_strip(chosen_tube)].copy()
                if len(sub2) > 1:
                    sub2["BoxUID"] = sub2["BoxUID"].astype(str).map(safe_strip)
                    uid_opts = sorted([x for x in sub2["BoxUID"].dropna().tolist() if safe_strip(x)])
                    chosen_uid = st.selectbox(
                        "Multiple matches found. Select BoxUID",
                        ["(select)"] + uid_opts,
                        key="use_uid",
                    )
                    if chosen_uid == "(select)":
                        chosen_uid = ""

            # Show matching record(s)
            if chosen_tube != "(select)":
                show = sub[sub["TubeNumber"] == safe_strip(chosen_tube)].copy()
                if chosen_uid and "BoxUID" in show.columns:
                    show["BoxUID"] = show["BoxUID"].astype(str).map(safe_strip)
                    show = show[show["BoxUID"] == safe_strip(chosen_uid)].copy()
                st.markdown("**Current matching record(s):**")
                st.dataframe(show, use_container_width=True, hide_index=True)

        # ---------------- FORM: submit only ----------------
        with st.form("ln3_use_form"):
            use_amt = st.number_input("Use", min_value=0, step=1, value=1)
            submitted_use = st.form_submit_button("Submit Usage", type="primary")

            if submitted_use:
                if chosen_box == "(select)" or chosen_tube == "(select)":
                    st.error("Please select BoxNumber and TubeNumber.")
                    st.stop()
                if use_amt <= 0:
                    st.error("Use must be > 0.")
                    st.stop()

                try:
                    idx0, cur_amount = find_ln3_row_index(
                        ln3_df=ln3_df,
                        box_number=chosen_box,
                        tube_number=chosen_tube,
                        box_uid=chosen_uid,
                    )
                    if idx0 is None:
                        st.error("No matching LN3 row found to update.")
                        st.stop()

                    new_amount = cur_amount - int(use_amt)
                    if new_amount < 0:
                        st.error(f"Not enough stock. Current TubeAmount = {cur_amount}, Use = {int(use_amt)}")
                        st.stop()

                    update_ln3_tubeamount_by_index(service=service, idx0=idx0, new_amount=new_amount)
                    st.success(f"Usage logged ✅ TubeAmount {cur_amount} → {new_amount}")

                    # Reload LN3 and store UPDATED ROW ONLY in session_state for download outside form
                    ln3_df = read_tab(LN3_TAB)

                    rep = ln3_df.copy()
                    rep["BoxNumber"] = rep["BoxNumber"].astype(str).map(safe_strip)
                    rep["TubeNumber"] = rep["TubeNumber"].astype(str).map(safe_strip)
                    mask = (rep["BoxNumber"] == safe_strip(chosen_box)) & (rep["TubeNumber"] == safe_strip(chosen_tube))
                    if chosen_uid and "BoxUID" in rep.columns:
                        rep["BoxUID"] = rep["BoxUID"].astype(str).map(safe_strip)
                        mask = mask & (rep["BoxUID"] == safe_strip(chosen_uid))

                    updated_row_df = ln3_df[mask].copy()

                    st.session_state.usage_report_df = updated_row_df
                    st.session_state.usage_report_name = "LN3_usage_updated_row.csv"

                except HttpError as e:
                    st.error("Google Sheets API error while logging usage.")
                    st.code(str(e), language="text")
                except Exception as e:
                    st.error("Failed to log usage.")
                    st.code(str(e), language="text")

        # ---------------- OUTSIDE FORM: show report + download ----------------
        if st.session_state.usage_report_df is not None:
            st.markdown("### ✅ Usage Report (updated row only)")
            if st.session_state.usage_report_df.empty:
                st.warning("Update succeeded, but could not match the updated row after refresh.")
            else:
                st.dataframe(st.session_state.usage_report_df, use_container_width=True, hide_index=True)

                csv_bytes = st.session_state.usage_report_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "⬇️ Download updated row CSV",
                    data=csv_bytes,
                    file_name=st.session_state.usage_report_name,
                    mime="text/csv",
                    key="download_usage_updated_row",
                )
