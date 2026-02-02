import re
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# =========================
# CONFIG
# =========================
DISPLAY_TABS = ["Cocaine", "Cannabis", "HIV-neg-nondrug", "HIV+nondrug"]
TAB_MAP = {
    "Cocaine": "cocaine",
    "Cannabis": "cannabis",
    "HIV-neg-nondrug": "HIV-neg-nondrug",
    "HIV+nondrug": "HIV+nondrug",
}
BOX_TAB = "boxNumber"
LN3_TAB = "LN3"

# BoxNumber / BoxUID codes
HIV_CODE = {"HIV+": "HP", "HIV-": "HN"}
DRUG_CODE = {"Cocaine": "COC", "Cannabis": "CAN", "Poly": "POL"}

BOXUID_RE = re.compile(r"^LN3-R\d{2}-[A-Z]{2}-[A-Z]{3}-\d{2}$")
TUBE_RE = re.compile(r"^(GICU|HCCU)\s+.+$")

st.set_page_config(page_title="Box Location + LN3", layout="wide")
st.title("📦 Box Location + 🧊 LN3 Liquid Nitrogen Tank")

SPREADSHEET_ID = st.secrets["connections"]["gsheets"]["spreadsheet"]

# =========================
# Google Sheets Service
# =========================
@st.cache_resource(show_spinner=False)
def sheets_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(
        dict(st.secrets["google_service_account"]),
        scopes=scopes,
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def safe_strip(x):
    return "" if x is None else str(x).strip()

def read_tab(tab_name: str) -> pd.DataFrame:
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

# =========================
# LN3: schema migration
# =========================
def ensure_ln3_schema(service):
    """
    Ensure LN3 header is exactly:
    RackNumber | BoxNumber | BoxUID | TubeNumber | TubeAmount | Memo
    If LN3 has old header (5 cols), we rewrite header and will fix rows with a repair step.
    """
    # Read first row
    resp = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{LN3_TAB}'!A1:Z1",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    row1 = (resp.get("values", [[]]) or [[]])[0]
    row1 = [safe_strip(x) for x in row1]

    desired = ["RackNumber", "BoxNumber", "BoxUID", "TubeNumber", "TubeAmount", "Memo"]

    # If empty header -> set
    if (not row1) or all(x == "" for x in row1):
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{LN3_TAB}'!A1",
            valueInputOption="RAW",
            body={"values": [desired]},
        ).execute()
        return

    # If already correct (contains BoxUID in 3rd col)
    if row1[:len(desired)] == desired:
        return

    # If old header (5 cols): RackNumber BoxNumber TubeNumber TubeAmount Memo
    if row1[:5] == ["RackNumber", "BoxNumber", "TubeNumber", "TubeAmount", "Memo"]:
        # rewrite header row to desired (6 cols)
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{LN3_TAB}'!A1",
            valueInputOption="RAW",
            body={"values": [desired]},
        ).execute()
        st.warning("LN3 header upgraded to include BoxUID. Now run the repair button once to fix old misaligned rows.")
        return

    st.warning("LN3 header is custom / unexpected. Please set header to: RackNumber, BoxNumber, BoxUID, TubeNumber, TubeAmount, Memo")

def compute_next_boxuid(ln3_df: pd.DataFrame, rack: int, hiv_status: str, drug_group: str) -> str:
    rack2 = f"{int(rack):02d}"
    hiv_code = HIV_CODE[hiv_status]
    drug_code = DRUG_CODE[drug_group]
    prefix = f"LN3-R{rack2}-{hiv_code}-{drug_code}-"

    max_n = 0
    if ln3_df is not None and not ln3_df.empty and "BoxUID" in ln3_df.columns:
        for v in ln3_df["BoxUID"].dropna().astype(str):
            s = v.strip()
            if s.startswith(prefix) and re.search(r"-(\d{2})$", s):
                n = int(s.split("-")[-1])
                max_n = max(max_n, n)

    nxt = max_n + 1
    if nxt > 99:
        raise ValueError(f"BoxUID sequence exceeded 99 for {prefix}**")
    return f"{prefix}{nxt:02d}"

def append_ln3_row(service, row_values: list):
    service.spreadsheets().values().append(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{LN3_TAB}'!A:Z",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row_values]},
    ).execute()

def repair_misaligned_ln3_rows(service):
    """
    Repair rows that look like:
    RackNumber | BoxNumber | TubeNumber(BoxUID) | TubeAmount(TubeNumber) | Memo(TubeAmount)
    After we updated header to 6 cols, those old rows still have values in wrong columns.
    This function:
      - If col C matches BoxUID pattern AND col D matches tube pattern -> shift:
        BoxUID <- col C
        TubeNumber <- col D
        TubeAmount <- col E (if numeric)
        Memo <- "" (or keep old memo if exists in F)
      - BoxNumber normalization:
        If BoxUID exists, infer BoxNumber as {HP/HN}-{CAN/COC/POL} from BoxUID.
    """
    df = read_tab(LN3_TAB)
    if df.empty:
        st.info("LN3 empty, nothing to repair.")
        return

    # Need at least columns A..F
    cols = df.columns.tolist()
    required = ["RackNumber", "BoxNumber", "BoxUID", "TubeNumber", "TubeAmount", "Memo"]
    for c in required:
        if c not in cols:
            st.error("LN3 columns not in expected schema. Set header first.")
            return

    updates = []
    # Google Sheets rows: header is row 1, df row 0 corresponds to sheet row 2
    for i in range(len(df)):
        boxuid_candidate = safe_strip(df.at[i, "BoxUID"])
        tubenum_candidate = safe_strip(df.at[i, "TubeNumber"])
        tubeamount_candidate = safe_strip(df.at[i, "TubeAmount"])

        # Typical misalignment after header upgrade:
        # BoxUID column (C) is empty, but TubeNumber column (D) contains BoxUID
        # However your specific wrong row (from your example) happened BEFORE BoxUID existed,
        # so after header rewrite, the old values are likely in the wrong places.
        #
        # We detect by: TubeNumber looks like BoxUID and TubeAmount looks like tubeNumber.
        if BOXUID_RE.match(tubenum_candidate) and TUBE_RE.match(tubeamount_candidate):
            found_boxuid = tubenum_candidate
            found_tube = tubeamount_candidate

            # amount might be sitting in Memo
            amt_in_memo = safe_strip(df.at[i, "Memo"])
            amt_val = ""
            if amt_in_memo.isdigit():
                amt_val = amt_in_memo

            # infer BoxNumber from BoxUID: LN3-R06-HP-CAN-01 -> HP-CAN
            parts = found_boxuid.split("-")
            inferred_boxnum = ""
            if len(parts) >= 5:
                inferred_boxnum = f"{parts[2]}-{parts[3]}"

            # Sheet row number:
            sheet_row = i + 2  # because header is row 1

            # Build batch update ranges:
            updates.append({
                "range": f"'{LN3_TAB}'!B{sheet_row}",  # BoxNumber
                "values": [[inferred_boxnum or safe_strip(df.at[i, 'BoxNumber'])]],
            })
            updates.append({
                "range": f"'{LN3_TAB}'!C{sheet_row}",  # BoxUID
                "values": [[found_boxuid]],
            })
            updates.append({
                "range": f"'{LN3_TAB}'!D{sheet_row}",  # TubeNumber
                "values": [[found_tube]],
            })
            updates.append({
                "range": f"'{LN3_TAB}'!E{sheet_row}",  # TubeAmount
                "values": [[amt_val]],
            })
            updates.append({
                "range": f"'{LN3_TAB}'!F{sheet_row}",  # Memo
                "values": [[""]],
            })

    if not updates:
        st.info("No misaligned rows detected.")
        return

    service.spreadsheets().values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={
            "valueInputOption": "RAW",
            "data": updates
        }
    ).execute()

    st.success(f"Repaired {len(updates)//5} row(s). Refresh to see corrected LN3 table.")

# =========================
# Sidebar
# =========================
with st.sidebar:
    st.subheader("Box Location")
    selected_display_tab = st.selectbox("Select a tab", DISPLAY_TABS, index=0)
    st.caption(f"Spreadsheet: {SPREADSHEET_ID[:10]}...")

# =========================
# 1) BOX LOCATION
# =========================
st.header("📦 Box Location")
tab_name = TAB_MAP[selected_display_tab]

try:
    df = read_tab(tab_name)
    if df.empty:
        st.warning(f"No data found in tab: {selected_display_tab}")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

        st.subheader("🔎 StudyID → BoxNumber (from boxNumber tab)")
        if "StudyID" in df.columns:
            options = sorted([safe_strip(x) for x in df["StudyID"].dropna().unique().tolist() if safe_strip(x)])
            pick = st.selectbox("StudyID", ["(select)"] + options)
            if pick != "(select)":
                bm = build_box_map()
                box = bm.get(pick.upper(), "")
                st.markdown("**BoxNumber:**")
                if safe_strip(box) == "":
                    st.error("Not Found")
                else:
                    st.success(box)
        else:
            st.info("No StudyID column in this tab.")

except Exception as e:
    st.error("Box Location error")
    st.code(str(e), language="text")

# =========================
# 2) LN3
# =========================
st.divider()
st.header("🧊 LN3 Liquid Nitrogen Tank")

service = sheets_service()
ensure_ln3_schema(service)

# Load LN3
try:
    ln3_df = read_tab(LN3_TAB)
except Exception:
    ln3_df = pd.DataFrame()

# One-click repair
with st.expander("🛠 Repair misaligned LN3 rows (run once if you had wrong column order)"):
    if st.button("Repair LN3 rows now"):
        repair_misaligned_ln3_rows(service)
        ln3_df = read_tab(LN3_TAB)

st.subheader("➕ Add LN3 Record")

with st.form("ln3_add", clear_on_submit=True):
    rack = st.selectbox("RackNumber", [1, 2, 3, 4, 5, 6], index=0)

    c1, c2 = st.columns(2)
    with c1:
        hiv_status = st.selectbox("HIV Status", ["HIV+", "HIV-"], index=0)
    with c2:
        drug_group = st.selectbox("Drug Group", ["Cocaine", "Cannabis", "Poly"], index=0)

    # BoxNumber requested as code: HP-CAN (NOT "HIV+ / Cannabis")
    box_number_code = f"{HIV_CODE[hiv_status]}-{DRUG_CODE[drug_group]}"

    c3, c4 = st.columns(2)
    with c3:
        tube_prefix = st.selectbox("Tube Prefix", ["GICU", "HCCU"], index=0)
    with c4:
        tube_input = st.text_input("Tube Input", placeholder="e.g., 01 005").strip()

    # TubeNumber stays unchanged: prefix + one space + input
    tube_number = f"{tube_prefix} {tube_input}" if tube_input else ""

    tube_amount = st.number_input("TubeAmount", min_value=0, step=1, value=1)
    memo = st.text_area("Memo (optional)")

    # preview BoxUID
    preview = ""
    err = ""
    try:
        preview = compute_next_boxuid(ln3_df, rack, hiv_status, drug_group)
    except Exception as e:
        err = str(e)

    st.markdown("**BoxUID (auto):**")
    if err:
        st.error(err)
    else:
        st.info(preview)

    ok = st.form_submit_button("Save to LN3", type="primary")

    if ok:
        if not tube_input:
            st.error("Tube Input is required.")
            st.stop()

        box_uid = compute_next_boxuid(ln3_df, rack, hiv_status, drug_group)

        # Correct order MUST match header:
        # RackNumber | BoxNumber | BoxUID | TubeNumber | TubeAmount | Memo
        row = [int(rack), box_number_code, box_uid, tube_number, int(tube_amount), memo]
        append_ln3_row(service, row)
        st.success(f"Saved ✅ {box_uid}")
        ln3_df = read_tab(LN3_TAB)

st.subheader("📋 LN3 Inventory Table")
if ln3_df.empty:
    st.info("LN3 is empty.")
else:
    st.dataframe(ln3_df, use_container_width=True, hide_index=True)

st.subheader("🔎 Search LN3 by BoxNumber")
if not ln3_df.empty and "BoxNumber" in ln3_df.columns:
    opts = sorted([safe_strip(x) for x in ln3_df["BoxNumber"].dropna().unique().tolist() if safe_strip(x)])
    chosen = st.selectbox("BoxNumber", ["(select)"] + opts)
    if chosen != "(select)":
        res = ln3_df[ln3_df["BoxNumber"].astype(str).map(safe_strip) == chosen].copy()
        if "TubeAmount" in res.columns:
            res["TubeAmount"] = pd.to_numeric(res["TubeAmount"], errors="coerce")
        st.dataframe(res, use_container_width=True, hide_index=True)
else:
    st.info("No BoxNumber data yet.")
