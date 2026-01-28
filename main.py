# ll = 'C:/Users/Dotpe/D/Farming-397e0ad79319.json'
# GOOGLE_SHEET_ID = "1eipfNkKILw8GoLU__aOATd0WUhpLpA9C4shKq4FTLsw"

from fastapi import FastAPI, Header, HTTPException, status, Body
from pydantic import BaseModel, Field, EmailStr, validator
import re, os, gspread
from datetime import datetime
import json
from gspread.exceptions import SpreadsheetNotFound, WorksheetNotFound
from typing import List

app = FastAPI(title="AadiFinance Lead-Ingest API")

# ----- CONFIG ---------------------------------------------------
API_KEY_MAP = {  # partner-specific keys ⇒ their partner_id
    "YBJD1FRUY45THJ": "CM",
    # "xxxx-DEF456": "partner_b",
}

SHEET_TITLE = "AadiFinance Leads"   # spreadsheet title when we create one
TAB_NAME    = "Leads"               # worksheet/tab name

HEADER_ROW = [                      # keep order for appends
    "timestamp", "phone", "email", "first name", "last name",
    "dob", "pan", "employment_type", "pincode", "income",
    "consent_datetime", "ip_address", "partner_id",
]

# ──────────────────────────────────────────────────────────────
# 2. GOOGLE-SHEETS HELPER
# ──────────────────────────────────────────────────────────────
def get_worksheet() -> gspread.Worksheet:
    """
    • Open the spreadsheet whose ID is in env GOOGLE_SHEET_ID.
    • If ID missing/invalid → create a new spreadsheet and
      print its ID so you can save it back to the env var.
    • Inside that spreadsheet, ensure a worksheet called TAB_NAME
      with HEADER_ROW in row 1 (only once).
    """
    cred_json = os.getenv("GOOGLE_CRED_JSON")
    if not cred_json:
        raise RuntimeError("GOOGLE_CRED_JSON env-var (service-account JSON) is missing.")

    gc = gspread.service_account_from_dict(json.loads(cred_json))
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "")

    # ➊ Spreadsheet
    try:
        sh = gc.open_by_key(sheet_id)
    except (SpreadsheetNotFound, gspread.exceptions.APIError):
        sh = gc.create(SHEET_TITLE)
        sheet_id = sh.id
        print(
            f"⚠️  New Spreadsheet created: {sheet_id}. "
            "Add this value to the GOOGLE_SHEET_ID env-var so future deploys reuse it."
        )

    # ➋ Worksheet
    try:
        ws = sh.worksheet(TAB_NAME)
    except WorksheetNotFound:
        ws = sh.add_worksheet(title=TAB_NAME, rows=1, cols=len(HEADER_ROW) + 5)
        ws.append_row(HEADER_ROW, value_input_option="USER_ENTERED")

    return ws


# initialise once at startup
ws = get_worksheet()

# ─────────────────────────────────────────────────────────���────
# 3. Pydantic schema & validation
# ──────────────────────────────────────────────────────────────
PAN_REGEX = re.compile(r"^[A-Z]{5}[0-9]{4}[A-Z]$")

class Lead(BaseModel):
    phone: str = Field(..., min_length=10, max_length=10)
    email: EmailStr
    first_name: str = Field(..., alias="first name")
    last_name: str = Field(..., alias="last name")
    dob: str
    pan: str
    employment_type: str
    pincode: str = Field(..., min_length=6, max_length=6)
    income: int
    consent_datetime: str | None = None
    ip_address: str | None = Field(None, alias="ip_address")
    partner_id: str

    # field-level validators
    @validator("phone")
    def phone_is_digits(cls, v):
        if not v.isdigit():
            raise ValueError("Phone number must be exactly 10 digits.")
        return v

    @validator("dob")
    def dob_format(cls, v):
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError("dob must be YYYY-MM-DD.")
        return v

    @validator("pan")
    def pan_format(cls, v):
        if not PAN_REGEX.match(v.upper()):
            raise ValueError("Invalid PAN format.")
        return v.upper()

    @validator("employment_type")
    def emp_enum(cls, v):
        if v not in {"salaried", "self-employed"}:
            raise ValueError("employment_type must be salaried or self-employed.")
        return v

    @validator("consent_datetime", always=True)
    def iso_dt(cls, v):
        if v:
            try:
                datetime.fromisoformat(v)
            except ValueError:
                raise ValueError("consent_datetime must be ISO-8601.")
        return v


# ──────────────────────────────────────────────────────────────
# 4. ROUTE
# ──────────────────────────────────────────────────────────────
@app.post("/vendor/submit-lead")
def submit_lead(
    body: Lead,
    x_api_key: str | None = Header(None, convert_underscores=False),
):
    # ➊ API-key auth
    if x_api_key not in API_KEY_MAP:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "success": False,
                "message": "Unauthorised Access! API Key is required!",
            },
        )

    # ➋ partner_id ↔ key match
    if API_KEY_MAP[x_api_key] != body.partner_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "success": False,
                "message": "Data Validation Failed!",
                "error": {"partner_id": "partner_id does not match supplied API key"},
            },
        )

    # ➌ Append row to Google Sheets
    data = body.dict(by_alias=True)
    row = [datetime.utcnow().isoformat()] + [data.get(h, "") for h in HEADER_ROW[1:]]
    ws.append_row(row, value_input_option="USER_ENTERED")

    return {"success": True, "message": "Lead created successfully"}


# New: batch endpoint - accepts an array of leads and appends them in one batch
# Validation: max 100 leads enforced at the FastAPI/Pydantic validation layer using Body(max_items=100)
@app.post("/vendor/submit-leads")
def submit_leads(
    body: List[Lead] = Body(..., max_items=100),
    x_api_key: str | None = Header(None, convert_underscores=False),
):
    # ➊ API-key auth
    if x_api_key not in API_KEY_MAP:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={
                "success": False,
                "message": "Unauthorised Access! API Key is required!",
            },
        )

    expected_partner = API_KEY_MAP[x_api_key]

    # ➋ Ensure all leads match partner_id for this API key
    mismatches = []
    for idx, lead in enumerate(body):
        if lead.partner_id != expected_partner:
            mismatches.append({"index": idx, "partner_id": lead.partner_id})

    if mismatches:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "success": False,
                "message": "Data Validation Failed!",
                "error": {"partner_id_mismatch": mismatches},
            },
        )

    # ➌ Build rows and append in a batch
    rows = []
    for lead in body:
        data = lead.dict(by_alias=True)
        row = [datetime.utcnow().isoformat()] + [data.get(h, "") for h in HEADER_ROW[1:]]
        rows.append(row)

    try:
        # try to use batch append if available
        if hasattr(ws, "append_rows"):
            ws.append_rows(rows, value_input_option="USER_ENTERED")
        else:
            # fallback: append one-by-one
            for r in rows:
                ws.append_row(r, value_input_option="USER_ENTERED")
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "success": False,
                "message": "Failed to write to spreadsheet.",
                "error": str(exc),
            },
        )

    return {"success": True, "created": len(rows), "message": "Leads created successfully"}
