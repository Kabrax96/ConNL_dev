import re
import pandas as pd

import re
import pandas as pd


def _normalize_amount_for_cp(val):
    """
    Normalize accounting-like inputs *before* clean_amount:
      - '(50)'  -> '-50'
      - '50-'   -> '-50'
      - '−50'   (Unicode minus U+2212) -> '-50'
    Returns None for blanks/NaN.
    """
    if pd.isna(val):
        return None

    s = str(val).strip()
    if s == "" or s.lower() == "nan":
        return None

    # Unicode minus → ASCII minus
    s = s.replace("\u2212", "-")

    # Parentheses as negative
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1].strip()

    # Trailing minus as negative
    if s.endswith("-") and not s.startswith("-"):
        s = "-" + s[:-1].strip()

    return s


def parse_fecha_header(text: str) -> tuple:
    """
    Extract full date and year_quarter from a header string.
    Example: "al 31 de marzo de 2023" -> ("2023-03-31", "2023_Q1")
    """
    match = re.search(r"(\d{1,2}) de (\w+)(?: de)? (\d{4})", text.lower())
    month_map = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
        "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
        "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
    }
    if match:
        day = int(match.group(1))
        month = month_map.get(match.group(2), 0)
        year = int(match.group(3))
        full_date = f"{year}-{month:02d}-{day:02d}"
        quarter = (month - 1) // 3 + 1 if month else 0
        year_quarter = f"{year}_Q{quarter}" if quarter else "unknown"
        return full_date, year_quarter
    return None, "unknown"


def extraer_codigo_y_sublabel(texto: str):
    """
    Extract ("A1", "Concepto") from strings like "A1. Concepto".
    Returns (code, sublabel). If no match, returns (None, original_text).
    """
    match = re.match(r"^(A[123]|B[12]|C[12]|E[12]|F[12]|G[12])\.\s*(.*)", texto)
    return (match.group(1), match.group(2)) if match else (None, texto)


def clean_amount(val):
    """
    Clean monetary values like "1,000", "$ 2,000", " 3 000 " (NBSP), etc., to float.
    Returns None if it can't parse.
    NOTE: Accounting negatives like "(50)" should be normalized by
          _normalize_amount_for_cp BEFORE calling this function.
    """
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        s = str(val).strip()
        if s == "" or s.lower() == "nan":
            return None

        # Remove common currency/spacing artifacts
        s = (
            s.replace("\u00A0", "")  # NBSP
             .replace("\u202F", "")  # thin NBSP
             .replace(" ", "")
             .replace("$", "")
             .replace(",", "")
        )
        return float(s)
    except Exception:
        return None
