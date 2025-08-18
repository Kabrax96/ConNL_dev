import re
import pandas as pd
from typing import Optional, Tuple, List

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


def _first_match_row(df: pd.DataFrame, patterns: List[re.Pattern], max_scan_cols: int = 8, start_row: int = 0) -> Optional[int]:
    """
    Devuelve el primer índice de fila donde alguno de los patrones hace match
    en cualquiera de las primeras 'max_scan_cols' columnas (desde start_row).
    """
    lo = start_row
    hi = df.shape[0]
    maxc = min(max_scan_cols, df.shape[1])
    for r in range(lo, hi):
        for c in range(0, maxc):
            val = str(df.iat[r, c]).strip()
            for rx in patterns:
                if rx.search(val):
                    return r
    return None

def _find_section_ii_bounds(df: pd.DataFrame) -> Tuple[Optional[int], Optional[int]]:
    """
    Devuelve (start_ii, end_ii_exclusive):
      * start_ii = fila del título "Transferencias Federales Etiquetadas" o, si existe, la que diga "II."
      * end_ii   = fila donde arranca "Ingresos Derivados de Financiamientos" (excluyente)
    Si no se encuentra alguno, devuelve None en su lugar.
    """
    rx_roman_ii = re.compile(r"^\s*II\.\s*$", re.IGNORECASE)
    rx_ii_title = re.compile(r"transferencias\s+federales\s+etiquetadas[:\.]?$", re.IGNORECASE)
    rx_financ = re.compile(r"ingresos\s+derivados\s+de\s+financiamientos", re.IGNORECASE)
    rx_financ_short = re.compile(r"derivados\s+de\s+financiamientos", re.IGNORECASE)

    # Preferimos el título explícito; si no, usamos "II."
    start_ii = _first_match_row(df, [rx_ii_title], max_scan_cols=8)
    if start_ii is None:
        start_ii = _first_match_row(df, [rx_roman_ii], max_scan_cols=6)

    # Fin = primera ocurrencia de "Ingresos Derivados de Financiamientos" después de start_ii
    end_ii = None
    if start_ii is not None:
        end_ii = _first_match_row(df, [rx_financ, rx_financ_short], max_scan_cols=8, start_row=start_ii + 1)

    return start_ii, end_ii
