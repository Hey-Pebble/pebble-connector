"""Validates that SQL queries are read-only SELECT statements."""

import re

# Dangerous SQL keywords that indicate write operations
WRITE_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE",
    "GRANT", "REVOKE", "COPY", "EXECUTE", "CALL",
]

def validate_query(sql: str) -> tuple[bool, str]:
    """Validate that a SQL query is a read-only SELECT statement.

    Returns (is_valid, error_message).
    """
    # Strip whitespace and comments
    cleaned = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
    cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
    cleaned = cleaned.strip()

    if not cleaned:
        return False, "Empty query"

    # Must start with SELECT or WITH (for CTEs)
    upper = cleaned.upper()
    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        return False, "Only SELECT queries are allowed"

    # Check for dangerous keywords (not inside quotes)
    for keyword in WRITE_KEYWORDS:
        # Simple check - look for keyword as a whole word
        pattern = rf'\b{keyword}\b'
        if re.search(pattern, upper):
            return False, f"Query contains forbidden keyword: {keyword}"

    return True, ""
