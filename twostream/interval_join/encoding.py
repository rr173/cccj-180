"""Order-preserving text encoding for arbitrary-precision integers.

SQLite indexes compare TEXT bytewise (BINARY collation).  To run range
scans over times/watermarks that may exceed 64 bits we encode every
integer as a fixed-format string whose bytewise order equals numeric
order:

  v >= 0 : "P" + zero-padded digit count (10 chars) + digits
  v <  0 : "N" + zero-padded (9999999999 - digit count) + per-digit 9's
           complement of |v|

Examples:
  -12 -> "N9999999997" + "87"      -5 -> "N9999999998" + "4"
    0 -> "P0000000001" + "0"       123 -> "P0000000003" + "123"

"N..." sorts before "P...".  Among negatives, more digits means a
smaller number, so the complemented count/digits restore numeric order.
Supports integers with up to 9,999,999,999 digits.
"""


def enc_int(v: int) -> str:
    if not isinstance(v, int) or isinstance(v, bool):
        raise TypeError(f"enc_int expects int, got {type(v).__name__}")
    if v >= 0:
        s = str(v)
        return "P" + "%010d" % len(s) + s
    s = str(-v)
    comp_count = 9999999999 - len(s)
    comp_digits = "".join(str(9 - ord(d) + 48) for d in s)
    return "N" + "%010d" % comp_count + comp_digits
