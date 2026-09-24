import csv
import os
import shutil
import pandas as pd

SRC = "simplyhired_final_cleaned.csv"
BAK = SRC + ".bak"
FIXED = "simplyhired_final_cleaned.fixed.csv"

# Ensure backup exists
if not os.path.exists(BAK):
    shutil.copy2(SRC, BAK)
    print(f"Backup created: {BAK}")
else:
    print(f"Backup exists: {BAK}")

# Read header to determine expected columns
with open(SRC, 'r', encoding='utf-8', errors='replace') as f:
    header_line = f.readline()
    header = next(csv.reader([header_line]))
    expected = len(header)
    print('Expected columns:', expected, header)

records = [header]
buf = ''

with open(SRC, 'r', encoding='utf-8', errors='replace') as f:
    # skip header already read
    _ = f.readline()
    for raw in f:
        buf += raw
        try:
            row = next(csv.reader([buf]))
        except Exception:
            # need more lines
            continue
        if len(row) == expected:
            records.append(row)
            buf = ''
        elif len(row) < expected:
            # incomplete row, keep buffering
            continue
        else:
            # too many fields -> merge middle into description
            tail_count = expected - 4  # title,url,company,description + tail_count fields
            if tail_count < 0:
                records.append(row)
                buf = ''
                continue
            if len(row) >= 4 + tail_count:
                title = row[0]
                url = row[1] if len(row) > 1 else ''
                company = row[2] if len(row) > 2 else ''
                description = ','.join(row[3:len(row)-tail_count])
                tail = row[len(row)-tail_count:]
                newrow = [title, url, company, description] + tail
                # pad
                while len(newrow) < expected:
                    newrow.append('')
                records.append(newrow)
                buf = ''
            else:
                records.append(row)
                buf = ''

# leftover buffer
if buf.strip():
    try:
        row = next(csv.reader([buf]))
        if len(row) == expected:
            records.append(row)
        else:
            tail_count = expected - 4
            if len(row) >= 4 + tail_count:
                title = row[0]
                url = row[1] if len(row) > 1 else ''
                company = row[2] if len(row) > 2 else ''
                description = ','.join(row[3:len(row)-tail_count])
                tail = row[len(row)-tail_count:]
                newrow = [title, url, company, description] + tail
                while len(newrow) < expected:
                    newrow.append('')
                records.append(newrow)
    except Exception:
        pass

# write fixed
with open(FIXED, 'w', newline='', encoding='utf-8') as f_out:
    writer = csv.writer(f_out, quoting=csv.QUOTE_MINIMAL)
    for r in records:
        writer.writerow(r)

print(f"Wrote fixed file: {FIXED} (rows: {len(records)-1})")

# validate
try:
    df = pd.read_csv(FIXED)
    print('Pandas read OK - rows:', len(df))
    # replace original with fixed (keep backup)
    shutil.move(FIXED, SRC)
    print(f"Replaced original {SRC} with fixed file. Backup retained at {BAK}.")
except Exception as e:
    print('Validation failed:', e)
    print('Fixed file left at', FIXED)
