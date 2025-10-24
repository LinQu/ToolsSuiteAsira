import csv
import io
from datetime import datetime

def format_value(val):
    val = val.strip()
    if val == "":
        return "''"

    # Coba parsing sebagai tanggal
    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(val, fmt)
            return f"'{dt.strftime('%Y-%m-%d')}'"
        except ValueError:
            pass

    # Escape tanda kutip tunggal
    return "'" + val.replace("'", "''") + "'"


def convert_csv_to_sql(file, table_name):
    """
    Mengonversi file CSV menjadi SQL INSERT IGNORE statements dan mengembalikannya dalam bentuk stream (BytesIO)
    """
    text = io.StringIO(file.getvalue().decode("utf-8"))
    reader = csv.reader(text)
    headers = next(reader)

    output = io.StringIO()
    for row in reader:
        values = [format_value(v) for v in row]
        sql = f"INSERT IGNORE INTO {table_name} ({', '.join(headers)}) VALUES ({', '.join(values)});\n"
        output.write(sql)

    output.seek(0)
    return output.getvalue()
