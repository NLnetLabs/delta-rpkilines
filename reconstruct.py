import argparse
import base64
import dateutil.parser
import io
import os
import sqlite3
import tarfile


# This script currently does not work, as it does not implement the blob storage back-end
# Please use the version before that if you want to use this script
# TODO: Update script to work with blob storage

if __name__ == "__main__":
    database = ""
    timestamp = 0
    output = ""

    parser = argparse.ArgumentParser(
                    prog='delta-rpkilines reconstructor',
                    description='Reconstruct the state of the RPKI at a specific point in time',
                    epilog='Proudly presented by the people at NLnet Labs')
    parser.add_argument("database", help="Path to the SQlite database")
    parser.add_argument("timestamp", help="String indicating a timestamp, will be parsed by dateutil")
    parser.add_argument("output", help="Output name for the .tar.gz")

    args = parser.parse_args()

    database = args.database
    timestamp = dateutil.parser.parse(args.timestamp)
    timestamp = int(timestamp.strftime("%s")) * 1000
    output = args.output

    con = sqlite3.connect(database)
    cur = con.cursor()
    res = cur.execute("SELECT * FROM objects WHERE visibleOn <= ? AND (disappearedOn >= ? OR disappearedOn IS NULL)", (timestamp, timestamp))
    objects = res.fetchall()

    with tarfile.open(f"{output}.tar.gz", "w:gz") as tar:
        for obj in objects:
            content, visible_on, disappeared_on, obj_hash, uri, publication_point = obj
            print(uri)
            path = uri.replace('rsync://', '')

            data = base64.b64decode(content)
            tarinfo = tarfile.TarInfo(path)
            tarinfo.size = len(data)
            tarinfo.mtime = int(visible_on) / 1000
            tar.addfile(tarinfo, io.BytesIO(data))

            # folder, file = path.rsplit("/", 1)
            # os.makedirs(f"{folder}", exist_ok=True)
            # with open(f"{folder}/{file}", "wb") as f:
            #     f.write(base64.b64decode(content))