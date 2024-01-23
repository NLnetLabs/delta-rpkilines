import argparse
import base64
import dateutil.parser
import io
import os
import sqlite3
import tarfile

import boto3
import botocore.client


if __name__ == "__main__":
    database = ""
    timestamp = 0
    output = ""

    parser = argparse.ArgumentParser(
                    prog='delta-rpkilines reconstructor',
                    description='Reconstruct the state of the RPKI at a specific point in time',
                    epilog='Proudly presented by the people at NLnet Labs')
    # parser.add_argument("database", help="Path to the SQlite database")
    parser.add_argument("config", help="Path to the app.properties config file")
    parser.add_argument("timestamp", help="String indicating a timestamp, will be parsed by dateutil")
    parser.add_argument("output", help="Output name for the .tar.gz")

    args = parser.parse_args()

    database = args.database
    timestamp = dateutil.parser.parse(args.timestamp)
    timestamp = int(timestamp.strftime("%s")) * 1000
    output = args.output
    config = args.config
    config_opts = {}

    con = sqlite3.connect(database)
    cur = con.cursor()
    res = cur.execute("SELECT * FROM objects WHERE visibleOn <= ? AND (disappearedOn >= ? OR disappearedOn IS NULL)", (timestamp, timestamp))
    objects = res.fetchall()

    with open(config, "r") as f:
        for line in f:
            key, value = line.split("=", 2)
            config_opts[key.strip()] = value.strip()

    # session = boto3.Session(region_name=config_opts["region"])
    # client = session.client(
    #     "s3", 
    #     endpoint_url=config_opts["endpoint"],
    #     # config=botocore.client.Config(s3={"addressing_style": "path"}),
    #     aws_access_key_id=config_opts["accessKey"],
    #     aws_secret_access_key=config_opts["secretKey"]
    # )

    with tarfile.open(f"{output}.tar.gz", "w:gz") as tar:
        for obj in objects:
            content, visible_on, disappeared_on, obj_hash, uri, publication_point = obj
            print(uri)
            path = uri.replace('rsync://', '')

            # s3_content = client.get_object(Bucket=config_opts["bucket"], Key=content)
            # data = s3_content["Body"].read()
            data = base64.b64decode(content)
            tarinfo = tarfile.TarInfo(path)
            tarinfo.size = len(data)
            tarinfo.mtime = int(visible_on) / 1000
            tar.addfile(tarinfo, io.BytesIO(data))

            # folder, file = path.rsplit("/", 1)
            # os.makedirs(f"{folder}", exist_ok=True)
            # with open(f"{folder}/{file}", "wb") as f:
            #     f.write(base64.b64decode(content))