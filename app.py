from influxdb import InfluxDBClient
from datetime import datetime, timezone
from flask import Flask, jsonify, flash, request
import constants
import pandas as pd
import json


app = Flask(__name__)

client = InfluxDBClient(host=constants.INFLUXDB_HOST,
                        port=constants.INFLUXDB_PORT,
                        username=constants.INFLUXDB_USER,
                        password=constants.INFLUXDB_PASSWORD,
                        database=constants.INFLUXDB_DATABASE)

# client.switch_database(constants.INFLUXDB_DATABASE)


ALLOWED_EXTENSIONS = set(['csv', 'parquet'])


def allowed_file(filename):
	return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@app.route('/api/databases/list', methods=['GET'])
def get_databases():
    databases = client.get_list_database()
    return jsonify(databases)


@app.route('/api/databases/<database_name>', methods=['POST'])
def create_database(database_name):
    client.create_database(database_name)
    return jsonify(f'{database_name} created')


@app.route('/api/sensors/files', methods=['POST'])
def insert_data_csv():
    if 'file' not in request.files:
        return jsonify('No file part')
    file = request.files['file']
    if file and allowed_file(file.filename):
        df = pd.read_csv(file)
    df['_time'] = pd.to_datetime(df['_time'], format="%Y-%m-%dT%H:%M:%SZ")
    df.set_index(['_time'])
    json_payload = []

    for index, row in df.iterrows():
        data = {
            "measurement": row["_measurement"],
            "tags": {
                "sensor_id": row["sensor_id"],
            },
            "time": row["_time"],
            "fields": {
                '_value': row["_value"],
                '_field': row["_field"]
            }
        }
        json_payload.append(data)
    client.write_points(json_payload)
    return jsonify(json_payload)


@app.route('/api/sensors/<measurement_name>', methods=['GET'])
def get_data_points(measurement_name):
    args = request.args
    start_time = args.get('start_timestamp')
    end_time = args.get('end_timestamp')

    if None not in (start_time, end_time):
        where_clause = f"time > {start_time} AND time < {end_time}"

        query = "SELECT * FROM {} WHERE {} ORDER BY time DESC;".format(measurement_name, where_clause)
        tabledata = client.query(query)
    else:
        tabledata = client.query(
    	    f'select * from {measurement_name} ORDER BY time DESC')

    data_points = []
    for measurement, tags in tabledata.keys():
        for p in tabledata.get_points(measurement=measurement_name, tags=tags):
            data_points.append(p)

    return jsonify(data_points)


if __name__ == "__main__":
    app.run()
