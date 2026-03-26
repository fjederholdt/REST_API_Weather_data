import database_commands as db
from flask import Flask, render_template, jsonify
from flask_restx import Api, Namespace, Resource, fields, Model, reqparse
from datetime import datetime

app = Flask(__name__)

''' Create the homepage for the API, which will display all the tables and columns in the database, as well as all the data in the database. '''
@app.route("/")
def home():
    con = db.connect_to_database()
    tables, columns = db.get_tables_and_columns(db.all_columns(con))
    total_data = db.get_all_data(con)
    column_data_types = {}
    for table in tables:
        for column in columns[table]:
            ''' Get the data type of the column, and create a model field for it.'''
            data_type = db.get_column_data_type(con, table, column)
            input_type = ""
            match data_type:
                case "integer" | "numeric" | "double precision":
                    input_type = "number"
                case "character varying" | "uuid":
                    input_type = "text"
                case "timestamp with zone": 
                    input_type = "datetime-local"
            column_data_types[(table, column)] = input_type
    db.close_connection(con)
    return render_template("index.html", tables=tables, columns=columns, total_data=total_data, column_data_types=column_data_types)

@app.route("/api/tables", methods=["GET"])
def get_tables():
    con = db.connect_to_database()
    tables, columns = db.get_tables_and_columns(db.all_columns(con))
    db.close_connection(con)
    return jsonify(tables)

@app.route("/api/columns", methods=["GET"])
def get_columns():
    con = db.connect_to_database()
    tables, columns = db.get_tables_and_columns(db.all_columns(con))
    db.close_connection(con)
    return jsonify(columns)

@app.route("/api/column_types", methods=["GET"])
def get_column_types():
    con = db.connect_to_database()
    tables, columns = db.get_tables_and_columns(db.all_columns(con))
    column_data_types = {}
    for table in tables:
        for column in columns[table]:
            ''' Get the data type of the column, and create a model field for it.'''
            data_type = db.get_column_data_type(con, table, column)
            input_type = ""
            match data_type:
                case "integer" | "numeric" | "double precision":
                    input_type = "number"
                case "character varying" | "uuid":
                    input_type = "text"
                case "timestamp with zone": 
                    input_type = "datetime-local"
            column_data_types[(table, column)] = input_type
    
    db.close_connection(con)
    return jsonify(column_data_types)

''' Create the API to handle docs and all the endpoints for the API.'''
api = Api(
    app,
    title="Weather Dashbord API",
    version="1.0",
    description="API for the weather dashbord",
    doc="/docs"
)

def get_table(self: Api):
    ''' Get all items from the table in the database, and return them as a JSON object.'''
    table = self.table
    conn = db.connect_to_database()
    data = db.get_table(conn, table)
    db.close_connection(conn)
    return data

def get_item(self: Api, id):
    ''' Get a single item with the specified ID from the table in the database, and return it as a JSON object.'''
    table = self.table
    conn = db.connect_to_database()
    column = db.get_primary_key(conn, table)
    print(column)
    data = db.get_id(conn, table, column, id)
    db.close_connection(conn)
    return data

def create_resource(table_name, namespace: Namespace, model_fields):
    model = namespace.model(f"{table_name}", model_fields)
    table_attrs = {
        "table": table_name,
    }
    
    table_attrs["get"] = namespace.marshal_list_with(model)(get_table)
    TableResourceClass = type(f"{table_name}", (Resource,), table_attrs)

    prim_key_attrs = {
        "table": table_name,
    }
    
    prim_key_attrs["get"] = namespace.marshal_list_with(model)(get_item)
    PrimKeyResourceClass = type("Primary_Key", (Resource,), prim_key_attrs)
    
    return TableResourceClass, PrimKeyResourceClass

''' Create the API documentation for all the tables and columns in the database.'''
def create_api_and_docs():
    con = db.connect_to_database()
    tables, columns = db.get_tables_and_columns(db.all_columns(con))
    namespaces_and_models = []
    for table in tables:
        ''' Create a namespace for each table in the database, and create a model for each column in the table.'''
        namespace = Namespace(table, description=f"Operations related to {table} in the storage system")
        primary_key = db.get_primary_key(con, table)
        if primary_key == 0:
            continue
        primary_key_type = ""
        model_fields = {}
        for column in columns[table]:
            ''' Get the data type of the column, and create a model field for it.'''
            data_type = db.get_column_data_type(con, table, column)
            model = ""
            match data_type:
                case "integer":
                    if column == primary_key:
                        primary_key_type = "int"
                    model = fields.Integer(required=True, description=f"{column} of {table}")
                case "numeric" | "double precision":
                    if column == primary_key:
                        primary_key_type = "float"
                    model = fields.Float(required=True, description=f"{column} of {table}")
                case "character varying":
                    if column == primary_key:
                        primary_key_type = "string"
                    model = fields.String(required=True, description=f"{column} of {table}")
                case "uuid":
                    if column == primary_key:
                        primary_key_type = "string"
                    model = fields.String(required=True, description=f"uuid for {column} of {table}")
                case "timestamp with time zone":
                    if column == primary_key:
                        primary_key_type = "timestamp"
                    model = fields.DateTime(required=True, description=f"timestamp with zone for {column} of {table}")
            
            model_fields[column] = model
        
        namespaces_and_models.append((namespace, model_fields, table, primary_key, primary_key_type))
    
    db.close_connection(con)

    for namespace, model_fields, table, primary_key, primary_key_type in namespaces_and_models:
        ''' Create the endpoints for each table in the database, and use the model to validate the input data.'''
        TableResource, PrimKeyResource = create_resource(table_name=table, namespace=namespace, model_fields=model_fields)
        
        namespace.add_resource(TableResource, f"/")
        namespace.add_resource(PrimKeyResource, f"/<{primary_key_type}:id>")

        api.add_namespace(namespace)        

def get_stations(self: Api):
    con = db.connect_to_database()
    tables, columns = db.get_tables_and_columns(db.all_columns(con))
    stations = {}
    for table in tables:
        if table == "humidity_data" or table == "pressure_data" or table == "temperature_data":
            continue
        column_name = ""
        if table == "DMI":
            column_name = "station_id"
        elif table == "SCD41":
            stations[table] = [table]
            continue
        else:
            column_name = "location"
        
        stations[table] = db.get_uniques(db.get_column(con, table, column_name))
    db.close_connection(con)
    return stations

def get_measurements_from_stations(self: Api):
    args = station_type_date_parser.parse_args()
    station = args["station"]
    measurement_type = args["type"]
    timespan = {}
    timespan["from"] = args["from"]
    timespan["to"] = args["to"]
    con = db.connect_to_database()
    measurements = db.get_measurement_from_station(con, station, measurement_type, timespan)
    db.close_connection(con)
    if measurements == 0:
        return 0
    formatted_measurements = [{"Measurements": x[0]} for x in measurements]
    return formatted_measurements

def get_latest(self):
    con = db.connect_to_database()
    latest_measurements = db.get_latest_measurement(con)
    converted_data = convert_tuples_to_lists(latest_measurements)
    print(latest_measurements)
    db.close_connection(con)
    return latest_measurements

def parse_date(value):
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise ValueError("Date must be in YYYY-MM-DD format")

station_type_date_parser = reqparse.RequestParser()
station_type_date_parser.add_argument(
    "station",
    type=str,
    required=True,
    location="args",
    help="What station table to fetch from"
)
station_type_date_parser.add_argument(
    "type",
    type=str,
    required=True,
    location="args",
    help="The type of measurement, e.g temperature"
)
station_type_date_parser.add_argument(
    "from",
    type=parse_date,
    required=True,
    location="args",
    help="Start date in format YYYY-MM-DD"
)
station_type_date_parser.add_argument(
    "to",
    type=parse_date,
    required=True,
    location="args",
    help="End date in format YYYY-MM-DD"
)

def convert_tuples_to_lists(data):
    if isinstance(data, dict):
        return {k: convert_tuples_to_lists(v) for k, v in data.items()}
    elif isinstance(data, tuple):
        return list(data)
    else:
        return data

def create_station_api_call():
    stations = "Stations"
    stations_namespace = Namespace(stations, "Get a list of all stations")
    station_model_fields = {}
    con = db.connect_to_database()
    tables, columns = db.get_tables_and_columns(db.all_columns(con))
    for table in tables:
        if table == "humidity_data" or table == "pressure_data" or table == "temperature_data":
                continue
        station_model_fields[table] = fields.List(fields.String, description=f"a list of {table} stations")
    db.close_connection(con)
    stations_model = stations_namespace.model(f"{stations}", station_model_fields)
    decorated_method = {}
    decorated_method["get"] = stations_namespace.marshal_with(stations_model)(get_stations)
    StationsResourceClass = type(f"{stations}", (Resource,), decorated_method)
    stations_namespace.add_resource(StationsResourceClass, f"/")
    api.add_namespace(stations_namespace)

def create_measurement_from_station_api_call():
    measurements = "Measurements"
    measurement_from_station_namespace = Namespace(measurements, "Get measurements from stations")
    measurement_from_station_model_fields = {}
    measurement_from_station_model_fields[measurements] = fields.String(description=f"a list of measurements")
    measurement_from_station_model = measurement_from_station_namespace.model(f"{measurements}", measurement_from_station_model_fields)
    decorated_method = {}
    decorated_method["get"] = measurement_from_station_namespace.expect(station_type_date_parser)(get_measurements_from_stations)
    decorated_method["get"] = measurement_from_station_namespace.marshal_list_with(measurement_from_station_model)(get_measurements_from_stations)
    MeasurementsResourceClass = type(f"{measurements}", (Resource,), decorated_method)
    measurement_from_station_namespace.add_resource(MeasurementsResourceClass, f"/")
    api.add_namespace(measurement_from_station_namespace)

def create_latest_measurement_from_all_stations():
    latest = "LatestMeasurements"
    latest_namespace = Namespace(latest, "Get a list of all latest measurements")
    latest_data_fields = {}
    latest_data_fields["humidity"] = fields.Float(required=False, description='Humidity readings or empty')
    latest_data_fields["pressure"] = fields.Float(required=False, description='Pressure readings or empty')
    latest_data_fields["temperature"] = fields.Float(required=False, description='Temperature readings or empty')
    latest_data_model = latest_namespace.model(f"data", latest_data_fields)
    latest_model_fields = {}
    con = db.connect_to_database()
    tables, columns = db.get_tables_and_columns(db.all_columns(con))
    for table in tables:
        if table == "humidity_data" or table == "pressure_data" or table == "temperature_data":
                continue
        latest_model_fields[table] = fields.Nested(latest_data_model)
    db.close_connection(con)
    latest_model = latest_namespace.model(f"{latest}", latest_model_fields)
    decorated_method = {}
    decorated_method["get"] = latest_namespace.marshal_with(latest_model)(get_latest)
    LatestsResourceClass = type(f"{latest}", (Resource,), decorated_method)
    latest_namespace.add_resource(LatestsResourceClass, f"/")
    api.add_namespace(latest_namespace)

create_station_api_call()
create_measurement_from_station_api_call()
create_latest_measurement_from_all_stations()
create_api_and_docs()


if __name__ == "__main__":
    app.run(debug=True)
    