import pytest
import json
from app import app as flask_app
from app import parse_date
import database_commands as db

@pytest.fixture(scope='module')
def test_client():
    flask_app.config['TESTING'] = True
    with flask_app.test_client() as testing_client:
        yield testing_client

@pytest.fixture(scope='module')
def db_connection():
    conn = db.connect_to_database()
    yield conn
    db.close_connection(conn)

@pytest.mark.integration
def test_homepage(test_client):
    response = test_client.get('/')
    assert response.status_code == 200
    assert b'Weather Dashboard API' in response.data

@pytest.mark.integration
def test_get_tables(db_connection):
    tables, columns = db.get_tables_and_columns(db.all_columns(db_connection)) 
    assert isinstance(tables, list) and isinstance(columns, dict)

@pytest.mark.integration
def test_get_columns(db_connection):
    table_name = "DMI"
    columns = db.get_columns_from_table(db_connection, table_name)
    assert isinstance(columns, list)

@pytest.mark.integration
def test_get_column_data_type(db_connection):
    table_name = "DMI"
    column_name = "value"
    data_type = db.get_column_data_type(db_connection, table_name, column_name)
    assert data_type is not None and data_type == "double precision"

@pytest.mark.integration
def test_get_measurement_from_station(db_connection):
    station = "DMI"
    measurement_type = "temp_dry"
    timespan = {}
    timespan["from"] = parse_date("2026-03-21")
    timespan["to"] = parse_date("2026-03-23")
    measurements = db.get_measurement_from_station(db_connection, station, measurement_type, timespan)
    assert measurements is not None and measurements != 0

@pytest.mark.integration
def test_get_latest_measurement(db_connection):
    measurements = db.get_latest_measurement(db_connection)
    assert measurements is not None and measurements != 0
    assert isinstance(measurements, dict)
    assert "DMI" in measurements or "BME280" in measurements or "DS18B20" in measurements or "SCD41" in measurements
    for source, data in measurements.items():
        assert isinstance(data, dict)
        assert "humidity" in data
        assert "pressure" in data
        assert "temperature" in data

@pytest.mark.integration
def test_api_get_stations(test_client):
    stations = test_client.get('/Stations/')
    assert stations.status_code == 200
    assert stations is not None
    assert b"DMI" in stations.data
    assert b"BME280" in stations.data
    assert b"SCD41" in stations.data
    assert b"DS18B20" in stations.data
    assert isinstance(json.loads(stations.data), dict)
    assert len(stations.data) > 0

@pytest.mark.integration
def test_api_get_measurements(test_client):
    station = "DMI"
    measurement_type = "temp_dry"
    from_date = parse_date("2026-03-21")
    to_date = parse_date("2026-03-23")
    response = test_client.get(f'/Measurements/?station={station}&type={measurement_type}&from={from_date}&to={to_date}')
    assert response.status_code == 200
    assert isinstance(json.loads(response.data), list)

@pytest.mark.integration
def test_api_get_latest_measurements(test_client):
    response = test_client.get('/LatestMeasurements/')
    assert response.status_code == 200
    assert b"DMI" in response.data
    assert b"BME280" in response.data
    assert b"SCD41" in response.data
    assert b"DS18B20" in response.data
    assert isinstance(json.loads(response.data), dict)