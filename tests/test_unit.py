import pytest
from unittest.mock import Mock, patch, MagicMock
from app import app, get_table, get_item, create_resource, parse_date
import database_commands as db
from datetime import datetime, date
from flask import Flask

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

@pytest.fixture
def mock_db_connection():
    return MagicMock()

# Tests for app.py
class TestAppRoutes:
    @pytest.mark.unit
    @patch('database_commands.connect_to_database')
    @patch('database_commands.all_columns')
    @patch('database_commands.get_tables_and_columns')
    @patch('database_commands.get_all_data')
    @patch('database_commands.get_column_data_type')
    @patch('database_commands.close_connection')
    def test_home_route(self, mock_close, mock_data_type, mock_all_data, mock_tables_cols, mock_all_cols, mock_connect, client, mock_db_connection):
        mock_connect.return_value = mock_db_connection
        mock_all_cols.return_value = {'test_table': ['col1', 'col2']}
        mock_tables_cols.return_value = (['test_table'], {'test_table': ['col1', 'col2']})
        mock_all_data.return_value = {}
        mock_data_type.return_value = 'integer'
        
        response = client.get('/')
        assert response.status_code == 200

    @pytest.mark.unit
    @patch('database_commands.connect_to_database')
    @patch('database_commands.get_tables_and_columns')
    @patch('database_commands.close_connection')
    def test_get_tables(self, mock_close, mock_tables_cols, mock_connect, client, mock_db_connection):
        mock_connect.return_value = mock_db_connection
        mock_tables_cols.return_value = (['table1', 'table2'], {})
        
        response = client.get('/api/tables')
        assert response.status_code == 200
        assert response.json == ['table1', 'table2']

    @pytest.mark.unit
    @patch('database_commands.connect_to_database')
    @patch('database_commands.get_tables_and_columns')
    @patch('database_commands.close_connection')
    def test_get_columns(self, mock_close, mock_tables_cols, mock_connect, client, mock_db_connection):
        mock_connect.return_value = mock_db_connection
        expected_cols = {'table1': ['col1', 'col2'], 'table2': ['col3']}
        mock_tables_cols.return_value = (['table1', 'table2'], expected_cols)
        
        response = client.get('/api/columns')
        assert response.status_code == 200
        assert response.json == expected_cols

class TestParseFunctions:
    @pytest.mark.unit
    def test_parse_date_valid(self):
        result = parse_date("2024-01-15")
        assert result == date(2024, 1, 15)

    @pytest.mark.unit
    def test_parse_date_invalid(self):
        with pytest.raises(ValueError):
            parse_date("2024/01/15")

    @pytest.mark.unit
    def test_parse_date_wrong_format(self):
        with pytest.raises(ValueError):
            parse_date("01-15-2024")

    @pytest.mark.unit
    def test_parse_date_wrong_format(self):
        with pytest.raises(ValueError):
            parse_date("2024-15-01")

# Tests for database_commands.py
class TestDatabaseConnection:
    @pytest.mark.unit
    @patch('database_commands.psycopg2.connect')
    def test_connect_to_database_success(self, mock_psycopg2_connect, mock_db_connection):
        mock_psycopg2_connect.return_value = mock_db_connection
        
        result = db.connect_to_database()
        assert result == mock_db_connection
        mock_psycopg2_connect.assert_called_once()

    @pytest.mark.unit
    @patch('database_commands.psycopg2.connect')
    def test_connect_to_database_failure(self, mock_psycopg2_connect, capsys):
        mock_psycopg2_connect.side_effect = Exception("Connection failed")
        result = None
        try:
            result = db.connect_to_database(password="fail")
        except Exception as e:
            assert str(e) == "Connection failed"
        assert result is None

    @pytest.mark.unit
    def test_close_connection(self):
        mock_connection = MagicMock()
        db.close_connection(mock_connection)
        mock_connection.close.assert_called_once()

    @pytest.mark.unit
    def test_close_connection_none(self):
        db.close_connection(None)

class TestDatabaseQueries:
    @pytest.mark.unit
    def test_get_cursor_success(self):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        result = db.get_cursor(mock_connection)
        assert result == mock_cursor

    @pytest.mark.unit
    def test_get_cursor_none_connection(self, capsys):
        result = db.get_cursor(None)
        assert result is None

    @pytest.mark.unit
    def test_get_uniques(self):
        data = ['a', 'b', 'a', 'c', 'b']
        result = db.get_uniques(data)
        assert result == ['a', 'b', 'c']

    @pytest.mark.unit
    def test_get_uniques_empty(self):
        result = db.get_uniques([])
        assert result == []

    @pytest.mark.unit
    @patch('database_commands.get_cursor')
    def test_all_columns(self, mock_get_cursor):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_cursor.return_value = mock_cursor
        
        mock_cursor.fetchall.side_effect = [
            [('table1',), ('table2',)],
            [('col1',), ('col2',)],
            [('col3',)]
        ]
        
        result = db.all_columns(mock_connection)
        assert 'table1' in result
        assert 'table2' in result

    @pytest.mark.unit
    @patch('database_commands.get_cursor')
    @patch('database_commands.get_columns_from_table')
    def test_get_table(self, mock_get_cols, mock_get_cursor):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_cursor.return_value = mock_cursor
        mock_get_cols.return_value = ['id', 'name']
        
        mock_cursor.fetchall.return_value = [(1, 'test')]
        
        result = db.get_table(mock_connection, 'test_table')
        assert isinstance(result, list)

    @pytest.mark.unit
    @patch('database_commands.get_cursor')
    def test_get_column(self, mock_get_cursor):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_cursor.return_value = mock_cursor
        
        mock_cursor.fetchall.return_value = [(1,), (2,), (3,)]
        
        result = db.get_column(mock_connection, 'test_table', 'id')
        assert result == [1, 2, 3]

    @pytest.mark.unit
    @patch('database_commands.get_cursor')
    def test_get_column_error(self, mock_get_cursor):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_cursor.return_value = mock_cursor
        mock_cursor.fetchall.side_effect = Exception("Query failed")
        
        result = db.get_column(mock_connection, 'test_table', 'id')
        assert result == 0

    @pytest.mark.unit
    @patch('database_commands.get_cursor')
    def test_get_primary_key(self, mock_get_cursor):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_cursor.return_value = mock_cursor
        
        mock_cursor.fetchall.return_value = [('id',)]
        
        result = db.get_primary_key(mock_connection, 'test_table')
        assert result == 'id'

    @pytest.mark.unit
    @patch('database_commands.get_cursor')
    def test_get_primary_key_no_result(self, mock_get_cursor):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_cursor.return_value = mock_cursor
        
        mock_cursor.fetchall.return_value = []
        
        result = db.get_primary_key(mock_connection, 'test_table')
        assert result == 0

    @pytest.mark.unit
    @patch('database_commands.get_cursor')
    @patch('database_commands.get_columns_from_table')
    def test_get_id(self, mock_get_cols, mock_get_cursor):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_cursor.return_value = mock_cursor
        mock_get_cols.return_value = ['id', 'name']
        
        mock_cursor.fetchall.return_value = [(1, 'test')]
        
        result = db.get_id(mock_connection, 'test_table', 'id', 1)
        assert isinstance(result, list)

class TestDataTypes:
    @pytest.mark.unit
    @patch('database_commands.get_cursor')
    def test_get_column_data_type(self, mock_get_cursor):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_cursor.return_value = mock_cursor
        
        mock_cursor.fetchall.return_value = [('id', 'integer'), ('name', 'varchar')]
        
        result = db.get_column_data_type(mock_connection, 'test_table', 'id')
        assert result is None or isinstance(result, str)

    @pytest.mark.unit
    @patch('database_commands.get_cursor')
    def test_get_column_data_type_error(self, mock_get_cursor):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_cursor.return_value = mock_cursor
        mock_cursor.fetchall.side_effect = Exception("Query failed")
        
        result = db.get_column_data_type(mock_connection, 'test_table', 'id')

class TestExecuteQueries:
    @pytest.mark.unit
    @patch('database_commands.get_cursor')
    def test_execute_single_query_string(self, mock_get_cursor):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_cursor.return_value = mock_cursor
        
        db.execute_single_query(mock_connection, "SELECT * FROM test")
        mock_cursor.execute.assert_called_once()

    @pytest.mark.unit
    @patch('database_commands.get_cursor')
    def test_execute_single_query_with_params(self, mock_get_cursor):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_get_cursor.return_value = mock_cursor
        
        db.execute_single_query(mock_connection, ("SELECT * FROM test WHERE id = %s", (1,)))
        mock_cursor.execute.assert_called_once()

    @pytest.mark.unit
    def test_execute_single_query_no_connection(self):
        db.execute_single_query(None, "SELECT * FROM test")

    @pytest.mark.unit
    @patch('database_commands.execute_single_query')
    def test_execute_multi_query(self, mock_execute_single):
        mock_connection = MagicMock()
        queries = [("INSERT INTO test VALUES (%s)", (1,)), ("INSERT INTO test VALUES (%s)", (2,))]
        
        db.execute_multi_query(mock_connection, queries)
        assert mock_execute_single.call_count == 2

    @pytest.mark.unit
    def test_execute_multi_query_no_connection(self):
        result = db.execute_multi_query(None, [])
        assert result == 0
