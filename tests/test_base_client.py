import pytest
from src.clients.base_client import BaseClient


class ConcreteClient(BaseClient):
    def __init__(self, config):
        super().__init__(config)
        self._connected = False
    
    def connect(self):
        self._connected = True
        self.connection = "mock_connection"
    
    def disconnect(self):
        self._connected = False
        self.connection = None
    
    def execute_query(self, query, params=None):
        if not self._connected:
            raise ConnectionError("Not connected")
        return [{"result": "data"}]
    
    def is_connected(self):
        return self._connected


class TestBaseClient:
    
    def test_initialization(self):
        config = {'host': 'localhost', 'port': 5432}
        client = ConcreteClient(config)
        
        assert client.config == config
        assert client.connection is None
    
    def test_context_manager_connect_disconnect(self):
        config = {'host': 'localhost'}
        client = ConcreteClient(config)
        
        assert not client.is_connected()
        
        with client:
            assert client.is_connected()
            assert client.connection == "mock_connection"
        
        assert not client.is_connected()
        assert client.connection is None
    
    def test_context_manager_with_query(self):
        config = {'host': 'localhost'}
        client = ConcreteClient(config)
        
        with client:
            results = client.execute_query("SELECT * FROM table")
            assert results == [{"result": "data"}]
    
    def test_context_manager_exception_handling(self):
        config = {'host': 'localhost'}
        client = ConcreteClient(config)
        
        try:
            with client:
                assert client.is_connected()
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        assert not client.is_connected()
    
    def test_manual_connect_disconnect(self):
        config = {'host': 'localhost'}
        client = ConcreteClient(config)
        
        client.connect()
        assert client.is_connected()
        
        client.disconnect()
        assert not client.is_connected()
    
    def test_execute_query_not_connected(self):
        config = {'host': 'localhost'}
        client = ConcreteClient(config)
        
        with pytest.raises(ConnectionError, match="Not connected"):
            client.execute_query("SELECT * FROM table")
    
    def test_abstract_methods_must_be_implemented(self):
        with pytest.raises(TypeError):
            BaseClient({'host': 'localhost'})
