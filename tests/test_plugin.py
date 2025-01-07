import pytest
from sqlalchemy.engine import URL

from geoalchemy2.admin.plugin import GeoEngine


def test_geo_engine_init():
    """Test GeoEngine initialization with different URL parameters."""
    # Test with no parameters
    url = URL.create("sqlite:///test.db")
    plugin = GeoEngine(url, {})
    assert dict(plugin.params["connect"]["sqlite"]) == {}

    # Test with all SQLite parameters
    url = URL.create(
        "sqlite:///test.db",
        query={
            "geoalchemy2_connect_sqlite_transaction": "true",
            "geoalchemy2_connect_sqlite_init_mode": "WGS84",
            "geoalchemy2_connect_sqlite_journal_mode": "OFF",
        },
    )
    plugin = GeoEngine(url, {})

    assert plugin.params["connect"]["sqlite"] == {
        "transaction": True,
        "init_mode": "WGS84",
        "journal_mode": "OFF",
    }


@pytest.mark.parametrize(
    "value,expected",
    [
        ("true", True),
        ("True", True),
        ("1", True),
        ("yes", True),
        ("false", False),
        ("False", False),
        ("0", False),
        ("no", False),
        ("anything_else", False),
    ],
)
def test_str_to_bool(value, expected):
    """Test string to boolean conversion."""
    assert GeoEngine.str_to_bool(value) == expected


def test_update_url():
    """Test URL parameter cleanup."""
    url = URL.create(
        "sqlite:///test.db",
        query={
            "geoalchemy2_connect_sqlite_transaction": "true",
            "geoalchemy2_connect_sqlite_init_mode": "WGS84",
            "geoalchemy2_connect_sqlite_journal_mode": "OFF",
            "other_param": "value",
        },
    )
    plugin = GeoEngine(url, {})
    updated_url = plugin.update_url(url)

    # Check that GeoEngine parameters are removed
    assert "geoalchemy2_connect_sqlite_transaction" not in updated_url.query
    assert "geoalchemy2_connect_sqlite_init_mode" not in updated_url.query
    assert "geoalchemy2_connect_sqlite_journal_mode" not in updated_url.query

    # Check that other parameters are preserved
    assert updated_url.query["other_param"] == "value"
