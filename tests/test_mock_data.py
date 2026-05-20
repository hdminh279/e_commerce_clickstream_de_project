"""
Unit tests for mock_data.py

Tests the state machine logic of UserSession:
- Initial state
- State transitions
- Payload structure
- Edge cases (drop, deactivation)
"""

import sys
from unittest.mock import patch
sys.path.insert(0, 'scripts')

from mock_data import UserSession, EventClick, Product


def test_initial_state():
    """A new UserSession starts with correct default values."""
    session = UserSession()

    assert session.current_state == "page_view"
    assert session.is_active is True
    assert session.cart_total == 0.0
    assert session.session_id is not None
    assert session.client_id is not None


def test_payload_excludes_internal_fields():
    """Internal fields should not leak into the Kafka payload."""
    session = UserSession()
    payload = session.get_next_event()

    assert "is_active" not in payload
    assert "current_state" not in payload
    assert "current_product" not in payload

def test_cart_total():
    """Check when add_to_cart, money increase"""
    session = UserSession()

    session.current_state = "add_to_cart"
    session.get_next_event()

    total_1 = session.cart_total

    session.is_activate = True
    session.current_state = "add_to_cart"
    session.get_next_event()

    total_2 = session.cart_total

    assert total_1 < total_2

def test_drop_deactivates_session():
    """Check when usser drop, session down"""
    session = UserSession()

    session.current_state = "drop"
    session._advance_state()

    assert session.is_active is False
    assert session.get_next_event() is None

def test_purchase_creates_transaction_id():
    "Check event purchase must have transaction_id"
    session1 = UserSession()

    session1.current_state = "view_item"
    payload_view = session1.get_next_event()

    assert payload_view.get("transaction_id") is None

    session2 = UserSession()
    session2.current_state = "purchase"
    payload_purchase = session2.get_next_event()

    assert payload_purchase.get("transaction_id") is not None
    assert payload_purchase["transaction_id"].startswith("TXN-")

@patch('mock_data.random.choices')
def test_state_advances_after_event(mock_choices):
    mock_choices.return_value = ['add_to_cart']

    session = UserSession()
    assert session.current_state == "page_view"

    session._advance_state()

    assert session.current_state == "add_to_cart"
    
