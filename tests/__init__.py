"""
Test suite for DeltaDyno trading system.

Structure:
- tests/unit/           Unit tests (fast, isolated)
- tests/integration/    Integration tests (slower, multi-component)
- tests/scenarios/      End-to-end scenario tests

Run all tests:
    pytest

Run specific category:
    pytest -m unit
    pytest -m integration
    pytest -m scenario

Run with coverage:
    pytest --cov=deltadyno --cov-report=html
"""
