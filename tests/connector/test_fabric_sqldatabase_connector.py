from __future__ import annotations

import unittest
from typing import Any

from enterprise_fabric_framework.connector import FabricSqlDatabase


class _Context:
    def __init__(self) -> None:
        self.entered = False
        self.exited = False

    def __enter__(self) -> Any:
        self.entered = True
        return object()

    def __exit__(self, *arguments: object) -> None:
        self.exited = True


class _Engine:
    def __init__(self) -> None:
        self.connection_context = _Context()
        self.dispose_calls = 0

    def connect(self) -> _Context:
        return self.connection_context

    def dispose(self) -> None:
        self.dispose_calls += 1


class FabricSqlDatabaseTests(unittest.TestCase):
    def test_new_session_opens_a_connection_context(self) -> None:
        engine = _Engine()
        database = FabricSqlDatabase.from_engine(engine)

        with database.new_session():
            pass

        self.assertTrue(engine.connection_context.entered)
        self.assertTrue(engine.connection_context.exited)

    def test_renew_and_close_dispose_the_connection_pool(self) -> None:
        engine = _Engine()
        database = FabricSqlDatabase.from_engine(engine)

        database.renew()
        database.close()

        self.assertEqual(engine.dispose_calls, 2)


if __name__ == "__main__":
    unittest.main()