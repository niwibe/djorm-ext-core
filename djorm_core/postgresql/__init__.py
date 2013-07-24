# -*- coding: utf-8 -*-

import sys

try:
    import psycopg2
except ImportError:
    print("psycopg2 import error, djorm_core.postgres modulue "
          "is only compatible with postgresql_psycopg2 backend")
    sys.exit(-1)


import threading
import uuid


_local_data = threading.local()


class server_side_cursors(object):
    def __init__(self, itersize=None):
        self.itersize = None

    def __enter__(self):
        self.old_itersize = getattr(_local_data, 'itersize', None)
        self.old_cursors = getattr(_local_data, 'server_side_cursors', False)
        _local_data.itersize = self.itersize
        _local_data.server_side_cursors = True

    def __exit__(self, type, value, traceback):
        _local_data.itersize = self.old_itersize
        _local_data.server_side_cursors = self.old_cursors


def patch_cursor_wrapper():
    from django.db.backends.postgresql_psycopg2 import base

    if hasattr(base, "_CursorWrapper"):
        return

    base._CursorWrapper = base.CursorWrapper

    class CursorWrapper(base._CursorWrapper):
        def __init__(self, *args, **kwargs):
            super(CursorWrapper, self).__init__(*args, **kwargs)

            if not getattr(_local_data, 'server_side_cursors', False):
                return

            connection = self.cursor.connection
            cursor = self.cursor

            name = uuid.uuid4().hex
            self.cursor = connection.cursor(name="cur{0}".format(name))
            self.cursor.tzinfo_factory = cursor.tzinfo_factory

            if getattr(_local_data, 'itersize', None):
                self.cursor.itersize = _local_data.itersize

    base.CursorWrapper = CursorWrapper


patch_cursor_wrapper()
