# -*- coding: utf-8 -*-

import threading
import uuid
import sys

from django.conf import settings
import django

try:
    import psycopg2
except ImportError:
    print("psycopg2 import error, djorm_core.postgres modulue "
          "is only compatible with postgresql_psycopg2 backend")
    sys.exit(-1)



_local_data = threading.local()


class server_side_cursors(object):
    def __init__(self, itersize=None, withhold=False):
        self.itersize = itersize
        self.withhold = withhold

    def __enter__(self):
        self.old_itersize = getattr(_local_data, 'itersize', None)
        self.old_withhold = getattr(_local_data, 'withhold', None)
        self.old_cursors = getattr(_local_data, 'server_side_cursors', False)
        _local_data.itersize = self.itersize
        _local_data.withhold = self.withhold
        _local_data.server_side_cursors = True

    def __exit__(self, type, value, traceback):
        _local_data.itersize = self.old_itersize
        _local_data.withhold = self.old_withhold
        _local_data.server_side_cursors = self.old_cursors


def patch_cursor_wrapper_django_lt_1_6():
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
            self.cs_cursor = self.cursor    # client-side cursor

            name = uuid.uuid4().hex
            self.cursor = connection.cursor(name="cur{0}".format(name),
                    withhold=getattr(_local_data, 'withhold', False))
            self.cursor.tzinfo_factory = cursor.tzinfo_factory

            self.ss_cursor = self.cursor    # server-side cursor

            if getattr(_local_data, 'itersize', None):
                self.cursor.itersize = _local_data.itersize

        def choose_cursor(self, query):
            if query.startswith('SELECT'):      # server-side cursor only for
                self.cursor=self.ss_cursor      # SELECT statement
            else:
                self.cursor=self.cs_cursor

        def execute(self, query, args=None):
            self.choose_cursor(query)
            super(CursorWrapper, self).execute(query, args)

        def executemany(self, query, args):
            self.choose_cursor(query)
            super(CursorWrapper, self).executemany(query, args)

    base.CursorWrapper = CursorWrapper


def patch_cursor_wrapper_django_gte_1_6():
    from django.db.backends.postgresql_psycopg2 import base
    if hasattr(base, "_ssc_patched"):
        return

    base._ssc_patched = True

    old_create_cursor = base.DatabaseWrapper.create_cursor
    def new_create_cursor(self):
        if getattr(_local_data, 'server_side_cursors', False):
            name = uuid.uuid4().hex
            cursor = self.connection.cursor(name="cur{0}".format(name))
            cursor.tzinfo_factory = base.utc_tzinfo_factory if settings.USE_TZ else None
            return cursor

        return old_create_cursor(self)

    base.DatabaseWrapper.create_cursor = new_create_cursor


if django.VERSION[:2] < (1, 6):
    patch_cursor_wrapper_django_lt_1_6()
else:
    patch_cursor_wrapper_django_gte_1_6()
