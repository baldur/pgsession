from notebook.services.sessions.sessionmanager import SessionManager
import psycopg2
import psycopg2.extras
import urlparse
import os


class PgSessionManager(SessionManager):
    @property
    def cursor(self):
        if self._cursor is None:
            self._cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            self._cursor.execute("""CREATE TABLE IF NOT EXISTS session
                (session_id TEXT, path TEXT, kernel_id TEXT)""")
        return self._cursor

    @property
    def connection(self):
        result = urlparse.urlparse(os.environ['NOTEBOOK_DATABASE_URL'])
        username = result.username
        password = result.password
        database = result.path[1:]
        hostname = result.hostname
        """Start a database connection"""
        if self._connection is None:
            self._connection = psycopg2.connect(database = database,
                                                user = username,
                                                password = password,
                                                host = hostname)
        return self._connection

    def session_exists(self, path):
        """Check to see if the session for a given notebook exists"""
        if self._connection:
            self._connection.commit()
        self.cursor.execute("SELECT * FROM session WHERE path= '%s'" % (path))
        reply = self.cursor.fetchone()
        if reply is None:
            return False
        else:
            return True

    def save_session(self, session_id, path=None, kernel_id=None):
        self.cursor.execute("INSERT INTO session VALUES (%s,%s,%s)",
            (session_id, path, kernel_id)
        )
        return self.get_session(session_id=session_id)


    def get_session(self, **kwargs):
        if not kwargs:
            raise TypeError("must specify a column to query")

        conditions = []
        for column in kwargs.keys():
            if column not in self._columns:
                raise TypeError("No such column: %r", column)
            conditions.append("%s=%%s" % column)

        query = "SELECT * FROM session WHERE %s" % (' AND '.join(conditions))

        self.cursor.execute(query, list(kwargs.values()))
        try:
            row = self.cursor.fetchone()
        except KeyError:
            # The kernel is missing, so the session just got deleted.
            row = None

        if row is None:
            q = []
            for key, value in kwargs.items():
                q.append("%s=%r" % (key, value))

            raise web.HTTPError(404, u'Session not found: %s' % (', '.join(q)))

        return self.row_to_model(row)

    def close(self):
        """Close the sqlite connection"""
        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None

    def __del__(self):
        """Close connection once SessionManager closes"""
        self.close()

    def update_session(self, session_id, **kwargs):
        self.get_session(session_id=session_id)

        if not kwargs:
            # no changes
            return

        sets = []
        for column in kwargs.keys():
            if column not in self._columns:
                raise TypeError("No such column: %r" % column)
            sets.append("%s=%%s" % column)
        query = "UPDATE session SET %s WHERE session_id='%s'" % (', '.join(sets))
        self.cursor.execute(query % list(kwargs.values()) + [session_id])

    def row_to_model(self, row):
        """Takes sqlite database session row and turns it into a dictionary"""
        if row['kernel_id'] not in self.kernel_manager:
            # The kernel was killed or died without deleting the session.
            # We can't use delete_session here because that tries to find
            # and shut down the kernel.
            query = "DELETE FROM session WHERE session_id='%s'" % row['session_id']
            self.cursor.execute(query)
            raise KeyError

        model = {
            'id': row['session_id'],
            'notebook': {
                'path': row['path']
            },
            'kernel': self.kernel_manager.kernel_model(row['kernel_id'])
        }
        return model

    def delete_session(self, session_id):
        """Deletes the row in the session database with given session_id"""
        # Check that session exists before deleting
        session = self.get_session(session_id=session_id)
        self.kernel_manager.shutdown_kernel(session['kernel']['id'])
        query = "DELETE FROM session WHERE session_id=%s" % session_id
        self.cursor.execute(query)

    def list_sessions(self):
        """Returns a list of dictionaries containing all the information from
        the session database"""
        if self._connection:
            self._connection.commit()
        self.cursor.execute("SELECT * FROM session")
        result = []
        # We need to use fetchall() here, because row_to_model can delete rows,
        # which messes up the cursor if we're iterating over rows.
        for row in self.cursor.fetchall():
            try:
                result.append(self.row_to_model(row))
            except KeyError:
                pass
        return result
