class MySQLError(Exception):
    def __init__(self, message, errno=None, sqlstate=None, query=None):
        super(MySQLError, self).__init__(message, errno, sqlstate, query)
        self.msg, self.errno, self.sqlstate, self.query = message, errno, sqlstate, query
