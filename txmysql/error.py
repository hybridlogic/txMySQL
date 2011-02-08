class MySQLError(Exception):
    def __init__(self, message, errno=None, sqlstate=None):
        super(MySQLError, self).__init__(message, errno, sqlstate)
        self.msg, self.errno, self.sqlstate = message, errno, sqlstate
