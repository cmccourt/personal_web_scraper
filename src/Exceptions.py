class DBNotAvailable(Exception):
    def __str__(self):
        return "WOOF! There is no database available."
