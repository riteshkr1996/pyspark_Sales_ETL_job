class logger:

    def __init__(self, flame):
        self.flame = flame

    def info(self, msg):
        f = open(self.flame, "a")
        f.write(msg)

    def error(self, error_message):
        f = open(self.flame, "a")
        f.write(error_message)
