import os

class TestC(object):
    fname = None
    def g1(self, val):
        self.fname=val.replace("$WS_HOME", os.getcwd())

    def f(self):
        key='g1'
        getattr(self, key)("$WS_HOME/templates/1000_histogram") 

    def h(self):
        self.f()

cc = TestC()
cc.h()
print(cc.fname)