from pkg.core import Greeter, make_greeter

def run():
    g = make_greeter()
    return g.greet("world")
