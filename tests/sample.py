from pypeflow.kernels import *
from pypeflow.construct import PipeSystem

inp1 = Inlet(name="Inlet 1")
x = Categorizer(name="categorizer 1", sparse=True)(inp1)

inp2 = Inlet(name="Inlet 2")
y = Categorizer(name="categorizer 2", sparse=True)(inp2)

conc = Concatenator(axis=1)(x, y)

out = Outlet(name="Outlet 1")(conc)

ps = PipeSystem(inlets=[inp1, inp2], outlets=[out], name="Pipe system")

