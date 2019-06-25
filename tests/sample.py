from pypeflow.kernels import *
from pypeflow.construct import PipeSystem
import pandas as pd
from pandas import DataFrame

inp1 = Inlet(name="Inlet 1")
x = Categorizer(name="categorizer 1", sparse=True)(inp1)

inp2 = Inlet(name="Inlet 2")
y = Categorizer(name="categorizer 2", sparse=True)(inp2)

conc = Concatenator(axis=1)(x, y)

out = Outlet(name="Outlet 1")(conc)

ps = PipeSystem(inlets=[inp1, inp2], outlets=[out], name="Pipe system")

df1 = DataFrame({"int1": [1, 3, 4, 1, 0], "str": ["test", "hello", "test", "fire", "test"]})

