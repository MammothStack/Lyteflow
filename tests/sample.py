from pypeflow.kernels import *
from pypeflow.construct import PipeSystem
import pandas as pd
import numpy as np

inp1 = Inlet(name="Inlet 1")
x = Categorizer(name="categorizer 1", columns=["str"], sparse=True, absent_ignore=True)(
    inp1
)

inp2 = Inlet(name="Inlet 2")
y = Categorizer(
    name="categorizer 2", columns=["str1"], sparse=True, absent_ignore=True
)(inp2)

conc = Concatenator(axis=1)(x, y)

out = Outlet(name="Outlet 1")(conc)

ps = PipeSystem(inlets=[inp1, inp2], outlets=[out], name="Pipe system")

df1 = pd.DataFrame(
    {"int1": [1, 3, 4, 1, 0], "str": ["test", "hello", "test", "fire", "test"]}
)

df2 = pd.DataFrame({"str1": ["s", "a", "s", "s", "s"], "int2": [3, 4, 3, 3, 4]})

arr = np.array(range(16)).reshape(4, 4)
l = []
for i in range(100):
    l.append(arr.copy())

arr = np.asarray(l)

brr = [1, 2, 2, 3, 3, 4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

"""This is a test to see if there is anything wrong when writing more than 120 
characters maybe thats a problem"""

s = f"this is a test string {2 + 2}" f" this is the second part of the string {3 + 3}"
