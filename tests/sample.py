from lyteflow.kernels import *
from lyteflow.construct import PipeSystem
from lyteflow.util import PTGraph

import numpy as np

images = np.random.randint(1, 5, (10, 10, 10))
labels = np.random.randint(0, 10, 10)


def make_complex_pipe_system():

    in_1 = Inlet(convert=False, name="in_1")
    sca = Scaler(scalar=1 / 255)(in_1)
    rot = Rotator([90, -90], remove_padding=False, keep_original=True)(sca)
    out_1 = Outlet(name="out_1")(rot)

    in_2 = Inlet(convert=True, name="in_2")
    cat = Categorizer(sparse=True)(in_2)
    dup = Duplicator()(cat)
    con = Concatenator()(dup, dup, dup)
    out_2 = Outlet(name="out_2")(con)

    return PipeSystem(
        inlets=[in_1, in_2], outlets=[out_1, out_2], name="ps", verbose=True
    )


def make_simple_pipesystem():
    in_1 = Inlet(convert=False, name="in_1")
    # sca = Scaler(scalar=1 / 255)(in_1)
    out_1 = Outlet(name="out_1")(in_1)

    in_2 = Inlet(convert=True, name="in_2")
    # cat = Categorizer(sparse=True)(in_2)
    out_2 = Outlet(name="out_2")(in_2)

    return PipeSystem(
        inlets=[in_1, in_2], outlets=[out_1, out_2], name="ps", verbose=True
    )


ps_complex = make_complex_pipe_system()
ps_simple = make_simple_pipesystem()

pt = PTGraph(ps_complex)
