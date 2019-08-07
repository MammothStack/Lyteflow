"""Utility function for PipeSystems and PipeElements

This module contains all functions relating to iterating
through all connected PipeElements in a PipeSystem, this 
includes all set Requirements as well.

This module also contains useful functions for creating Reachability Graphs that are
used by the PipeSystem to ensure that the given connected PipeElements are configured
correctly

"""

# Standard library imports
import queue

# Third party imports
import pandas as pd
import numpy as np

# Local application imports


def fetch_pipe_elements(pipesystem, ignore_inlets=False, ignore_outlets=False):
    """Iterates through the PipeSystem and returns a list of all PipeElements

    Arguments
    ------------------
    pipesystem : PipeSystem
        The PipeSystem for which the elements should be gathered for

    ignore_inlets : bool
        If the Inlet PipeElements should be ignored

    ignore_outlets : bool
        If the Outlet PipeElements should be ignored

    Returns
    ------------------
    elements : list
        List of PipeElements that are found in the given PipeSystem

    """

    def traverse(element):
        if element is None or element in elements:
            return

        if not (
            (element in elements)
            or (ignore_inlets and element in pipesystem.inlets)
            or (ignore_outlets and element in pipesystem.outlets)
        ):
            elements.append(element)

        for down in element.downstream:
            traverse(down)

    elements = []
    for i in pipesystem.inlets:
        traverse(i)
    return elements


def column_names_to_formatted_list(list_of_columns):
    """Method for creating two dimensional conform array

    Arguments
    ------------------
    list_of_columns : list / str
        Either a single column name as str which will result in [[str]], or a list of
        str which will result in [[str, str str]].

    Returns
    ------------------
    formatted_list : list
        The formatted list

    Raises
    ------------------
    ValueError
        When the given list cannot be parsed

    """
    if isinstance(list_of_columns, str):
        return [[list_of_columns]]
    elif isinstance(list_of_columns, list):
        if any([isinstance(c, list) for c in list_of_columns]):
            columns = []
            for c in list_of_columns:
                if isinstance(c, str):
                    columns.append([c])
                elif isinstance(c, list):
                    columns.append(c)
                else:
                    raise ValueError(f"{c} cannot be parsed to column")
            return columns
        else:
            return [list_of_columns]
    else:
        raise ValueError(f"{list_of_columns} cannot be parsed")


class PTGraph:
    """Place Transition Graph built from PipeSystem to calculate execution

    The PTGraph class exists solely to calculate the execution sequence of the
    PipeElements in the given PipeSystem. In order to do that it creates a
    place-transition graph which simulates the execution of the PipeSystem. Since the
    PipeSystem's PipeElements can be connected non-sequentially it is possible to have
    multiple execution paths (or none). Calculating a valid execution path is needed
    for the successful execution of the PipeSystem.

    Methods
    ------------------
    get_execution_sequence()
        Gives an execution that is successful in reaching the end state

    Attributes
    ------------------
    W_neg : pd.DataFrame
        Transition matrix of preset nodes

    W_pos : pd.DataFrame
        Transition matrix of postset nodes

    W_t : pd.DataFrame
        Transition matrix of delta nodes when W_pos - W_neg

    """

    def __init__(self, pipesystem):
        """Constructor of PTGraph

        The conversion of the PipeSystem is immediately started in the constructor

        Arguments
        ------------------
        ps : PipeSystem
            The PipeSystem that should be converted into a PTGraph and analyzed for
            execution sequences

        """
        self._M_0 = set()
        self._F = set()
        self._T = set()
        self._P = set()
        self._state = None
        self.W_neg = None
        self.W_pos = None
        self.W_t = None
        self._convert(pipesystem)

    def _convert(self, pipesystem):
        """Converts the given PipeSystem to a PTGraph

        Arguments
        ------------------
        pipesystem : PipeSystem
            The PipeSystem that should be converted

        """

        # Dictionary with _Transitions with PipeElement.id as key
        transitions = {
            pipe_element.id: _Transition(
                pipe_element=pipe_element,
                meta_node=_Node(name=pipe_element.name + "_meta"),
            )
            for pipe_element in pipesystem.all_elements
        }

        # Iterate transitions and create nodes and connect to other nodes
        for transition in transitions.values():

            # Create nodes for every unique upstream element and connect them
            for up in set(transition.pipe_element.upstream):
                n = _Node(
                    name=transitions[up.id].pipe_element.name
                    + "->-"
                    + str(transition.pipe_element.name)
                )
                # Connects the node between the transition of the up element and the
                # current transition
                transitions[up.id].add_to_node(n)
                transition.add_from_node(n)

            # For each requirement connect transition to meta node
            for r in transition.pipe_element.requirements:
                n = transitions[r.pipe_element.id].meta_node
                transition.add_from_node(n)
                transition.add_to_node(n)

        # Create upstream nodes for inlets and designate them as _M_0
        for inlet in pipesystem.inlets:
            n = _Node(name=inlet.name)
            transitions[inlet.id].add_from_node(n)
            self._M_0 = set(list(self._M_0) + [n])

        # Create downstream nodes for outlets and desginate them as _F
        for outlet in pipesystem.outlets:
            n = _Node(name=outlet.name)
            transitions[outlet.id].add_to_node(n)
            self._F = set(list(self._F) + [n])

        # Set the transitions and places and calculate the transitions matrices
        self._T = set(transitions.values())
        self._P = set(np.concatenate([t.from_node + t.to_node for t in self._T]))
        self._set_transition_matrix()

    def _set_transition_matrix(self):
        """Creates the transition matrices

        Creates the W_neg, W_pos, and W_t attributes. The W_neg and W_pos are created
        by creating a matrix that has dimensions (places, transitions). The W_neg
        matrix is then populated by iterating through every transition and setting
        their nodes that lead to them in the table as 1. This occurs for W_pos,
        but nodes that lead from the transition are marked. The W_t matrix is created
        by W_pos - W_neg

        """
        self.W_neg = pd.DataFrame(
            np.zeros((len(self._P), len(self._T))), columns=self._T, index=self._P
        )
        self.W_pos = pd.DataFrame(
            np.zeros((len(self._P), len(self._T))), columns=self._T, index=self._P
        )

        for t in self._T:
            self.W_neg.loc[t.from_node, t] = 1
            self.W_pos.loc[t.to_node, t] = 1

        self.W_t = self.W_pos - self.W_neg

    def _can_transition_execute(self, transition):
        return (self._state.loc[self.W_neg[transition] == 1] == 1).all()

    def _reset_to_starting_configuration(self):
        self._state = pd.Series(np.zeros((len(self._P))), index=self._P)
        self._state.loc[self._M_0] = 1

    def _get_executable_transitions(self):
        return self.W_neg.loc[
            :, self.W_neg.apply(lambda x: self._can_transition_execute(x.name))
        ].columns

    def _execute_transition(self, transition):
        if transition not in self._T:
            raise ValueError(f"{transition} not found in P/T Graph")

        if self._can_transition_execute(transition):
            self._state = self._state + self.W_t.loc[:, transition]
        else:
            raise AttributeError(f"{transition} cannot execute")

    def _calculate_reachability(self):
        # Create last in, first out queue
        q = queue.LifoQueue()
        self._reset_to_starting_configuration()

        # Get the state as a tuple
        data = tuple(self._state.tolist())
        # For every executable transition add the state, transition and history to queue
        for transition in self._get_executable_transitions():
            q.put((data, transition, []))

        while q.qsize() > 0:
            data, transition, hist = q.get()
            self._state = pd.Series(data, index=self._P)
            self._execute_transition(transition)
            hist.append(transition)
            if (self._state.loc[self._F] == 1.0).all():
                return hist

            data = tuple(self._state.tolist())
            executable_transitions = self._get_executable_transitions()
            if len(executable_transitions) == 0:
                return
            else:
                for transition in executable_transitions:
                    q.put((data, transition, hist))

    def get_execution_sequence(self):
        """Calculates and returns a execution that satisfies the end state

        Returns
        ------------------
        execution_sequence : list
            A ordered list of PipeElements in execution order

        Raises
        ------------------
        AttributeError
            When the given PipeSystem does not produce a valid Execution sequence

        """
        transitions = self._calculate_reachability()
        if transitions is None:
            raise AttributeError(
                f"Given PipeSystem does not produce an execution sequence"
            )
        else:
            return [transition.pipe_element for transition in transitions]


class _Node:
    """Class for Place-Transition-Graph Node modelling"""

    def __init__(self, name="Node"):
        self.name = name

    def __repr__(self):
        return f"{self.name}"


class _Transition:
    """Class for Place-Transition-Graph Transition modelling"""

    def __init__(self, pipe_element, meta_node=None):
        self.pipe_element = pipe_element
        self.from_node = []
        self.to_node = []
        self.meta_node = meta_node

        if meta_node is not None:
            self.add_to_node(meta_node)

    def add_from_node(self, *node):
        self.from_node += list(node)

    def add_to_node(self, *node):
        self.to_node += list(node)

    def add_meta_node(self, node):
        if self.meta_node is None:
            self.meta_node = node
            self.add_to_node(node)
        else:
            raise AttributeError("Meta Node is already set")

    def __repr__(self):
        return str(self.pipe_element.name)
