"""Utility function for PipeSystems and PipeElements

This module contains all functions relating to iterating
through all connected PipeElements in a PipeSystem, this 
includes all set Requirements as well.

This module also contains useful functions for creating
Reachability Graphs that are used by the PipeSystem to
ensure that the given connected PipeElements are configured
correctly

"""

# Standard library imports
import queue

# Third party imports
import pandas as pd
import numpy as np

# Local application imports
from lyteflow.kernels.base import PipeElement, Requirement


def fetch_pipe_elements(pipesystem, ignore_inlets=False, ignore_outlets=False):
    def traverse(element):
        if element is None:
            return

        if element not in elements:
            if ignore_inlets and element in pipesystem.inlets:
                pass
            elif ignore_outlets and element in pipesystem.outlets:
                pass
            else:
                elements.append(element)

        for down in element.downstream:
            traverse(down)

    elements = []
    for i in pipesystem.inlets:
        traverse(i)
    return elements


def connect_pipe_elements(elements):
    for e in elements:
        if len(e.downstream) == 0 and len(e.upstream) == 0:
            raise AttributeError(
                """All given elements need to have the 
                correct id numbers as the inlets and outlets"""
            )

    _all = {e.id: e for e in list(elements)}
    for e in list(elements):
        for down_id in e.downstream:
            try:
                e.attach_downstream(_all[down_id])
            except TypeError:
                pass

        for up_id in e.upstream:
            try:
                e.attach_upstream(_all[up_id])
            except TypeError:
                pass

        e.configure_requirements(*elements)
        e.validate_stream()


class PTGraph:
    def __init__(self, ps):
        self.M_0 = set()
        self.F = set()
        self.T = set()
        self.P = set()
        self.state = None
        self.W_neg = None
        self.W_pos = None
        self.W_t = None
        self._convert(ps)

    def _convert(self, ps):
        # Get all pipe elements
        all_elements = fetch_pipe_elements(ps)

        # Dictionary with _Transitions with PipeElement.id as key
        transitions = {
            pipe_element.id: _Transition(
                pipe_element=pipe_element,
                meta_node=_Node(name=pipe_element.name + "_meta"),
            )
            for pipe_element in all_elements
        }

        for transition in transitions.values():
            for up in set(transition.pipe_element.upstream):
                n = _Node(
                    name=transitions[up.id].pipe_element.name
                    + "->-"
                    + str(transition.pipe_element.name)
                )
                transitions[up.id].add_to_node(n)
                transition.add_from_node(n)

            # For each requirement connect transition to meta node
            for r in transition.pipe_element.requirements:
                n = transitions[r.pipe_element.id].meta_node
                transition.add_from_node(n)
                transition.add_to_node(n)

        # Create upstream nodes for inlets and designate them as M_0
        for inlet in ps.inlets:
            n = _Node(name=inlet.name, marked=True)
            transitions[inlet.id].add_from_node(n)
            self.M_0 = set(list(self.M_0) + [n])

        for outlet in ps.outlets:
            n = _Node(name=outlet.name)
            transitions[outlet.id].add_to_node(n)
            self.F = set(list(self.F) + [n])

        self.T = set(transitions.values())
        self.P = set(np.concatenate([t.from_node + t.to_node for t in self.T]))
        self._set_transition_matrix()
        # self.reset_graph()

    def _set_transition_matrix(self):
        self.W_neg = pd.DataFrame(
            np.zeros((len(self.P), len(self.T))), columns=self.T, index=self.P
        )
        self.W_pos = pd.DataFrame(
            np.zeros((len(self.P), len(self.T))), columns=self.T, index=self.P
        )

        for t in self.T:
            self.W_neg.loc[t.from_node, t] = 1
            self.W_pos.loc[t.to_node, t] = 1

        self.W_t = self.W_pos - self.W_neg

    def _can_transition_execute(self, transition):
        return (self.state.loc[self.W_neg[transition] == 1] == 1).all()

    def reset_to_starting_configuration(self):
        self.state = pd.Series(np.zeros((len(self.P))), index=self.P)
        self.state.loc[self.M_0] = 1

    def get_executable_transitions(self):
        return self.W_neg.loc[
            :, self.W_neg.apply(lambda x: self._can_transition_execute(x.name))
        ].columns

    def execute_transition(self, transition):
        if transition not in self.T:
            raise ValueError(f"{transition} not found in P/T Graph")

        if self._can_transition_execute(transition):
            self.state = self.state + self.W_t.loc[:, transition]
        else:
            raise AttributeError(f"{transition} cannot execute")

    def set_state(self, data):
        self.state = pd.Series(data, index=self.P)

    def get_hashable_state(self):
        return tuple(self.state.tolist())

    def end_state(self):
        return (self.state.loc[self.F] == 1.0).all()


class ReachabilityGraph:
    def __init__(self, pt_graph):
        self.pt_graph = pt_graph

    def get_execution_sequence(self):
        transitions = self._calculate_reachability()
        return [transition.pipe_element for transition in transitions]

    def _calculate_reachability(self):
        q = queue.LifoQueue()
        self.pt_graph.reset_to_starting_configuration()
        data = self.pt_graph.get_hashable_state()
        for transition in self.pt_graph.get_executable_transitions():
            # print(transition)
            q.put((data, transition, []))

        while q.qsize() > 0:
            data, transition, hist = q.get()
            self.pt_graph.set_state(data)
            self.pt_graph.execute_transition(transition)
            hist.append(transition)
            if self.pt_graph.end_state():
                return hist

            data = self.pt_graph.get_hashable_state()
            for transition in self.pt_graph.get_executable_transitions():
                q.put((data, transition, hist))


class _Node:
    def __init__(self, marked=False, name="Node"):
        self.marked = marked
        self.name = name

    def __repr__(self):
        return f"{self.name}"


class _Transition:
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
