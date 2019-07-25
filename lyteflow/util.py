"""Utility function for PipeSystems and PipeElements

This module contains all functions relating to iterating
through all connected PipeElements in a PipeSystem, this 
includes all set Requirments as well.

This module also contains useful functions for creating
Reachability Graphs that are used by the PipeSytem to
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

NODE_CREATION_COUNTER = 0

def fetch_pipe_elements(pipesystem, ignore_inlets=False, ignore_outlets=False):
    def traverse(element):
        if element is None:
            return
            
        if not element in elements:
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
            
    all = {e.id: e for e in list(elements)}
    for e in list(elements):
        for down in e.downstream:
            try:
                e.attach_downstream(all[down])
            except TypeError:
                pass
                
        for up in e.upstream:
            try:
                e.attach_upstream(all[up])
            except TypeError:
                pass
                
        requirements_actual = []
        for r in e.requirements:
            requirements_actual.append(Requirement.from_config(r, all[r.pipe_element]))
            
        e.requirements = set(requirements_actual)
        
        e.validate_stream()
    
class PTGraph:
    def __init__(self, ps):
        #self.ps = ps
        self.convert(ps)
        
    def convert(self, ps):
        self.M_0 = set()
        self.F = set()
        self.T = set()
        self.P = []
        
        # Get all pipe elements
        all_elements = fetch_pipe_elements(ps)
                
        # Create transition relations for each pipe element
        petr = {e.id: _PipeElementTransitionRelation(e) for e in all_elements}
        
        for pt in petr.values():
            # For each upstream element create a singular node
            for up in pt.pipe_element.upstream:
                if up is not None:
                    n = _Node(name=str(pt.pipe_element) + "-<-" + str(petr[up.id].pipe_element))
                    petr[up.id].transition.add_to_node(n)
                    pt.transition.add_from_node(n)
            
            # For each downstream element create singular node
            #for down in pt.pipe_element.downstream:
            #    if down is not None:
            #        n = _Node()
            #        petr[down.id].transition.add_from_node(n)
            #        pt.transition.add_to_node(n)
            
            # For each requirement connect transition to meta node
            for r in pt.pipe_element.requirements:
                n = petr[r.pipe_element.id].meta_node
                pt.transition.add_from_node(n)
                pt.transition.add_to_node(n)
                
        # Create upstream nodes for inlets and designate them as M_0
        for inlet in ps.inlets:
            n = _Node(name=str(inlet), marked=True)
            petr[inlet.id].transition.add_from_node(n)
            self.M_0 = set(list(self.M_0) + [n])
            
        for outlet in ps.outlets:
            n = _Node(name=str(outlet))
            petr[outlet.id].transition.add_to_node(n)
            self.F = set(list(self.F) + [n])
            
        self.T = set([p.transition for p in petr.values()])
        
        for pt in petr.values():
            for n in pt.transition.from_node + pt.transition.to_node:
                self.P.append(n)
        
        self.P = set(self.P)
        
        self.W_neg = pd.DataFrame(columns=self.T, index=self.P)
        self.W_pos = pd.DataFrame(columns=self.T, index=self.P)
        
        for t in self.T:
            self.W_neg.loc[t.from_node[0], t] = 1
            self.W_pos.loc[t.to_node[0], t] = 1
        
        self.W_t = self.W_pos - self.W_neg
        self.reset_graph()
        
    def reset_graph(self):
        self.state = pd.Series(np.zeros((len(self.P))), index=self.P)
        
    def remove_all_marks(self):
        self.reset_graph()
            
    def mark_nodes(self, *nodes):
        for node in nodes:
            if not node in self.P:
                raise ValueError(f"{node} is not found in the P/T Graph")
        
        self.state.loc[nodes] = 1
        
    def get_marked_nodes(self):
        return list(self.state.loc[self.state > 0].index())
        
    def can_transition_execute(self, transition):
        if not transition in self.T:
            raise ValueError(f"{transition} not found in P/T Graph")
        b_arr = self.W_neg.loc[:, transition] > 0
        return self.W_neg.loc[b_arr] == self.state.loc[b_arr]
            
    def get_executable_transitions(self):
        executable_transitions = []
        for transition in self.T:
            if can_transition_execute(transition):
                executable_transitions.append(transition)
        return executable_transitions
        
    def execute_transition(self, transition):
        if not transition in self.T:
            raise ValueError(f"{transition} not found in P/T Graph")
        
        if can_transition_execute(transition):
            self.state = self.state - self.W_t[transition]
        else:
            raise AttributeError(f"{transition} cannot execute")
            
    def set_state(self, data):
        self.state = pd.Series(data, index=self.P)
        
    def end_state(self, strict=True):
        if strict:
            rest = self.state.index.difference(self.F)
            return (self.state.loc[self.F] == 1.0).all() and (self.state.loc[rest] == 0.0).all()
        else:
            return (self.state.loc[self.F] == 1.0).all()
        
class ReachabilityGraph:
    def __init__(self, pt_graph):
        self.pt_graph = pt_graph
        
    def calculate_reachability(self):
        can_execute = True
        
        q = queue.Queue()
        succesful_execution_sequences = []
        
        nodes = self.pt_graph.M_0
        self.pt_graph.remove_all_marks()
        self.pt_graph.mark_nodes(*nodes)
        data = tuple(self.pt_graph.state.tolist())
        for transition in self.pt_graph.get_executable_transitions():
            q.put((data, transition, []))
        
        while q.qsize() > 0:
            data, transition, hist = q.get()
            
            self.pt_graph.set_state(data)
            self.execute_transition(transition)
            hist.append(transition)
            if self.pt_graph.end_state():
                succesful_execution_sequences.append(hist)
            
            data = tuple(self.pt_graph.state.tolist())
            for transition in self.pt_graph.get_executable_transitions():
                q.put((data, transition, hist))
                
        return succesful_execution_sequences
    
class _Node:
    def __init__(self, marked=False, name=None, count=None):
        global NODE_CREATION_COUNTER
        self.marked = marked
        if count is None:
            count = str(NODE_CREATION_COUNTER)
            NODE_CREATION_COUNTER += 1
        if name is None:
            name = "Node"
            
        self.name = name + "_" + count
        
    def __repr__(self):
        return f"{self.name}"

        
class _Transition:
    def __init__(self):
        self.from_node = []
        self.to_node = []
        
    def add_from_node(self, *node):
        self.from_node += list(node)
        
    def add_to_node(self, *node):
        self.to_node += list(node)

        
class _PipeElementTransitionRelation:
    def __init__(self, pipe_element):
        self.pipe_element = pipe_element
        self.transition = _Transition()
        self.meta_node = _Node(name=str(pipe_element) + "_meta_node")
        self.transition.add_to_node(self.meta_node)

        
    
