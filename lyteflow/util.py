"""Utility function for PipeSystems and PipeElements

This module contains all functions relating to iterating
through all connected PipeElements in a PipeSystem, this 
includes all set Requirments as well.

This module also contains useful functions for creating
Reachability Graphs that are used by the PipeSytem to
ensure that the given connected PipeElements are configured
correctly

"""

def fetch_pipe_elements(pipesystem):
    elements = []
    
    def traverse(element):
        if element is None:
            return
            
        if isinstance(element, PipeElement):
            if element in elements:
                pass
            else:
                elements.append(element)
                
        if isinstance(element.downstream, list):
            down = element.downstream
        else:
            down = [element.downstream]
        
        for d in down:
            traverse(down)
            
    return elements
    
class ReachabilityGraph:
    def __init__(self, ps=None):
        if ps is not None:
            self.graph = self.convert(ps)
    def convert(ps):
        all_elements = fetch_pipe_elements(ps)
        petr = {e: PipeElementTransitionRelation(e) for e in all_elements}
        down_up_pair = {set(())}
        
        
class Node:
    def __init__(self, marked=False, count=0):
        self.marked = marked
        self.name = "Node_" + str(count)
        
    def __repr__(self):
        return f"{self.name}: marked={self.marked}"

        
class Transition:
    def __init__(self):
        self.from_node = []
        self.to_node = []
        
    def add_from_node(self, *node):
        self.from_node += list(node)
        
    def add_to_node(self, *node):
        self.to_node += list(node)
        
    def can_fire(self):
        for f in self.from_node:
            if not f.marked:
                return False
        return True
        
class PipeElementTransitionRelation:
    def __init__(self, pipe_element):
        self.pipe_element = pipe_element
        self.transition = Transition()
        self.meta_node = Node()
        self.transition.add_to_node(self.meta_node)
