#!/usr/bin/env python
import os, os.path, re, sys
import ScriptGraph.Graph.Graph as Graph
sys.path.append( os.path.dirname( os.path.abspath( __file__ )  ))
import genTree

g = Graph.Graph()
baseWorkDir = "/uscms_data/d2/meloam/dijetexercise"
g.setWorkDir( baseWorkDir )

treeGraph = genTree.getGraph()

g.addGraph( treeGraph )

def getGraph():
    global g
    return g
