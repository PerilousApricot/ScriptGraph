#!/usr/bin/python

import os, os.path, re, sys
import ScriptGraph.Graph.Graph as Graph
from ScriptGraph.Graph.Node import Node
from ScriptGraph.Graph.LocalScriptEdge import LocalScriptEdge
from ScriptGraph.Helpers.BindFunction import BindFunction
from ScriptGraph.Helpers.BindSubstitutes import BindSubstitutes
from ScriptGraph.Helpers.BindPreviousOutput import BindPreviousOutput
from ScriptGraph.Graph.NullEdge import NullEdge

sys.path.append( os.path.dirname( os.path.abspath( __file__ )  ))
import 39x
from s8.MonitorInput import run_monitor_input_helper
from s8.RootQCD import merge_with_root_qcd_helper
from s8.Hadd import hadd_helper

from ScriptGraph.Helpers.Miter import Miter

g = Graph.Graph()
singleMonitorMiter = Miter()
baseWorkDir = "/uscms_data/d2/meloam/input39x2"
g.setWorkDir( baseWorkDir )
treeGraph = genTree.getGraph()
exports   = genTree.getDatasets()

g.addGraph( treeGraph )

#
# Merge triggers for the B range
#
dataForWeights = {}
haddedDataMiterBRange = Miter()
dataForPlots = Miter()
for opoint in operating_points:
    for bin in jet_bins:  
        currNode = hadd_helper( g, "-%s-%s-data-brange" % (bin[0],opoint), 
                                        skiplessDataMiter.getValues( bin=bin[0],
                                        opoint = opoint,
                                        dataset = 'RUN2010B' ) )
        haddedDataMiterBRange.add(
                            currNode,
                            opoint = opoint,
                            bin = bin[0] )
        print "the opoint is %s" % opoint
        singleMonitorMiter.add( currNode,\
                                                bin     = bin[0],
                                                opoint  = opoint,
                                                njetpt  = True,
                                                njeteta = True,
                                                privert = True,
                                                type    = "hadd_data" )
        dataForWeights[ bin[0] ] = currNode
        if opoint == "TCHEM":
            dataForPlots.add( currNode, bin = bin[0] )
#
# Merge Bins
#
haddedDataFinalMiter = Miter()
for opoint in haddedDataMiter.iterGrouped( 'opoint' ):
    currNode = hadd_helper( g, "-%s-data-final" % (opoint.vals[0][1]['opoint'],), 
                                skiplessDataMiter.getValues( opoint = opoint.vals[0][1]['opoint']) )
    haddedDataFinalMiter.add(
                        currNode,
                        opoint = opoint.vals[0][1]['opoint'])

    singleMonitorMiter.add( currNode,\
                                            opoint  = opoint.vals[0][1]['opoint'],
                                            njetpt  = True,
                                            njeteta = True,
                                            privert = True,
                                            type    = "hadd_data-final" )



