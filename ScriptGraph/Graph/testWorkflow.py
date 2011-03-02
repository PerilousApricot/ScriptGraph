#!/usr/bin/env python

import ScriptGraph.Graph.Graph as Graph
import ScriptGraph.Graph.Node as NodeModule
Node = NodeModule.Node
import ScriptGraph.Graph.LocalScriptEdge as LocalScriptEdge
import ScriptGraph.Graph.CrabEdge as CrabEdge
from ScriptGraph.Helpers.BindPreviousOutput import BindPreviousOutput

g = Graph.Graph()
g.setWorkDir("/uscms_data/d2/meloam/testWorkflow")

#nodes
nullInput = Node(name = "nullInput")
fileList  = Node(name = "fileList")
treeFiles = Node(name = "treeFile")
s8Input   = Node(name = "s8_input")
s8Monitor = Node(name = "s8_monitor")
s8Solver  = Node(name = "s8_solve")



datasetSearch = LocalScriptEdge.LocalScriptEdge(name = "dbsQuery", command = "dbs search --query='find file where dataset=/BTau/Run2010A-Dec22ReReco_v1/AOD' | tail -n +5 > input.txt", output="input.txt", noEmptyFiles=True)
inputTxtBind = BindPreviousOutput( )

produceTree = CrabEdge.CrabEdge(name = "generateTree", crabConfig = "/uscms_data/d2/meloam/testWorkflow/crab.cfg", 
						cmsswConfig = "/uscms_data/d2/meloam/testWorkflow/cmssw_data.py", 
						cmsswReplacements = [ ["!!!INPUT FILE LIST!!!", inputTxtBind ] ] )

g.addNode( nullInput )
g.addNode( fileList  )
g.addNode( treeFiles )

g.addEdge( nullInput, fileList, datasetSearch )
g.addEdge( fileList,  treeFiles, produceTree  )

g.checkStatus()
g.dumpInfo()
g.pushGraph()
