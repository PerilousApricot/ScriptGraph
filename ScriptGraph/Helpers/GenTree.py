from ScriptGraph.Graph.Node import Node
from ScriptGraph.Graph.CrabEdge import CrabEdge

def genTree( g=None,input=None, nodeName=None, 
			 edgeName=None, crabCfg=None, 
			 cmsswCfg=None, dataset=None,
			 cmsswReplacements = None,
			 crabReplacements = None ):
	#
	# Generate the data
	#
	if not cmsswReplacements:
		cmsswReplacements = []
	if not crabReplacements:
		crabReplacements = []
	
	crabReplacements.append( [ "INPUTDATASET", dataset ] )
	treeNode = Node( name = nodeName )
	produceTree = CrabEdge( \
						name = edgeName, crabConfig = crabCfg, 
						cmsswConfig = cmsswCfg, 
						crabReplacements  = crabReplacements,
						cmsswReplacements = cmsswReplacements )
	# add our current object to the tree
	g.addNode( treeNode )
	g.addEdge( input, treeNode, produceTree  )
	return treeNode, produceTree


