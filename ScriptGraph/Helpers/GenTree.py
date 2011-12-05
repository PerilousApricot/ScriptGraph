from ScriptGraph.Graph.Node import Node
from ScriptGraph.Graph.CrabEdge import CrabEdge
import ScriptGraph.Graph.LocalScriptEdge as LocalScriptEdge

def genTree( g=None,input=None, nodeName=None, 
			 edgeName=None, crabCfg=None, 
			 cmsswCfg=None, dataset=None,
			 cmsswReplacements = None,
			 crabReplacements = None, 
             overridepath = None,
             overridefiles = None):
    # if we're overriding things, escape
    if overridepath:
        treeNode = Node( name = nodeName )
        produceTree = LocalScriptEdge.LocalScriptEdge(
                            command = "echo SKIPPING CRAB",
                            output = overridefiles,
                            name = edgeName,
                            noEmptyFiles = True )
        produceTree.crabWorkDir = overridepath
        produceTree.setWorkDir( overridepath )
        g.addNode( treeNode )
        g.addEdge( input, treeNode, produceTree )
        return treeNode, produceTree

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
