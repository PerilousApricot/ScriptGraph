#!/usr/bin/env python

# unneeded
#import ScriptGraph.Node as Node
#import ScriptGraph.Edge as Edge

class Graph:
	# main graph class, has sweet algorithms
	# is a DAG

	def __init__( self ):
		self.edges = {}
		self.nodes = {}

	def getEdges( self ):
		return self.edges
	
	def getNodes( self ):
		return self.nodes
	
	def getEdge( self, name ):
		return self.edges[name]
	def getNode( self, name ):
		return self.nodes[name]

	def addNode( self, newNode ):
		# todo: add checks for uniqueness
		self.nodes[ newNode.getName() ] = newNode
	
	def addEdge( self, parentNode, childNode, newEdge ):
		# todo: add checks for uniqueness
		# todo: check that there are no cycles
		newEdge.setParent( parentNode )
		newEdge.setChild ( childNode  )
		parentNode.addOutput( newEdge )
		childNode.addInput( newEdge )
		self.edges[ newEdge.getName() ] = newEdge
	
	def getOutputs( self ):
		# returns all the nodes without outbound edges
		# these should (theoretically) be our final products
		retval = []
		for node in self.nodes:
			if ( node.getChildren() == [] ):
				retval.append( node )
		return retval
	
	def getInputs( self ):
		# returns all the nodes without outbound edges
		# these should (theoretically) be our final products
		retval = []
		for node in self.nodes:
			if ( node.getParents() == [] ):
				retval.append( node )
		return retval

