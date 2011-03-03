#!/usr/local/bin python

class Node:
	def __init__( self, name = "" ):
		self.parentEdges = {}
		self.childEdges  = {}
		if not name:
			name = "UNSET"
		self.name = name

	def setName( self, name ):
		if ( self.name != "UNSET" ):
			raise RuntimeError, "Cannot rename nodes"
		self.name = name

	def getName( self ):
		if ( self.name == "UNSET"):
			raise RuntimeError, "Name was not set in node"

		return self.name

	def addInput( self, inputEdge ):
		self.parentEdges[ inputEdge.getName() ] = inputEdge
	
	def addOutput( self, outputEdge ):
		self.childEdges[ outputEdge.getName() ] = outputEdge
	
	def getChildren( self ):
		return self.childEdges.values()

	def getParents( self ):
		return self.parentEdges.values()
	
	def __str__( self ):
		print "Getting str"
		return "< %s>" % ( self.getName())
