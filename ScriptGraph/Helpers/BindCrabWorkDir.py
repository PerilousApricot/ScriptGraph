from ScriptGraph.Helpers.LateBind import LateBind
import os.path

class BindCrabWorkDir( LateBind ):
	def __init__( self, crabEdge, suffix = None ):
		self.crabEdge = crabEdge
		self.suffix   = suffix

	def bind( self, edge ):
		if self.suffix:
			return os.path.join( self.crabEdge.crabWorkDir, self.suffix )
		else:
			return self.crabEdge.crabWorkDir

