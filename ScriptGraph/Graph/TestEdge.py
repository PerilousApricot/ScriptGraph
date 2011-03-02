import ScriptGraph.Graph.Edge as BaseEdge
import os.path

class ReadyEdgeBase( BaseEdge.Edge ):
	def getOutputFiles( self ):
		return [ os.path.join(self.getName(), "file1"),
				os.path.join(self.getName(), "file2"),
				os.path.join(self.getName(), "file3")
		]

class NeverReadyEdge( ReadyEdgeBase ):
	def checkStatusImpl( self ):
		self.setIncomplete()
	def executeImpl( self, inputFileList, inputFileMap ):
		print "NeverReadyEdgeExecuted"
		self.isExecuted = True
		
class AlwaysReadyEdge( ReadyEdgeBase ):
	def checkStatusImpl( self ):
		self.setComplete()
	def executeImpl( self ):
		raise RuntimeError, "This edge is already complete, should not have been run"
