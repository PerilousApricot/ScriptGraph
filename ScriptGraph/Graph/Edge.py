import ScriptGraph.BaseGraph.Edge as BaseEdge
import os.path
import shutil
import ScriptGraph.Helpers.LateBind as LateBind
class Edge( BaseEdge.Edge ):

		
	def __init__( self, name="" ):
		self.statusCache = ""
		self.completeStatus = "COMPLETE"
		self.incompleteStatus = "INCOMPLETE"
		self.runningStatus = "RUNNING"
		self.failStatus = "FAILED"
		BaseEdge.Edge.__init__( self, name )
		self.workDir = ""
		self.inputFiles = []
		self.outputFiles = []
	
	def isRunning( self ):
		if not self.statusCache:
			self.checkStatus()
		return (self.statusCache == self.runningStatus)

	def isIncomplete( self ):
		if not self.statusCache:
			self.checkStatus()
		return (self.statusCache == self.incompleteStatus)

	def isComplete( self ):
		if not self.statusCache:
			self.checkStatus()
		return (self.statusCache == self.completeStatus)



	def getStatus( self ):
		return self.statusCache
	
	def lateBind( self, bindobj ):
		if (isinstance( bindobj, LateBind.LateBind ) ):
			return bindobj.bind( self )
		else:
			return bindobj

	def setWorkDir( self, dir ):
		if ( self.workDir ):
			print "Note tried to replace workdir for edge %s with %s previous %s\nThis is disabled by default" % (self.getName(), dir, self.workDir)
			raise
			return
		self.workDir = dir
	
	def makeWorkDir( self ):
		if not os.path.exists( self.workDir ):
			os.makedirs( self.workDir )
	
	def getWorkDir( self ):
		if ( not self.workDir ):
			raise RuntimeException, "Workdir is unset"
		return self.workDir


	def isComplete( self ):
		# have we run and gotten output back?
		if self.statusCache == "":
			self.checkStatus()
		#return (self.statusCache == self.completeStatus)
		return (self.parent.isReady() ) and (self.statusCache == self.completeStatus)
	
	def isFail( self ):
		# have we run and gotten output back?
		if self.statusCache == "":
			self.checkStatus()
		#return (self.statusCache == self.completeStatus)
		#return (self.parent.isReady() ) and (self.statusCache == self.completeStatus)
		return ( self.statusCache == self.failStatus )
	def canRun( self ):
		if self.statusCache == "":
			self.checkStatus()
		return (self.parent.isReady() ) and (self.statusCache == self.incompleteStatus)	

	def setRunning( self ):
		self.statusCache = self.runningStatus

	def setFailed( self ):
		self.statusCache = self.failStatus

	def setComplete( self ):
		self.statusCache = self.completeStatus

	def setIncomplete( self ):
		self.statusCache = self.incompleteStatus

	def checkStatusImpl( self ):
		raise NotImplementedError, self
	
	def checkStatus( self ):
#		print "    Checking status of edge: %s" % self.getName()
		if self.statusCache:
			return
		oldcwd = os.getcwd()
		os.chdir( self.workDir )
		self.checkStatusImpl()
		os.chdir( oldcwd )
		if not self.statusCache:
			raise RuntimeError, "Status was checked, but the cache wasn't updated"

	def clearStatus( self ):
		self.statusCache = ""
	
	def execute( self, inputFiles, inputFileMap ):
		self.copyInputFiles()
		oldcwd = os.getcwd()
		os.chdir( self.workDir )
		print "edge.executing"
		self.executeImpl( inputFiles, inputFileMap )
		os.chdir( oldcwd )

	def executeImpl( self, inputFiles, inputFileMap ):
		raise NotImplementedError

	def copyInputFiles( self ):
		if getattr( self, "inputFiles", [] ):
			for file in self.inputFiles:
				print "copying file %s in edge %s" % ( file, self.getName() )
				shutil.copyfile( file, os.path.join( self.workDir, os.path.basename( file ) ) )
	
	def addInputFile( self, filename ):
		self.inputFiles.append( filename )

	def addOutputFile( self, filename ):
		self.outputFiles.append( filename )
	
	def getOutputFiles( self ):
		return self.outputFiles
	
	def clearOutputFiles( self ):
		self.outputFiles = []

