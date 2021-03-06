#!/usr/bin/env python

import ScriptGraph.Graph.Edge as Edge
import os.path
import os
import commands
import types
from ScriptGraph.Helpers.LateBind import LateBind
class LocalScriptEdge( Edge.Edge ):
	def __init__( self, command, output,
						noEmptyFiles = False, 
						name = "",
						addFileNamesToCommandLine=False):
		self.command = command
		self.output  = output
		self.noEmptyFiles = noEmptyFiles
		self.addFileNames = addFileNamesToCommandLine
		Edge.Edge.__init__( self, name )
	
	def checkStatusImpl( self ):
		self.clearOutputFiles()
		targetPath = os.path.join( self.workDir, self.output ) 
		if ( os.path.exists( targetPath )
		        and not ( self.noEmptyFiles and ( os.path.getsize( targetPath ) == 0 ) ) ):
			self.addOutputFile( targetPath )
			self.setComplete()
			return
		elif os.path.exists( targetPath ) and ( self.noEmptyFiles and ( os.path.getsize( targetPath ) == 0 ) ):
			print "Edge %s was set to not have zero-length files but the output was zero length" % self.getName()
			self.setFailed()
			return
		else:
			self.setIncomplete()
	
	def executeImpl( self, inputFiles, inputMap ):
		print "%s" % LateBind
		print "%s" % type( self.command )
		print "%s" % self.command
		if isinstance( self.command, type([]) ):
			commandReplaced = []
			for onearg in self.command:
				commandReplaced.append( str( self.lateBind( onearg ) ) )
			if self.addFileNames:
				commandReplaced.extend( inputFiles )
			commandReplaced = " ".join(commandReplaced)
		elif issubclass( self.command.__class__ , LateBind ):
			print "LATE BIND CALLED"
			commandReplaced = self.lateBind( self.command )
			if isinstance( commandReplaced, type([]) ):
				commandReplaced2 = []
				for onearg in commandReplaced:
					commandReplaced2.append( str( self.lateBind( onearg ) ) )
				print "replace2 = %s" % commandReplaced2
				commandReplaced = " ".join(commandReplaced2)
			if self.addFileNames:
				commandReplaced = commandReplaced + ' ' + ' '.join( inputFiles )
		else:
			commandReplaced = self.command
			if self.addFileNames:
				commandReplaced = commandReplaced + ' ' + ' '.join( inputFiles )
		
		print "Executing: %s" % commandReplaced
		try:
			output = commands.getstatusoutput( commandReplaced )
		except Exception as e:
			print "Error in executing command %s" % commandReplaced
			raise e

		print output[1]

		if self.noEmptyFiles:
			if not os.path.exists( self.output ):
				raise RuntimeError, "Output from script wasn't found (%s/%s)" % (self.workDir, self.output)
			if os.path.getsize( self.output ) == 0:
				raise RuntimeError, "Output from script had zero bytes"
			
