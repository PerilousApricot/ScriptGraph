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
		if isinstance( self.command, type([]) ):
			commandReplaced = []
			for onearg in self.command:
				commandReplaced.append( str( self.lateBind( onearg ) ) )
			if self.addFileNames:
				commandReplaced.extend( inputFiles )

			print "Executing: %s" % commandReplaced
			output = commands.getstatusoutput( " ".join(commandReplaced) )
		elif isinstance( self.command, LateBind ):
			commandReplaced = self.lateBind( self.command )
			if self.addFileNames:
				commandReplaced = commandReplaced + ' ' + ' '.join( inputFiles )
			print "Executing: %s" % commandReplaced
			output = commands.getstatusoutput( commandReplaced )
		else:
			commandReplaced = self.command
			if self.addFileNames:
				commandReplaced = commandReplaced + ' ' + ' '.join( inputFiles )
			print "Executing: %s" % commandReplaced
			output = commands.getstatusoutput( commandReplaced )
		
		print output[1]

		if self.noEmptyFiles:
			if not os.path.exists( self.output ):
				raise RuntimeError, "Output from script wasn't found"
			if os.path.getsize( self.output ) == 0:
				raise RuntimeError, "Output from script had zero bytes"
			
