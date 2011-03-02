#!/usr/bin/env python

import ScriptGraph.Graph.Edge as Edge
import ScriptGraph.Helpers.LateBind as LateBind
import os.path
import re
import subprocess
import os
import sys
import commands

nullCounter = 0
class NullEdge( Edge.Edge ):
	
	def __init__( self, name = "" ):
		global nullCounter
		if not name:
			Edge.Edge.__init__(self, name = "nullEdge%s" % nullCounter )
			nullCounter += 1
		Edge.Edge.__init__(self)

	def executeImpl( self, fileList, fileMapList ):
		pass
	
	def checkStatusImpl( self ):
		parentFiles,_ = self.getParent().getFiles()
		self.clearOutputFiles()
		for file in parentFiles:
			self.addOutputFile( file )
		
		if self.getParent().isReady():
			self.setComplete()
		else:
			self.setIncomplete()
