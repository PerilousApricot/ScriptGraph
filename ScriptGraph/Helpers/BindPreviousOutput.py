#!/usr/bin/env python

import ScriptGraph.Helpers.LateBind as LateBind

class BindPreviousOutput( LateBind.LateBind ):

	def bind( self, edge ):
		files,_ = edge.getParent().getFiles()
		if ( len(files) != 1 ):
			raise RuntimeError, "Too many/few files in previous node named %s" % edge.getParent().getName()
		return files[0]
