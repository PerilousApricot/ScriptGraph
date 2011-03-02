#!/usr/bin/env python

class LateBind:
	def bind( self, edge ):
		raise NotImplementedError, "LateBind subclasses must define the bind method"
