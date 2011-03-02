from ScriptGraph.Helpers.LateBind import LateBind
class BindSubstitutes( LateBind ):
	def __init__( self, formatString, bindList ):
		self.formatString = formatString
		self.bindList     = bindList

	def bind( self, edge ):
		bindStrs = []
		for bind in self.bindList:
			bindStrs.append( bind.bind( edge ) )
		print "binding >%s< with %s" % (self.formatString, bindStrs)
		return self.formatString % tuple(bindStrs)


