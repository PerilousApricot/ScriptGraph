#
# accepts input edges, makes a collect node and output node for a s8monitor edge
#
from ScriptGraph.Helpers.LateBind import LateBind
import os.path
import tempfile
class BindFileList( LateBind ):
	def __init__(self,name,filePattern = None):
		self.fileName = name
		self.filePattern = filePattern
	def bind( self, edge ):
		node  = edge.getParent()
		files,_ = node.getFiles()
		print "My parent node is %s" % node.getName()
		print "GOT THESE FILES %s" % files
		handle = open(self.fileName, 'w')
		for file in files:
			if not os.path.isabs(file):
				file = os.path.normpath(os.path.join( edge.getWorkDir(), file ) )
			handle.write( file + "\n" )
		handle.close()
		if not os.path.isabs( self.fileName ):
			return os.path.abspath( self.fileName )
		else:
			return self.fileName
	
