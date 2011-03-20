#
# accepts input edges, makes a collect node and output node for a s8monitor edge
#
from ScriptGraph.Helpers.LateBind import LateBind
import os.path
import tempfile
import re
class BindFileList( LateBind ):
	def __init__(self,name,filePattern = None):
		self.fileName = name
		self.filePattern = filePattern
	def bind( self, edge ):
		node  = edge.getParent()
		files,_ = node.getFiles()
		handle = open(self.fileName, 'w')
		if self.filePattern:
			pattern = re.compile( self.filePattern )
		else:
			pattern = re.compile( ".*" )
		for file in files:
			if not os.path.isabs(file):
				file = os.path.normpath(os.path.join( edge.getWorkDir(), file ) )
			if pattern.search( file ):
				handle.write( file + "\n" )
		handle.close()
		if not os.path.isabs( self.fileName ):
			return os.path.abspath( self.fileName )
		else:
			return self.fileName
	
