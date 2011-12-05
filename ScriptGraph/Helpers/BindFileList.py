#
# accepts input edges, makes a collect node and output node for a s8monitor edge
#
from ScriptGraph.Helpers.LateBind import LateBind
import os.path
import tempfile
import re
class BindFileList( LateBind ):
    def __init__(self,name,filePattern = None, relative = False, offset = ""):
        self.fileName = name
        self.filePattern = filePattern
        self.relative = relative
        self.offset = offset

    def bind( self, edge ):
        node  = edge.getParent()
        print "parent name %s" % node.getName()
        files,_ = node.getFiles()
        print "parent files %s" % files
        handle = open(self.fileName, 'w')
        if self.filePattern:
            pattern = re.compile( self.filePattern )
        else:
            pattern = re.compile( ".*" )

        foundFile = False
        for file in files:
            if not os.path.isabs(file):
                file = os.path.normpath(os.path.join( edge.getWorkDir(), file ) )
            if pattern.search( file ):
                foundFile = True
                if self.relative:
                    file = os.path.basename(file)
                handle.write( file + "\n" )
        if not foundFile:
            raise RuntimeError, "No files found: %s %s" % (edge.getName(), edge)

        handle.close()
        if not os.path.isabs( self.fileName ):
            return os.path.abspath( self.fileName )
        else:
            return self.fileName
    
