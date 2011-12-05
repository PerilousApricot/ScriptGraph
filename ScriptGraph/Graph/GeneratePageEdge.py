#!/usr/bin/env python

from ScriptGraph.Graph.Edge import Edge
import os.path
import os
import commands
import types
from ScriptGraph.Helpers.LateBind import LateBind
from ScriptGraph.Graph.LocalScriptEdge import LocalScriptEdge

class GeneratePageEdge( Edge ):

#globalPageEdge = GeneratePageEdge( name     = "generateGlobalPage",
#                                   filename = "index.html",
#                                   webRoot  = webRoot,
#                                   httpRoot = httpRoot,
#                                   content  = BindFunction( func = pageText,
#                                                args = { 'pages' : allPages } ) )

#    def __init__( self, command, output,
#                       noEmptyFiles = False, 
#                       name = "",
#                       addFileNamesToCommandLine=False):
    def __init__( self, name, filename, webRoot, httpRoot, content ):
        self.filename = filename
        self.webRoot  = webRoot
        self.httpRoot = httpRoot
        self.content  = content
        Edge.__init__( self, name )
    
    def checkStatusImpl( self ):
        self.clearOutputFiles()
        targetPath = os.path.join( self.webRoot, self.filename ) 
        if ( os.path.exists( targetPath )
                and not ( os.path.getsize( targetPath ) == 0 ) ):
            self.addOutputFile( targetPath )
            self.setComplete()
            return
        elif os.path.exists( targetPath ) and os.path.getsize( targetPath ) == 0:
            print "Page %s is zero length" % self.getName()
            self.setFailed()
            return
        else:
            self.setIncomplete()
    
    def executeImpl( self, inputFiles, inputMap ):
        print "Content type is - %s" % type( self.content )
        print "Content dump is - %s" % self.content
        
        if isinstance( self.content, type([]) ):
            commandReplaced = []
            for onearg in self.content:
                commandReplaced.append( str( self.lateBind( onearg ) ) )
            
            commandReplaced = " ".join(commandReplaced)
        elif issubclass( self.content.__class__ , LateBind ):
            commandReplaced = self.lateBind( self.content )
            if isinstance( commandReplaced, type([]) ):
                commandReplaced = " ".join(commandReplaced)
        else:
            commandReplaced = self.content
        
        targetFileName = os.path.join( self.webRoot, self.filename )
        handle = open(targetFileName, "w+")
        handle.write( commandReplaced )
        handle.close()
        
        if not os.path.exists( targetFileName ):
            raise RuntimeError, "Output from script wasn't found"
        if os.path.getsize( targetFileName ) == 0:
            raise RuntimeError, "Output from script had zero bytes"
        
