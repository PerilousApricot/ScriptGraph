#!/usr/bin/env python

import ScriptGraph.Helpers.LateBind as LateBind
import ScriptGraph.Graph.Node as Node
class BindPreviousOutput( LateBind.LateBind ):
    
    def __init__( self, target = None ):
        self.target = target
    
    
    def bind( self, edge ):
        if self.target:
            target = self.target
        else:
            target = edge

        if issubclass( target.__class__, Node.Node ):
            print "BINDING NODE"
            files,_ = target.getFiles()
        else:
            print "BINDING EDGE"
            print target
            print self.target
            print edge
            #files,_ = target.getParent().getFiles()
            files = target.getOutputFiles()

        if ( len(files) != 1 ):
            raise RuntimeError, "Too many/few files in target %s" % (target.getName() )
        return files[0]
