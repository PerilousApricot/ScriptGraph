#!/usr/bin/env python

import ScriptGraph.Graph.Edge as Edge
import os.path
import os
import commands
import types
from ScriptGraph.Helpers.LateBind import LateBind

def nullFunc():
    pass

class LambdaEdge( Edge.Edge ):
    def __init__( self, command, name = "", status = None, **extraArgs ):
        self.command = command
        if status:
            self.status = status
        else:
            self.status = None

        if extraArgs:
            self.extraArgs = extraArgs
        else:
            self.extraArgs = {}

        Edge.Edge.__init__( self, name )
    
    def checkStatusImpl( self ):
        if self.status:
            return self.status( self, **(self.extraArgs) )

    def executeImpl( self, inputFiles , inputMap  ):
        return self.command( self, inputFiles, inputMap, **(self.extraArgs) )
        
