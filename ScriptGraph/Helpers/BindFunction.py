from ScriptGraph.Helpers.LateBind import LateBind
class BindFunction( LateBind ):
    def __init__( self, func, args = None ):
        self.func = func
        if not args:
            args = {}
        self.args = args

    def bind( self, edge ):
        return self.func( self.args )


