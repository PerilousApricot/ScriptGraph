class Edge:
    parent = None
    child  = None
    name = "UNSET"
    def __init__( self, name = "" ):
        if name:
            self.name = name

    def setName( self, name ):
        if ( self.name != "UNSET" ):
            raise RuntimeError, "Cannot rename edges"
        self.name = name

    def getName( self ):
        if ( self.name == "UNSET"):
            raise RuntimeError, "Name was not set in edge"

        return self.name

    def setParent( self, parent ):
        if self.hasParent() and ( self.parent != parent ):
            print "%s and %s" % (self.parent, parent)
            raise RuntimeError, "Can't change the parent"
        self.parent = parent
    
    def setChild( self, child ):
        if self.hasChild() and ( self.child != child ):
            raise RuntimeError, "Can't change the child"
        self.child = child

    def hasParent( self ):
        return self.parent != None

    def hasChild( self ):
        return self.child != None

    def getParent( self ):
        return self.parent
    
    def getChild( self ):
        return self.child
