from ScriptGraph.Graph.Node import Node
from ScriptGraph.Graph.NullEdge import NullEdge
from ScriptGraph.Graph.LocalScriptEdge import LocalScriptEdge
#
# Use hadd to collapse on triggers. We then have 1 file for [data|MC] * bin * opoint
#

def hadd_helper( g, name, inputNodes ):
    collectNode = Node( "collect-" + name )
    haddNode    = Node( "haddNode-" + name    )
    g.addNode( haddNode    )
    g.addNode( collectNode )
    if not inputNodes:
        raise RuntimeError, "No parents to the hadd node; %s" % name

    for node in inputNodes:
        g.addEdge( node, collectNode, NullEdge() )

    haddEdge = LocalScriptEdge( name = "hadd-%s" % name,
                                     command = "hadd -f merged.root ",
                                     output  = "merged.root",
                                     addFileNamesToCommandLine = True,
                                     noEmptyFiles = True)
    g.addEdge( collectNode, haddNode, haddEdge )
    return haddNode


