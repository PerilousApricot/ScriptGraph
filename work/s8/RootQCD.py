from ScriptGraph.Graph.Node import Node
from ScriptGraph.Graph.LocalScriptEdge import LocalScriptEdge
from ScriptGraph.Helpers.LateBind import LateBind

class getLumi( LateBind ):
    def __init__(self, lumiNodeList):
        self.lumiNodes = lumiNodeList
    def bind( self, edge ):
        retval = 0
        print ("str %s" % self.lumiNodes.__class__) + "class"
        for node in self.lumiNodes:
            print "sublumi %s" % node
            retval += float( node[0].getValueFromOnlyOutputFile() )
        return retval
def makeRootQCDFilenameFromInput( name ):
    pattern = "-qcd(.+?)-"
    match   = re.search( pattern, name )
    if match:
        return "pt" + match.group(1) + ".root"
    else:
        raise RuntimeError, "Unknown pt bin"


class appendCommandLineSymlink( LateBind ):
    def __init__(self, currCommand, nodeList):
        self.currCommand = currCommand
        self.nodeList    = nodeList
    def bind( self, edge ):
        for node in self.nodeList:
            for oldFile in node.getFiles()[0]:
                newFile = makeRootQCDFilenameFromInput( oldFile )
                if os.path.exists( newFile ):
                    os.unlink( newFile )
                os.symlink( oldFile, newFile )
                self.currCommand.append( newFile )
#       print "appended commandline: %s" % self.currComand
        return self.currCommand


def merge_with_root_qcd_helper( g, name , step_postfix,
                                inputNodes, triggerName, lumiMiter ):
    mergeNode   =  Node( name = name )           
    collectNode =  Node( name = "collect-" + name ) 
    # make the node
    g.addNode( mergeNode )
    g.addNode( collectNode )
    # Add the luminosity as a dependency
    for onenode in lumiMiter.get( trigger = triggerName ):
        g.addEdge( onenode[0], collectNode, NullEdge() )

    # Add the previous S8 runs as a dependency
    for node in inputNodes:
        g.addEdge( node, collectNode, NullEdge() )
    
    #
    # Generate the command line
    #
    command = appendCommandLineSymlink(
                    currCommand = 
                        ["root_qcd", getLumi( lumiMiter.get( trigger = triggerName ) ), "merge.root" ],
                            nodeList = inputNodes )

    merge_edge = LocalScriptEdge(
                        command = command,
                        output  = "merge.root",
                        noEmptyFiles = True,
                        name = "run_root_qcd-" + step_postfix )

    g.addEdge( collectNode, mergeNode, merge_edge )



    return mergeNode

