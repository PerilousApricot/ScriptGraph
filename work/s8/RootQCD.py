from ScriptGraph.Graph.Node import Node
from ScriptGraph.Graph.LocalScriptEdge import LocalScriptEdge
from ScriptGraph.Helpers.LateBind import LateBind
from ScriptGraph.Graph.NullEdge import NullEdge
import re,os,os.path

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
    pattern = "_input-([0-9to]+?)-"
    match   = re.search( pattern, name )
    if match:
        filename = match.group(1)
        if filename == "150to":
            filename = "150"

        return "pt" + filename + ".root"
    else:
        raise RuntimeError, "Unknown pt bin with name %s" % name


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
        print "appended commandline: %s" % self.currCommand
        return self.currCommand

def merge_with_root_qcd_helper( g, name , step_postfix,
                                inputNodes, triggerName = None, lumiMiter = None, pu = False ):
    mergeNode   =  Node( name = name )           
    collectNode =  Node( name = "collect-" + name )
    # make the node
    g.addNode( mergeNode )
    g.addNode( collectNode )
    # Add the luminosity as a dependency
    if lumiMiter:
        for onenode in lumiMiter.get( trigger = triggerName ):
            g.addEdge( onenode[0], collectNode, NullEdge() )

    # Add the previous S8 runs as a dependency
    for node in inputNodes:
        g.addEdge( node, collectNode, NullEdge() )
    
    #
    # Generate the command line
    #
    executable = "root_qcd_pu"
    if pu:
        executable = "root_qcd_pu"
        
    if lumiMiter:
        command = appendCommandLineSymlink(
                        currCommand = 
                            [executable, getLumi( lumiMiter.get( trigger = triggerName ) ), "merge.root" ],
                                nodeList = inputNodes )
    else:
         command = appendCommandLineSymlink(
                        currCommand = 
                            [executable, "1", "merge.root" ],
                                nodeList = inputNodes )

    merge_edge = LocalScriptEdge(
                        command = command,
                        output  = "merge.root",
                        noEmptyFiles = True,
                        name = "run_root_qcd-" + step_postfix )

    g.addEdge( collectNode, mergeNode, merge_edge )



    return mergeNode

