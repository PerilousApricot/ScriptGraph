#!/usr/bin/env python
import os, os.path, re, sys
import ScriptGraph.Graph.Graph as Graph
from ScriptGraph.Graph.Node import Node
from ScriptGraph.Graph.LocalScriptEdge import LocalScriptEdge
from ScriptGraph.Helpers.BindFunction import BindFunction
from ScriptGraph.Helpers.BindSubstitutes import BindSubstitutes
from ScriptGraph.Graph.NullEdge import NullEdge

sys.path.append( os.path.dirname( os.path.abspath( __file__ )  ))
import genTree
from s8.MonitorInput import run_monitor_input_helper
from s8.RootQCD import merge_with_root_qcd_helper
from s8.Hadd import hadd_helper

from ScriptGraph.Helpers.Miter import Miter

g = Graph.Graph()
baseWorkDir = "/uscms_data/d2/meloam/dijetexercise"
g.setWorkDir( baseWorkDir )
treeGraph = genTree.getGraph()
exports   = genTree.getDatasets()

g.addGraph( treeGraph )

operating_points = ["TCHEM", "TCHEL",
                    "TCHPT",  "TCHPM",  "TCHPL",
                    "SSVT",   "SSVM",   "SSVL",
                    "SSVHET", "SSVHEM",
                    "SSVHPT" ]

# operating_points = operating_points[0:1]

jet_bins = [ #["40to60", "40..60", "35", "65"]
             #,["60to80", "60..80", "55", "85" ]
             ["80to140", "80..140","75","145"]
             ,["140to", "140..","135","225"]
           ]
trigger_list_linked = { 'RUN2010B' : [ [ "HLT_BTagMu_DiJet20U*","hltdijet20u", "147196", "148818","HLT_BTagMu_DiJet20U" ],
                                     # "HLT_BTagMu_DiJet30U*","hltdijet30u", "148819", "149294","HLT_BTagMu_DiJet30U"   ],
                                     [ "HLT_BTagMu_Jet20U"   ,"hltjet20u"  , "146428", "147116","HLT_BTagMu_Jet20U"  ] ],
                        'RUN2010A' : [ [ "HLT_BTagMu_Jet10U"   ,"hltjet10u"  , "141961", "142039","HLT_BTagMu_Jet10U" ] ]
                                  }
data_datasets = [ ['/BTau/Run2010B-Dec22ReReco_v1/AOD', 'RUN2010B'],
                  ['/BTau/Run2010A-Dec22ReReco_v1/AOD', 'RUN2010A']
]


#
# Run s8_monitor_input over QCD without skipping events
#
dijet10QCDMiter   = Miter()
for dataset in exports['qcd'].iterMany():
    for bin in jet_bins:
        for opoint in operating_points:
            step_postfix = "-%s-%s-%s-djet10mc" % (dataset[1]['dataset'],bin[0],opoint)
            currNode = run_monitor_input_helper( g,
                        jet_pt = bin[1],
                        tag = opoint,
                        fileKey = dataset[1]['dataset'],
                        trigger_name = "HLT_BTagMu_DiJet10U",
                        step_postfix = step_postfix,
                        muon_pt = "6..",
                        input_files = dataset[0] )
            dijet10QCDMiter.add( currNode, bin = bin[0], dataset=dataset[1]['dataset'], opoint=opoint )
singleMonitorMiter = Miter()
rootQCDMiter = Miter()
# merge the dijet10 QCD into one guy
for currmerge in dijet10QCDMiter.iterGrouped( 'bin', 'opoint' ):
    step_postfix = "-%s-%s" % ( currmerge.vals[0][1]['bin'], currmerge.vals[0][1]['opoint'] )
    mergeNode = merge_with_root_qcd_helper( g, "root-qcd" + step_postfix, step_postfix,
                    inputNodes  = currmerge.getValues(),
                    triggerName = "hltjet10u",
                    lumiMiter   = exports['luminosityMiter']
                )
    rootQCDMiter.add( mergeNode,
                        bin     = currmerge.vals[0][1]['bin'],
                        opoint  = currmerge.vals[0][1]['opoint'] )
    singleMonitorMiter.add( mergeNode,\
                                    trigger = 'hltdijet10u',
                                    bin     = currmerge.vals[0][1]['bin'],
                                    opoint  = currmerge.vals[0][1]['opoint'],
                                    njetpt  = True,
                                    njeteta = True,
                                    privert = True,
                                    type    = "skipless_root_qcd" )
skiplessDataMiter = Miter()
for opoint in operating_points:
    for bin in jet_bins:
        for sample in trigger_list_linked.keys():
            for trigger in trigger_list_linked[ sample ]:
                step_postfix = "-%s-%s-%s-%s-noskip" % (sample,trigger[1],bin[0],opoint)
                monitor_node =\
                        run_monitor_input_helper( g,
                            jet_pt = bin[1],
                            tag = opoint,
                            fileKey = sample,
                            trigger_name = trigger[4],
                            step_postfix = step_postfix,
                            muon_pt = "6..",
                            data = "1",
                            input_files = exports['data'].getValues( dataset = sample ))
                
                singleMonitorMiter.add( monitor_node,dataset = sample,
                                                trigger = trigger[1],
                                                bin     = bin[0],
                                                opoint  = opoint,
                                                njetpt  = True,
                                                njeteta = True,
                                                privert = True,
                                                type    = "data" )
 
                skiplessDataMiter.add( monitor_node, dataset = sample,
                                                     trigger = trigger[1],
                                                     bin     = bin[0],
                                                     opoint  = opoint )

haddedDataMiter = Miter()
for opoint in operating_points:
    for bin in jet_bins:  
        currNode = hadd_helper( g, "-%s-%s-data-highpt-20" % (bin[0],opoint), 
                                        skiplessDataMiter.getValues( bin=bin[0],
                                        opoint = opoint) )
        haddedDataMiter.add(
                            currNode,
                            opoint = opoint,
                            bin = bin[0] )

        singleMonitorMiter.add( currNode,\
                                                bin     = bin[0],
                                                opoint  = opoint,
                                                njetpt  = True,
                                                njeteta = True,
                                                privert = True,
                                                type    = "hadd_data" )
 

#
# Some website stuff
#
allPages = Miter()
webRoot  = "/afs/fnal.gov/files/home/room3/meloam/public_html/s8/highpt-with-20/"
httpRoot = "http://home.fnal.gov/~meloam/s8/highpt-with-20/"


#
# Generate single monitoring plots
#
singleMonitorImages = Miter()
singleMonitorTarget = Node( name = "singleMonitorTarget" )
singleMonitorAbs = os.path.join( webRoot, 'singleMonitor' )
singleMonitorRel  = 'singleMonitor'

g.addNode( singleMonitorTarget )

count = 0
singleComparisonMacro = '/uscms/home/meloam/scratch/s8workflow/SingleComparison.C'

def getFileNameStub( args ):
    return args['node'].getOnlyFile()

singleMonitorPlotList = Miter()
for onenode in singleMonitorMiter.iterMany():
    count += 1
    subMonitorAbs = os.path.join( singleMonitorAbs, onenode[1]['opoint'], onenode[1]['bin'] )
    subMonitorRel = os.path.join( singleMonitorRel, onenode[1]['opoint'], onenode[1]['bin'] )
    monitorNames = [ [ "MonitorAnalyzer/n/njet_pt", "-njetpt.png", "Njet_{pt}" ],
                     [ "MonitorAnalyzer/n/njet_eta","-njeteta.png", "Njet_{eta}"],
                     [ "MonitorAnalyzer/generic/pvs","-pvs.png", "N_{pv}" ] ]
    fileDesc = onenode[1]['type']
    if 'dataset' in onenode[1]:
        fileDesc += '-%s' % onenode[1]['dataset']
    if 'trigger' in onenode[1]:
        fileDesc += '-%s' % onenode[1]['trigger']

    for oneMonitor in monitorNames:
        #root -b -q 'SingleComparison.C("edges/run_s8_monitor_input-RUN2010A-hltjet10u-60to80-TCHEM-noskip/output.root","MonitorAnalyzer/n/njet_pt","test2.png","this is a header")'

        targetFileNameAbs = os.path.join( subMonitorAbs, "%s%s" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRel = os.path.join( subMonitorRel, "%s%s" % ( fileDesc, oneMonitor[1] ) )
        getImage = LocalScriptEdge(
                            name = "extract-monitor-%s-%s" % (count, oneMonitor[1]) , 
                            command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\",\"%s\",\"%s\")'" % \
                                ( singleComparisonMacro, '%s', 
                                    oneMonitor[0], targetFileNameAbs, oneMonitor[2] ),
                                  [BindFunction( func = getFileNameStub,
                                                 args = { 'node': onenode[0] } )] ),
                            output=targetFileNameAbs, 
                            noEmptyFiles=True)
        getImage.setWorkDir( subMonitorAbs )
        g.addEdge( onenode[0], singleMonitorTarget, getImage )

        singleMonitorPlotList.add( targetFileNameRel, opoint = onenode[1]['opoint'], 
                                                      bin    = onenode[1]['bin'],
                                                      trigger= onenode[1]['trigger'] if 'trigger' in onenode[1] else "none" )



mcVsDataCollect = Node( name = "mcVsData-collect" )
mcVsDataWebpage = Node( name = "mcVsData-page" )
doubleMonitorImages = Miter()
doubleMonitorTarget = Node( name = "doubleMonitorTarget" )
doubleMonitorAbs = os.path.join( webRoot, 'doubleMonitor' )
doubleMonitorRel  = 'doubleMonitor'

g.addNode( doubleMonitorTarget )

count = 0
doubleComparisonMacro = '/uscms/home/meloam/scratch/s8workflow/DoubleComparison.C'

def getFileNameStub( args ):
    print "looking at %s" % args['node'].getName()
    try:
        return args['node'].getOnlyFile()
    except:
        print "failed at %s" % args['node'].getName()
        raise

doubleMonitorPlotList = Miter()

for (onedata, onemc) in haddedDataMiter.zip( rootQCDMiter ):
    print "zipping %s %s" % (onedata[1], onemc[1] )

    
    subMonitorAbs = os.path.join( doubleMonitorAbs, onedata[1]['opoint'], onedata[1]['bin'] )
    subMonitorRel = os.path.join( doubleMonitorRel, onedata[1]['opoint'], onedata[1]['bin'] )
    for onebin in jet_bins:
        if onebin[0] == onedata[1]['bin']:
            minbin = onebin[2]
            maxbin = onebin[3]

    monitorNames = [ [ "MonitorAnalyzer/n/njet_pt", "-njetpt", "Njet_{pt}" ],
                     [ "MonitorAnalyzer/n/njet_eta","-njeteta", "Njet_{eta}"],
                     [ "MonitorAnalyzer/generic/pvs","-pvs", "N_{pv}" ] ]
    fileDesc = "mcVsdata"
#    if 'dataset' in onenode[1]:
#        fileDesc += '-%s' % onenode[1]['dataset']
#    if 'trigger' in onenode[1]:
#        fileDesc += '-%s' % onenode[1]['trigger']

    for oneMonitor in monitorNames:
        #root -b -q 'SingleComparison.C("edges/run_s8_monitor_input-RUN2010A-hltjet10u-60to80-TCHEM-noskip/output.root","MonitorAnalyzer/n/njet_pt","test2.png","this is a header")'
        count += 1
        targetFileNameAbs = os.path.join( subMonitorAbs, "%s%s.png" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRel = os.path.join( subMonitorRel, "%s%s.png" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRatioAbs = os.path.join( subMonitorAbs, "%s%s-ratio.png" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRatioRel = os.path.join( subMonitorRel, "%s%s-ratio.png" % ( fileDesc, oneMonitor[1] ) )
        getImage = LocalScriptEdge(
                            name = "extract-monitor-double-%s-%s" % (count, oneMonitor[1]) , 
                            command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",%s,%s)'" % \
                                ( doubleComparisonMacro, '%s','%s', 
                                    oneMonitor[0], targetFileNameAbs,targetFileNameRatioAbs, oneMonitor[2], minbin, maxbin  ),
                                  [BindFunction( func = getFileNameStub,
                                                 args = { 'node': onedata[0] } ),
                                   BindFunction( func = getFileNameStub,
                                                 args = { 'node': onemc[0] } )] ),
                            output=targetFileNameAbs, 
                            noEmptyFiles=True)
        getImage.setWorkDir( subMonitorAbs )
        collectNode = Node( "collect-double-%s" % count )
        g.addNode( collectNode )
        g.addEdge( onedata[0], collectNode, NullEdge() )
        g.addEdge( onemc[0], collectNode, NullEdge() )

        g.addEdge( collectNode, doubleMonitorTarget, getImage )

        doubleMonitorPlotList.add( targetFileNameRel, opoint = onedata[1]['opoint'], 
                                                      bin    = onedata[1]['bin'],
                                                      type   = oneMonitor[1],
                                                      ratiorel= targetFileNameRatioRel )

output = "<html><head><title>dijet10 exercise</title></head><body><h1>dijet10U exercise</h1><hr />"
output += "<p>MC is cut on the HLT_BTagMu_DiJet10U trigger, Data is Jet10U+Jet20U+DiJet20U</p><hr />"
for onebin in doubleMonitorPlotList.iterGrouped( 'bin' ):
    currbin = onebin.vals[0][1]['bin']
    output += "<h2> bin %s </h2><hr />" % currbin
    for oneop in onebin.iterGrouped( 'opoint' ):
        currop  = oneop.vals[0][1]['opoint']
        output += "<h3>Operating point %s</h3>" % currop
        for onemon in oneop.iterMany():
            output += "<h4>%s</h4>\n" % onemon[1]['type']
            output += '<img src="%s" /> <img src="%s" />\n' % (onemon[0], onemon[1]['ratiorel'])
output += "</body>"

print output
handle = open( webRoot + "/dump.html", "w+" )
handle.write(output)
handle.close()



def getGraph():
    global g
    return g
#for opoint in operating_points:
#    for bin in jet_bins:
#        for sample in qcd_datasets:
#            for trigger in trigger_list:
#                step_postfix = "-%s-%s-%s-%s-noskip" % (sample[1],trigger[1],bin[0],opoint)
#                if not opoint in skiplessS8Monitor:
#                    skiplessS8Monitor[ opoint ] = {}
#                if not bin[0] in skiplessS8Monitor[ opoint ]:
#                    skiplessS8Monitor[ opoint ][ bin[0] ] = {}
#                if not trigger[1] in skiplessS8Monitor[ opoint ][ bin[0] ]:
#                    skiplessS8Monitor[ opoint ][ bin[0] ][ trigger[1] ] = []
#
#                currNode = run_monitor_input_helper( g,
#                            jet_pt = bin[1],
#                            tag = opoint,
#                            fileKey = sample[1],
#                            trigger_name = trigger[4],
#                            step_postfix = step_postfix,
#                            muon_pt = "6..",
#                            input_files = qcdTreeMiter.getOneValue( dataset =  sample[1] ),
#                            simulate_trigger = triggers_to_simulate[ trigger[1] ])
#                skiplessS8Monitor[ opoint ][ bin[0] ][ trigger[1] ].append( currNode )
#                singleMonitorMiter.add( currNode,dataset = sample[1],
#                                                trigger = trigger[1],
#                                                bin     = bin[0],
#                                                opoint  = opoint,
#                                                njetpt  = True,
#                                                njeteta = True,
#                                                privert = True,
#                                                type    = "skipless" )
# 
#
#                skiplessQCDMiter.add( currNode, dataset = sample[1],
#                                                trigger = trigger[1],
#                                                bin     = bin[0],
#                                                opoint  = opoint )
# 
#
#
#
