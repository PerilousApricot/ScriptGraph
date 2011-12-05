#!/usr/bin/env python
import os, os.path, re, sys
import ScriptGraph.Graph.Graph as Graph
from ScriptGraph.Graph.Node import Node
from ScriptGraph.Graph.LocalScriptEdge import LocalScriptEdge
from ScriptGraph.Helpers.BindFunction import BindFunction
from ScriptGraph.Helpers.BindSubstitutes import BindSubstitutes
from ScriptGraph.Helpers.BindPreviousOutput import BindPreviousOutput
from ScriptGraph.Graph.NullEdge import NullEdge

sys.path.append( os.path.dirname( os.path.abspath( __file__ )  ))
import genTree
from s8.MonitorInput import run_monitor_input_helper
from s8.RootQCD import merge_with_root_qcd_helper
from s8.Hadd import hadd_helper

from ScriptGraph.Helpers.Miter import Miter

g = Graph.Graph()
singleMonitorMiter = Miter()
baseWorkDir = "/uscms_data/d2/meloam/input39x2"
g.setWorkDir( baseWorkDir )
treeGraph = genTree.getGraph()
exports   = genTree.getDatasets()

g.addGraph( treeGraph )

operating_points = ["TCHEM", "TCHEL", "TCHET",
                    "TCHPT",  "TCHPM",  "TCHPL",
                    "SSVT",   "SSVM",   "SSVL",
                    "SSVHET", "SSVHEM",
                    "SSVHPT" ]

#operating_points = [ "TCHEL" ]

# operating_points = operating_points[0:1]

jet_bins = [ ["40to60", "40..60",   "35", "65"]
             ,["60to80", "60..80",  "55", "85" ]
             ,["80to", "80..","75", "200"]
 #            ,["140to", "140..", "135", "200"]
           ]

trigger_list_linked = { 'RUN2010B' : [ [ "HLT_BTagMu_DiJet20U*","hltdijet20u", "147196", "148818","HLT_BTagMu_DiJet20U" ],
                                     [ "HLT_BTagMu_DiJet20U*","hltdijet20u-fullrange", "147196", "149294","HLT_BTagMu_DiJet20U" ],
                                     [ "HLT_BTagMu_DiJet30U*","hltdijet30u", "148819", "149294","HLT_BTagMu_DiJet30U"   ],
                                     [ "HLT_BTagMu_Jet20U"   ,"hltjet20u"  , "146428", "147116","HLT_BTagMu_Jet20U"  ] ],
                        'RUN2010A' : [ [ "HLT_BTagMu_Jet10U"   ,"hltjet10u"  , "141961", "142039","HLT_BTagMu_Jet10U" ] ]
                                  }
data_datasets = [ ['/BTau/Run2010B-Dec22ReReco_v1/AOD', 'RUN2010B'],
                  ['/BTau/Run2010A-Dec22ReReco_v1/AOD', 'RUN2010A']
]

trigger_list = [ [ "HLT_BTagMu_DiJet20U*","hltdijet20u", "147196", "148818","HLT_BTagMu_DiJet20U" ],
                 [ "HLT_BTagMu_DiJet20U*","hltdijet20u-fullrange", "147196", "149294","HLT_BTagMu_DiJet20U" ],
                 [ "HLT_BTagMu_DiJet30U*","hltdijet30u", "148819", "149294","HLT_BTagMu_DiJet30U" ],
                 [ "HLT_BTagMu_Jet10U"   ,"hltjet10u"  , "141961", "142039","HLT_BTagMu_Jet10U" ],
                 [ "HLT_BTagMu_Jet20U"   ,"hltjet20u"  , "146428", "147116","HLT_BTagMu_Jet20U" ] 
]

trigger_map  = { 
    'hltdijet20u' :
                 [ "HLT_BTagMu_DiJet20U*","hltdijet20u", "147196", "148818","HLT_BTagMu_DiJet20U", 'RUN2010B' ],
    'hltdijet20u-fullrange' :
                 [ "HLT_BTagMu_DiJet20U*","hltdijet20u-fullrange", "147196", "149294","HLT_BTagMu_DiJet20U" , 'RUN2010B' ],
    'hltdijet30u' :
                 [ "HLT_BTagMu_DiJet30U*","hltdijet30u", "148819", "149294","HLT_BTagMu_DiJet30U" , 'RUN2010B' ],
    'hltjet10u' :
                 [ "HLT_BTagMu_Jet10U"   ,"hltjet10u"  , "141961", "142039","HLT_BTagMu_Jet10U" , 'RUN2010A' ],
    'hltjet20u' :
                 [ "HLT_BTagMu_Jet20U"   ,"hltjet20u"  , "146428", "147116","HLT_BTagMu_Jet20U" , 'RUN2010B' ] 
}

dataTriggerMiter = Miter()

dataTriggerMiter.add( trigger_map['hltjet10u'], bin = "40..60" )
dataTriggerMiter.add( trigger_map['hltjet20u'], bin = "40..60" )

dataTriggerMiter.add( trigger_map['hltjet10u'], bin = "60..80" )
dataTriggerMiter.add( trigger_map['hltjet20u'], bin = "60..80" )
dataTriggerMiter.add( trigger_map['hltdijet20u-fullrange'], bin = "60..80" )
dataTriggerMiter.add( trigger_map['hltdijet20u'], bin = "60..80" )


dataTriggerMiter.add( trigger_map['hltjet10u'], bin = "80.." )
dataTriggerMiter.add( trigger_map['hltjet20u'], bin = "80.." )
dataTriggerMiter.add( trigger_map['hltdijet20u'], bin = "80.." )
dataTriggerMiter.add( trigger_map['hltdijet30u'], bin = "80.." )


#dataTriggerMiter.add( trigger_map['hltjet10u'], bin = "140.." )
#dataTriggerMiter.add( trigger_map['hltjet20u'], bin = "140.." )
#dataTriggerMiter.add( trigger_map['hltdijet20u'], bin = "140.." )
#dataTriggerMiter.add( trigger_map['hltdijet30u'], bin = "140.." )

skiplessDataMiter = Miter()
for opoint in operating_points:
    for bin in jet_bins:
        for tmap in dataTriggerMiter.iterGrouped( 'bin' ):
            if tmap.vals[0][1]['bin'] != bin[1]:
                continue

            for onetmap in tmap.iterMany():
                sample = onetmap[0][5]
                trigger = onetmap[0]
                step_postfix = "-%s-%s-%s-%s" % (sample,trigger[1],bin[0],opoint)
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

#
# Merge triggers
#
dataForWeights = {}
haddedDataMiter = Miter()
dataForPlots = Miter()
for opoint in operating_points:
    for bin in jet_bins:  
        currNode = hadd_helper( g, "-%s-%s-data-combine" % (bin[0],opoint), 
                                        skiplessDataMiter.getValues( bin=bin[0],
                                        opoint = opoint) )
        haddedDataMiter.add(
                            currNode,
                            opoint = opoint,
                            bin = bin[0] )
        print "the opoint is %s" % opoint
        singleMonitorMiter.add( currNode,\
                                                bin     = bin[0],
                                                opoint  = opoint,
                                                njetpt  = True,
                                                njeteta = True,
                                                privert = True,
                                                type    = "hadd_data" )
        dataForWeights[ bin[0] ] = currNode
        if opoint == "TCHEM":
            dataForPlots.add( currNode, bin = bin[0] )
#
# Merge Bins
#
haddedDataFinalMiter = Miter()
for opoint in haddedDataMiter.iterGrouped( 'opoint' ):
    currNode = hadd_helper( g, "-%s-data-final-combine" % (opoint.vals[0][1]['opoint'],), 
                                skiplessDataMiter.getValues( opoint = opoint.vals[0][1]['opoint']) )
    haddedDataFinalMiter.add(
                        currNode,
                        opoint = opoint.vals[0][1]['opoint'])

    singleMonitorMiter.add( currNode,\
                                            opoint  = opoint.vals[0][1]['opoint'],
                                            njetpt  = True,
                                            njeteta = True,
                                            privert = True,
                                            type    = "hadd_data-final" )


#################
###### At this point, we have all we need for data
#################


#
# Run s8_monitor_input over QCD, for the input to reweighting (so we don't care about tag
# or trigger, only the bin)
#
preweightMiter   = Miter()
for dataset in exports['qcd'].iterMany():
    for bin in jet_bins:
        step_postfix = "-%s-%s-preweight" % (dataset[1]['dataset'],bin[0])
        currNode = run_monitor_input_helper( g,
                    jet_pt = bin[1],
#                    tag = operating_,
                    fileKey = dataset[1]['dataset'],
#                        trigger_name = "HLT_BTagMu_DiJet10U",
                    step_postfix = step_postfix,
                    muon_pt = "6..",
                    input_files = dataset[0] )
        preweightMiter.add( currNode, bin = bin[0], dataset=dataset[1]['dataset'] )

#
# Merge the pthats
#

mcForWeights = {}
preweightMergeMiter = Miter()
mcForPlots = Miter()
for currmerge in preweightMiter.iterGrouped( 'bin' ):
    step_postfix = "-%s-preweight" % ( currmerge.vals[0][1]['bin'], )
    mergeNode = merge_with_root_qcd_helper( g, "root-qcd" + step_postfix, step_postfix,
                    inputNodes  = currmerge.getValues(), pu = True
                )
    preweightMergeMiter.add( mergeNode,
                        bin     = currmerge.vals[0][1]['bin'] )
    singleMonitorMiter.add( mergeNode,\
                                    bin     = currmerge.vals[0][1]['bin'],
                                    njetpt  = True,
                                    njeteta = True,
                                    privert = True,
                                    type    = "skipless_root_qcd" )
    mcForWeights[ currmerge.vals[0][1]['bin'] ] = mergeNode
#    mcForPlots.add( mergeNode, bin = currmerge.vals[0][1]['bin'] )
#
# Calculate the weights (on a bin-only basis)
#
weightMiter = Miter()
binToWeight = {}
binToWeightNode = {}
print "data %s mc %s" % (dataForWeights, mcForWeights )
for bin in jet_bins:
    step_key    = "-%s-combine" % ( bin[0], )
    step_postfix= step_key
    collectNode = Node( name = "collect-reweight" + step_postfix )
    weightNode  = Node( name = "weight" + step_postfix )
    reweighNode = Node( name = "reweighed" + step_postfix )

    dataNode    = dataForWeights[ bin[0] ]
    mcNode      = mcForWeights[ bin[0] ]
    # Usage: root_trigger_weights out.root data_monitor.root mc_monitor.root
    calcWeightEdge = LocalScriptEdge(
                    name = "calcweight" +  step_postfix,
                    command = ["root_trigger_weights",
                                    bin[1],
                                    #"-3..3",
                                    "weight.root",
                                    BindPreviousOutput( dataNode ),
                                    BindPreviousOutput( mcNode   ) ],
                    output = "weight.root",
                    noEmptyFiles = True )
    g.addNode( collectNode )
    g.addNode( weightNode )
    g.addEdge( dataNode, collectNode, NullEdge() )
    g.addEdge( mcNode, collectNode, NullEdge()   )
    g.addEdge( collectNode, weightNode, calcWeightEdge )

    binToWeight[ bin[0] ] = calcWeightEdge
    binToWeightNode[ bin[0] ] = weightNode

reweightMiter = Miter()
for opoint in operating_points:
    for bin in jet_bins:
        for dataset in exports['qcd'].iterMany():
            sample     = dataset[0]
            samplename = dataset[1]['dataset'] 
            step_postfix= "-%s-%s-%s-%s-reweight-combine" % ( samplename, trigger[1], bin[0], opoint )
            step_key    = "%s-%s-%s" % ( trigger[1], bin[0], opoint )
            # now, use the weights to run s8_monitor_input again
            deps = [
                     binToWeightNode[ bin[0] ],
                     # we automatically include deps to input files
                     #qcdTreeNodes[ sample[1]
                   ]


            reweightNode = run_monitor_input_helper( g,
                        step_postfix,
                        fileKey = samplename,
                        jet_pt  = bin[1],
                        tag = opoint,
                        input_files = sample,
                        muon_pt = "6..",
                        additional_dependencies = deps,
                        reweight_trigger = BindPreviousOutput( binToWeight[ bin[0] ] ) )
            reweightMiter.add( reweightNode, 
                                    opoint = opoint,
                                    bin = bin[0], 
                                    dataset= samplename )
            

#
# Merge the pthats
#

reweightMergePthatMiter = Miter()
for currmerge in reweightMiter.iterGrouped( 'bin', 'opoint' ):
    step_postfix = "-%s-%s-reweight-combine" % ( currmerge.vals[0][1]['bin'],currmerge.vals[0][1]['opoint'] )
    mergeNode = merge_with_root_qcd_helper( g, "root-qcd" + step_postfix, step_postfix,
                    inputNodes  = currmerge.getValues(), pu = True
                )
    reweightMergePthatMiter.add( mergeNode,
                        bin     = currmerge.vals[0][1]['bin'],
                        opoint  = currmerge.vals[0][1]['opoint'])
    singleMonitorMiter.add( mergeNode,\
                                    bin     = currmerge.vals[0][1]['bin'],
                                    opoint  = currmerge.vals[0][1]['opoint'],
                                    njetpt  = True,
                                    njeteta = True,
                                    privert = True,
                                    type    = "root_qcd" )
    if currmerge.vals[0][1]['opoint'] == 'TCHEL':
        mcForPlots.add( mergeNode, bin = currmerge.vals[0][1]['bin'] )
#
#
# Merge Bins
#
haddedMCFinalMiter = Miter()
for opoint in reweightMergePthatMiter.iterGrouped( 'opoint' ):
    currNode = hadd_helper( g, "-%s-qcd-mergebin-combine" % (opoint.vals[0][1]['opoint'],), 
                                reweightMergePthatMiter.getValues( opoint = opoint.vals[0][1]['opoint']) )
    haddedMCFinalMiter.add(
                        currNode,
                        opoint = opoint.vals[0][1]['opoint'])

    singleMonitorMiter.add( currNode,\
                                            opoint  = opoint.vals[0][1]['opoint'],
                                            njetpt  = True,
                                            njeteta = True,
                                            privert = True,
                                            type    = "hadd_mc-final" )



#
# Rerun over QCD with weights
#

#preweightMiter   = Miter()
#for dataset in exports['qcd'].iterMany():
#    for bin in jet_bins:
#        step_postfix = "-%s-%s-weighter" % (dataset[1]['dataset'],bin[0])
#        currNode = run_monitor_input_helper( g,
#                    jet_pt = bin[1],
##                    tag = operating_,
#                    fileKey = dataset[1]['dataset'],
##                        trigger_name = "HLT_BTagMu_DiJet10U",
#                    step_postfix = step_postfix,
#                    muon_pt = "6..",
#                    input_files = dataset[0] )
#        preweightMiter.add( currNode, bin = bin[0], dataset=dataset[1]['dataset'] )
#
#
# Some website stuff
#
allPages = Miter()
#webRoot  = "/afs/fnal.gov/files/home/room3/meloam/public_html/s8/39x/"
webRoot  = "/uscms_data/d2/meloam/input39x2/edges/plots-combined/"
httpRoot = "http://home.fnal.gov/~meloam/s8/39x/"


#
# Generate single monitoring plots
#
#singleMonitorImages = Miter()
#singleMonitorTarget = Node( name = "singleMonitorTarget" )
#singleMonitorAbs = os.path.join( webRoot, 'singleMonitor' )
#singleMonitorRel  = 'singleMonitor'
#
#g.addNode( singleMonitorTarget )
#
#count = 0
#singleComparisonMacro = '/uscms/home/meloam/scratch/s8workflow/SingleComparison.C'
#
#def getFileNameStub( args ):
#    return args['node'].getOnlyFile()
#
#singleMonitorPlotList = Miter()
#for onenode in singleMonitorMiter.iterMany():
#
#    print "miter is %s" % onenode[1]
#    count += 1
#    subMonitorAbs = os.path.join( singleMonitorAbs )#, onenode[1]['opoint'], onenode[1]['bin'] )
#    subMonitorRel = os.path.join( singleMonitorRel )#, onenode[1]['opoint'], onenode[1]['bin'] )
#    monitorNames = [ [ "MonitorAnalyzer/n/njet_pt", "-njetpt.png", "Njet_{pt}" ],
#                     [ "MonitorAnalyzer/n/njet_eta","-njeteta.png", "Njet_{eta}"],
#                     [ "MonitorAnalyzer/generic/pvs","-pvs.png", "N_{pv}" ] ]
#    fileDesc = onenode[1]['type']
#    if 'dataset' in onenode[1]:
#        fileDesc += '-%s' % onenode[1]['dataset']
#    if 'trigger' in onenode[1]:
#        fileDesc += '-%s' % onenode[1]['trigger']
#
#    for oneMonitor in monitorNames:
#        #root -b -q 'SingleComparison.C("edges/run_s8_monitor_input-RUN2010A-hltjet10u-60to80-TCHEM-noskip/output.root","MonitorAnalyzer/n/njet_pt","test2.png","this is a header")'
#
#        targetFileNameAbs = os.path.join( subMonitorAbs, "%s%s" % ( fileDesc, oneMonitor[1] ) )
#        targetFileNameRel = os.path.join( subMonitorRel, "%s%s" % ( fileDesc, oneMonitor[1] ) )
#        getImage = LocalScriptEdge(
#                            name = "extract-monitor-%s-%s" % (count, oneMonitor[1]) , 
#                            command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\",\"%s\",\"%s\")'" % \
#                                ( singleComparisonMacro, '%s', 
#                                    oneMonitor[0], targetFileNameAbs, oneMonitor[2] ),
#                                  [BindFunction( func = getFileNameStub,
#                                                 args = { 'node': onenode[0] } )] ),
#                            output=targetFileNameAbs, 
#                            noEmptyFiles=True)
#        getImage.setWorkDir( subMonitorAbs )
#        g.addEdge( onenode[0], singleMonitorTarget, getImage )
#
#        singleMonitorPlotList.add( targetFileNameRel, opoint = onenode[1]['opoint'], 
#                                                      bin    = onenode[1]['bin'],
#                                                      trigger= onenode[1]['trigger'] if 'trigger' in onenode[1] else "none" )
#
#

def getFileNameStub( args ):
    print "looking at %s" % args['node'].getName()
    try:
        return args['node'].getOnlyFile()
    except:
        print "failed at %s" % args['node'].getName()
        raise


s8macro = "/uscms/home/meloam/s8/CMSSW_3_9_8_patch1/src/RecoBTag/PerformanceMeasurements/test/S8Solver/run_s8.C"
for (onedata, onemc) in haddedDataFinalMiter.zip( haddedMCFinalMiter ):
    print "zipping1 %s, %s" % (onedata[1],onemc[1])
    opoint = onedata[1]['opoint']
    collect = Node( name = "collect-s8solve-%s" % opoint )
    solution= Node( name = "s8volve-%s" % opoint )
    g.addNode( collect )
    g.addNode( solution )
    g.addEdge( onedata[0], collect, NullEdge() )
    g.addEdge( onemc[0]  , collect, NullEdge() )

    runs8edge = LocalScriptEdge(
                        name = "s8solver-combined-%s" % ( opoint) , 
                        command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\")' &> output.txt" % \
                            ( s8macro, '%s','%s' ),
                              [BindFunction( func = getFileNameStub,
                                             args = { 'node': onedata[0] } ),
                               BindFunction( func = getFileNameStub,
                                             args = { 'node': onemc[0] } )] ),
                        output="output.txt", 
                        noEmptyFiles=True)
    g.addEdge( collect, solution, runs8edge )


mcVsDataCollect = Node( name = "mcVsData-collect" )
mcVsDataWebpage = Node( name = "mcVsData-page" )
doubleMonitorImages = Miter()
doubleMonitorTarget = Node( name = "doubleMonitorTarget" )
doubleMonitorAbs = os.path.join( webRoot, 'doubleMonitor' )
doubleMonitorRel  = 'doubleMonitor'

g.addNode( doubleMonitorTarget )

count = 0
doubleComparisonMacro = '/uscms/home/meloam/scratch/s8workflow/DoubleComparison.C'
doubleMonitorPlotList = Miter()

for (onedata, onemc) in dataForPlots.zip( mcForPlots ):
    print "zipping %s %s" % (onedata[1], onemc[1] )

    
    subMonitorAbs = os.path.join( doubleMonitorAbs, onedata[1]['bin'] )
    subMonitorRel = os.path.join( doubleMonitorRel, onedata[1]['bin'] )
    minbin = 35
    maxbin = 205
    if 'bin' in onedata[1]:
        for onebin in jet_bins:
            if onebin[0] == onedata[1]['bin']:
                minbin = onebin[2]
                maxbin = onebin[3]

    monitorNames = [ [ "MonitorAnalyzer/n/njet_pt", "-njetpt", "Njet_{pt}" ],
                     [ "MonitorAnalyzer/n/njet_eta","-njeteta", "Njet_{eta}"],
                     [ "MonitorAnalyzer/n/npvs","-pvs", "N_{pv}" ] ]
    fileDesc = "mcVsdata-reweight"
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
        targetFileNameAbsPdf = os.path.join( subMonitorAbs, "%s%s.pdf" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRelPdf = os.path.join( subMonitorRel, "%s%s.pdf" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRatioAbsPdf = os.path.join( subMonitorAbs, "%s%s-ratio.pdf" % ( fileDesc, oneMonitor[1] ) )
        targetFileNameRatioRelPdf = os.path.join( subMonitorRel, "%s%s-ratio.pdf" % ( fileDesc, oneMonitor[1] ) )

        print "Zipping %s to %s" % (onedata[0].getName(), onemc[0].getName() )
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

        doubleMonitorPlotList.add( targetFileNameRel,# opoint = onedata[1]['opoint'], 
                                                     #b:in    = onedata[1]['bin'],
                                                      type   = oneMonitor[1],
                                                      ratiorel= targetFileNameRatioRel )
        getImage = LocalScriptEdge(
                            name = "extract-monitor-double-%s-%s-pdf" % (count, oneMonitor[1]) , 
                            command = BindSubstitutes("root -b -q '%s(\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",%s,%s)'" % \
                                ( doubleComparisonMacro, '%s','%s', 
                                    oneMonitor[0], targetFileNameAbsPdf,targetFileNameRatioAbsPdf, oneMonitor[2], minbin, maxbin  ),
                                  [BindFunction( func = getFileNameStub,
                                                 args = { 'node': onedata[0] } ),
                                   BindFunction( func = getFileNameStub,
                                                 args = { 'node': onemc[0] } )] ),
                            output=targetFileNameAbs, 
                            noEmptyFiles=True)
        getImage.setWorkDir( subMonitorAbs )
        collectNode = Node( "collect-double-%s-pdf" % count )
        g.addNode( collectNode )
        g.addEdge( onedata[0], collectNode, NullEdge() )
        g.addEdge( onemc[0], collectNode, NullEdge() )

        g.addEdge( collectNode, doubleMonitorTarget, getImage )

        doubleMonitorPlotList.add( targetFileNameRel,# opoint = onedata[1]['opoint'], 
                                                     #b:in    = onedata[1]['bin'],
                                                      type   = oneMonitor[1],
                                                      ratiorel= targetFileNameRatioRel )


output = "<html><head><title>dijet10 exercise</title></head><body><h1>dijet10U exercise</h1><hr />"
output += "<p>MC is cut on the HLT_BTagMu_DiJet10U trigger, Data is Jet10U+Jet20U</p><hr />"
#for onebin in doubleMonitorPlotList.iterGrouped( 'bin' ):
#    currbin = onebin.vals[0][1]['bin']
#    output += "<h2> bin %s </h2><hr />" % currbin
for oneop in doubleMonitorPlotList.iterGrouped( 'opoint' ):
#    currop  = oneop.vals[0][1]['opoint']
    currop = "test"
    output += "<h3>Operating point %s</h3>" % currop
    for onemon in oneop.iterMany():
        output += "<h4>%s</h4>\n" % onemon[1]['type']
        output += '<img src="%s" /> <img src="%s" />\n' % (onemon[0], onemon[1]['ratiorel'])
output += "</body>"

try:
    print output
    handle = open( webRoot + "/dump.html", "w+" )
    handle.write(output)
    handle.close()
except:
    print "Couldn't do output"


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
