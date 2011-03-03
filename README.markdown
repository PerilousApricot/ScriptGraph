ScriptGraph - a tool for pepole who like tools
==============================================

Motivation
----------

I use this project to automate my physics research, but this could apply to anyone.
The (little) research I've done involves basically a pattern like this:

>(start) -> (step a) -> (output a) -> (step b) -> (output b)

Where the different steps can be things like a short script to add up luminosities
or grid submission consisting of 1000's of jobs. If this was the only thing I'd have
to do, it'd be easy, but invariably, the suggestion comes from above to make things
more difficult.

>(start) -> (step a) -> (output a) -> (step b with parameter 1) -> (output b1)
>(start) -> (step a) -> (output a) -> (step b with parameter 2) -> (output b2)
>(start) -> (step a) -> (output a) -> (step b with parameter 3) -> (output b3)
>(start) -> (step a) -> (output a) -> (step b with parameter 4) -> (output b4)

And more difficult

>(start) -> (step a w/param x) -> (output ax) -> (step b with parameter 1) -> (output bx1)
>(start) -> (step a w/param y) -> (output ay) -> (step b with parameter 1) -> (output by1)
>(start) -> (step a w/param z) -> (output az) -> (step b with parameter 1) -> (output bz1)
>(start) -> (step a w/param w) -> (output aw) -> (step b with parameter 1) -> (output bw1)
>(start) -> (step a w/param x) -> (output ax) -> (step b with parameter 2) -> (output bx2)
>(start) -> (step a w/param y) -> (output ay) -> (step b with parameter 2) -> (output by2)
>(start) -> (step a w/param z) -> (output az) -> (step b with parameter 2) -> (output bz2)
>(start) -> (step a w/param w) -> (output aw) -> (step b with parameter 2) -> (output bw2)
>(start) -> (step a w/param x) -> (output ax) -> (step b with parameter 3) -> (output bx3)
>(start) -> (step a w/param y) -> (output ay) -> (step b with parameter 3) -> (output by3)
>(start) -> (step a w/param z) -> (output az) -> (step b with parameter 3) -> (output bz3)
>(start) -> (step a w/param w) -> (output aw) -> (step b with parameter 3) -> (output bw3)

And so on, and so on.

Clearly there's a lot of headaches involved with this process. One could try and
expand out this combinatorial explosion manually, hoping (praying) that they
never forget to change an output filename in a script or miss rerunning a step
when the underlying program changes. One could also try to write a script that does:

> for a in paramlista:
>     for b in paramlistb:
>         command.execute( a, b )

But that ends up getting ugly as the number of parameters you want to vary over
increases, doesn't mesh well with asynchronous steps, and serializes your effort
since (unless you get tricky), you are executing one process at a time.

What ScriptGraph does
---------------------

What's really needed is a framework that can keep track the completion status of
(possibly) asynchronous jobs, and which step is dependent on what other steps. A natural
data structure for this is a graph, where the edges are different actions to be performed
and the nodes are some type of output.

Using our simple example above (and hopefully easily rendered using ascii),


>                                 /-> (step b with parameter 1) -> (output b1)
>(start) -> (step a)--> (output a)--> (step b with parameter 2) -> (output b2)
>                                 \-> (step b with parameter 3) -> (output b3)

ScriptGraph has extensible classes that can check the status and execute a step. For instance,
step a may be a grid job (where we submit several jobs to a cluster to execute), and
in this case to check status, the edge class will query the scheduler. To execute a step, the
edge will wrap up the command in a jobwrapper and execute the submit function. There are
prebuilt subclasses to run local scripts, submit to the condor scheduler or use CRAB
(an experiment-specific program).

Once the workflow is set up, executing the steps is done with

> scriptGraph.py --cfg=someworkflow.py --push

Which will check the status of all the edges, and for any edges that have their
dependencies complete, it will execute them. Each edge is run in its own
subdirectory, and has its command line arguments populated automatically
so there's less of a chance of minor errors

Simple example:
---------------

Without more talking, here's a simple example:

	from ScriptGraph.Graph.Node import Node
	from ScriptGraph.Graph.LocalScriptEdge import LocalScriptEdge
	from ScriptGraph.Graph.Graph import Graph

	# Create the graph object
	g = Graph()
	
	# An node without any incoming edges is always ready to be
	# executed. These make good "start" nodes
	nullInput = Node( name = "nullInput" )
	# Add this node to the graph
	g.addNode( nullInput )

	datasetSum = ['dataset1', 'dataset2', 'dataset3']
	for dataset in datasetSum:
		# Make a node to store the output from this script
		fileList = Node( name = "filelist-%s" % dataset )

		# This is the edge that will actually execute the 'dbs' script
		datasetSearch = LocalScriptEdge(
							name = "dbsQuery-%s" % dataset,
							command = "dbs search --query='find file where dataset=%s' | tail -n +5 > input.txt" % dataset,
							output="input.txt",
							noEmptyFiles=True)

		# Add this node to the graph
		g.addNode( fileList )

		# Tell the graph that the way to get from nullInput to fileList is
		# by executing the datasetSearch command
		g.addEdge( nullInput, fileList, datasetSearch )

	# a helper function to help the framework find the graph object
	def getGraph():
		global g
		return g

Conceptually, this becomes a workflow that looks like this:

>              /-> (dbsQuery-dataset1) -> (filelist-dataset1)
> (nullInput) ---> (dbsQuery-dataset2) -> (filelist-dataset2)
>			   \-> (dbsQuery-dataset3) -> (filelist-dataset3)

Obviously, this is a simple example, but it lets me demonstrate some simple patterns
with ScriptGraph. First, some notes:

1. Names for nodes and edges must be unique
2. Nodes and edges must be added to the graph to let it know how things are linked
3. Configuration files must have a getGraph() function that returns the graph

When --push is called, it will see that nullInput is in the "ready" state since
it has no dependencies, then it will execute each of the datasetSearch edges,
since their parent node (nullInput in this case) is ready. Future executions
will check to see if the output file from datasetSearch already exists (in this
case, the file "input.txt"), and if so, it will note the dbsQuery edges as
"complete" which, in turn sets the filelist nodes to be "ready" to execute
any child edges is has.

Now, the whole point of chainging these commands together is to work on some
input files, let's look at how that works

>              /-> (dbsQuery-dataset1) -> (filelist-dataset1) -> (getHead) -> (head-dataset1)
> (nullInput) ---> (dbsQuery-dataset2) -> (filelist-dataset2) -> (getHead) -> (head-dataset2)
>			   \-> (dbsQuery-dataset3) -> (filelist-dataset3) -> (getHead) -> (head-dataset3)


We would do this by adding a new set of edges and nodes, and a new thing called
late binding.
	
	from ScriptGraph.Helpers.BindSubstitutes import BindSubstitutes
	from ScriptGraph.Helpers.BindPreviousOutput import BindPreviousOutput
	for dataset in datasetSum:
		previousNode = g.getNode( "filelist-%s" % dataset )
		newNode      = Node ( "head-%s" % dataset )
		headEdge     = LocalScriptEdge(
							name = "getHead-%s" % dataset,
							command = BindSubstitutes( formatString = "head %s > headfile.txt",
													   bindList = BindPreviousOutput() ),
							output = "headfile.txt",
							noEmptyFiles = True )
		g.addNode( newNode )
		g.addEdge( previousNode, newNode, headEdge )











