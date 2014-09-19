//---------------------------------------------------------------------------
// TripleBit
// (c) 2011 Massive Data Management Group @ SCTS & CGCL. 
//     Web site: http://grid.hust.edu.cn/triplebit
//
// This work is licensed under the Creative Commons
// Attribution-Noncommercial-Share Alike 3.0 Unported License. To view a copy
// of this license, visit http://creativecommons.org/licenses/by-nc-sa/3.0/
// or send a letter to Creative Commons, 171 Second Street, Suite 300,
// San Francisco, California, 94105, USA.
//---------------------------------------------------------------------------

#include "RDFQuery.h"
#include "SPARQLLexer.h"
#include "SPARQLParser.h"
#include "QuerySemanticAnalysis.h"
#include "PlanGenerator.h"
#include "TripleBitQuery.h"
#include "TripleBitQueryGraph.h"
#include "TripleBitRepository.h"

#include <sys/time.h>

RDFQuery::RDFQuery(TripleBitQuery* _bitmapQuery, TripleBitRepository* _repo)
{
	this->bitmapQuery = _bitmapQuery;
	repo = _repo;
	this->queryGraph = new TripleBitQueryGraph();
	this->planGen = new PlanGenerator(*repo);
	this->semAnalysis = new QuerySemanticAnalysis(*repo);
}

RDFQuery::~RDFQuery() {
	// TODO Auto-generated destructor stub
	//delete bitmapQuery;
	if(repo != NULL)
		delete repo;
	repo = NULL;
	if(queryGraph != NULL)
		delete queryGraph;
	queryGraph = NULL;
	if(planGen != NULL)
		delete planGen;
	planGen = NULL;
	if(semAnalysis != NULL)
		delete semAnalysis;
	semAnalysis = NULL;
}

Status RDFQuery::Execute(string& queryString, vector<string>& resultSet)
{
	struct timeval start, end;

	gettimeofday(&start,NULL);

	SPARQLLexer *lexer = new SPARQLLexer(queryString);
	SPARQLParser *parser = new SPARQLParser(*lexer);
	try {
		parser->parse();
	} catch (const SPARQLParser::ParserException& e) {
		cerr << "parse error: " << e.message << endl;
		return ERROR;
	}

	queryGraph->Clear();

	if(!this->semAnalysis->transform(*parser,*queryGraph)) {
		return NOT_FOUND;
	}

	if(queryGraph->knownEmpty() == true) {
		cerr<<"Empty result"<<endl;
		return OK;
	}

	//cout<<"----------------------------After transform------------------------------------"<<endl;
	//Print();
	planGen->generatePlan(*queryGraph);
	//cout<<"-----------------------------After Generate Plan-------------------------------"<<endl;
	//Print();
	//t.restart();
	//bitmapQuery->releaseBuffer();
	bitmapQuery->query(queryGraph, resultSet);
	gettimeofday(&end,NULL);
	cerr<<" time elapsed: "<<((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec ) / 1000000.0<<" s"<<endl;
	//cout<<"time elapsed: "<<t.elapsed()<<endl;
	queryGraph->Clear();
	delete lexer;
	delete parser;
	return OK;
}

void RDFQuery::Print()
{
	TripleBitQueryGraph::SubQuery& query = queryGraph->getQuery();
	unsigned int i, size, j;
	size = query.tripleNodes.size();
	cout<<"join triple size: "<<size<<endl;

	vector<TripleBitQueryGraph::TripleNode>& triples = query.tripleNodes;
	for ( i = 0; i < size; i++)
	{
		cout<<i<<" triple: "<<endl;
		cout<<triples[i].constSubject<<" "<<triples[i].subject<<endl;
		cout<<triples[i].constPredicate<<" "<<triples[i].predicate<<endl;
		cout<<triples[i].constObject<<" "<<triples[i].object<<endl;
		cout<<endl;
	}

	size = query.joinVariables.size();
	cout<<"join variable size: "<<size<<endl;
	vector<TripleBitQueryGraph::JoinVariableNodeID>& variables = query.joinVariables;

	for( i = 0 ; i < size; i++)
	{
		cout<<variables[i]<<endl;
	}

	vector<TripleBitQueryGraph::JoinVariableNode>& nodes = query.joinVariableNodes;

	size = nodes.size();
	cout<<"Join variable nodes size: "<<size<<endl;
	for( i = 0; i < size; i++)
	{
		cout<<i <<" vairable node"<<endl;
		cout<<nodes[i].value<<endl;
		for ( j = 0; j < nodes[i].appear_tpnodes.size(); j++ )
		{
			cout<<nodes[i].appear_tpnodes[j].first<<" "<<nodes[i].appear_tpnodes[j].second<<endl;
		}
		cout<<endl;
	}

	size = query.joinVariableEdges.size();
	cout<<"join variable edges size: "<<size<<endl;
	vector<TripleBitQueryGraph::JoinVariableNodesEdge>& edge = query.joinVariableEdges;
	for( i = 0; i < size; i++)
	{
		cout<<i<<" edge"<<endl;
		cout<<"From: "<<edge[i].from<<" To: "<<edge[i].to<<endl;
	}

	cout<<" query type: "<<query.joinGraph<<endl;

	cout<<" root ID: "<<query.rootID<<endl;

	cout<<" query leafs: ";
	size = query.leafNodes.size();
	for( i = 0 ; i < size; i++)
	{
		cout<<query.leafNodes[i]<<" ";
	}

	cout<<endl;

	vector<ID>& projection = queryGraph->getProjection();

	cout<<"variables need to project: "<<endl;
	cout<<"variable count: "<<projection.size()<<endl;
	for( i = 0; i < projection.size(); i++)
	{
		cout<<projection[i]<<endl;
	}
	cout<<endl;
}
