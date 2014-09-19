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

#include "PlanGenerator.h"
#include "TripleBitRepository.h"
#include "ThreadPool.h"

#include <vector>
#include <map>
#include <queue>
using namespace std;

bool isUnused(const TripleBitQueryGraph::SubQuery& query,const TripleBitQueryGraph::TripleNode& node,unsigned val)
// Check if a variable is unused outside its primary pattern
{
	for (vector<TripleBitQueryGraph::Filter>::const_iterator iter=query.filters.begin(),limit=query.filters.end();iter!=limit;++iter)
		if ((*iter).id==val)
			return false;
	for (vector<TripleBitQueryGraph::TripleNode>::const_iterator iter=query.tripleNodes.begin(),limit=query.tripleNodes.end();iter!=limit;++iter) {
		const TripleBitQueryGraph::TripleNode& n=*iter;
		if ((&n)==(&node))
			continue;
		if ((!n.constSubject)&&(val==n.subject)) return false;
		if ((!n.constPredicate)&&(val==n.predicate)) return false;
		if ((!n.constObject)&&(val==n.object)) return false;
	}
	for (vector<TripleBitQueryGraph::SubQuery>::const_iterator iter=query.optional.begin(),limit=query.optional.end();iter!=limit;++iter)
		if (!isUnused(*iter,node,val))
			return false;
	for (vector<vector<TripleBitQueryGraph::SubQuery> >::const_iterator iter=query.unions.begin(),limit=query.unions.end();iter!=limit;++iter)
		for (vector<TripleBitQueryGraph::SubQuery>::const_iterator iter2=(*iter).begin(),limit2=(*iter).end();iter2!=limit2;++iter2)
			if (!isUnused(*iter2,node,val))
				return false;
	return true;
}

static bool isUnused(const TripleBitQueryGraph& graph, const TripleBitQueryGraph::TripleNode& node, unsigned val)
{
	// check if a variable is unused outside its primary pattern.
	for ( TripleBitQueryGraph::projection_iterator begin = graph.projectionBegin(), limit = graph.projectionEnd(); begin != limit; begin++ )
	{
		if ( *begin == val)
			return false;
	}

	return isUnused(graph.getQuery(), node, val);
}

PlanGenerator* PlanGenerator::self = NULL;

PlanGenerator::PlanGenerator(IRepository& _repo):repo(_repo) {
	// TODO Auto-generated constructor stub
	self = this;
}

PlanGenerator::~PlanGenerator() {
	// TODO Auto-generated destructor stub
}

Status PlanGenerator::generatePlan(TripleBitQueryGraph& _graph)
{
	TripleBitQueryGraph::SubQuery& _query = _graph.getQuery();
	graph = &_graph;
	query = &_query;
	TripleBitQueryGraph::JoinVariableNode::JoinType joinType;
	map<TripleBitQueryGraph::JoinVariableNodeID,int> selectivityMap;
	bool setted = false;
	vector<TripleBitQueryGraph::JoinVariableNode>::iterator joinVariableIter = query->joinVariableNodes.begin();
	int selectivity = INT_MAX;
	int minSeleID;

	// generate the selectivity for patterns and join variables;
	for(vector<TripleBitQueryGraph::TripleNode>::iterator iter = query->tripleNodes.begin(), limit = query->tripleNodes.end(); iter != limit; iter++) {
	//	CThreadPool::getInstance().AddTask(boost::bind(&PlanGenerator::getSelectivity, this, iter->tripleNodeID));
		PlanGenerator::getSelectivity(iter->tripleNodeID);
		//PlanGenerator::getSelectivity(iter);
	}
//	CThreadPool::getInstance().Wait();

	for(; joinVariableIter != query->joinVariableNodes.end(); joinVariableIter++)
	{
		generateSelectivity(*joinVariableIter,selectivityMap);
	}

	joinVariableIter = query->joinVariableNodes.begin();

	for(; joinVariableIter != query->joinVariableNodes.end(); joinVariableIter++)
	{
		joinType = getJoinType(*joinVariableIter);
		if(joinType == TripleBitQueryGraph::JoinVariableNode::SP || joinType == TripleBitQueryGraph::JoinVariableNode::OP){
			query->rootID = joinVariableIter->value;
			setted = true;
			break;
		}
//		cout<<"join var:" << joinVariableIter->value << "  select:" << selectivityMap[joinVariableIter->value] << endl;
		if(selectivityMap[joinVariableIter->value] < selectivity){
			selectivity = selectivityMap[joinVariableIter->value];
			minSeleID = joinVariableIter->value;
		}
	}

	// if the root id has not been set, the set the root node min selectivity node.
	if(setted == false){
		query->rootID = minSeleID;
	}

	bfsTraverseVariableNode();

	/// generate the patterns' scan operator.
	vector<bool> tnodes;
	tnodes.resize(query->tripleNodes.size() + 1, false);
	//cout<<"joinVariables.size() = "<<query->joinVariables.size()<<endl;
	for(size_t i = 0; i != query->joinVariables.size(); i++) {
		size_t j, k;
		for(j = 0; j != query->joinVariableNodes.size(); j++) {
			if(query->joinVariableNodes[j].value == query->joinVariables[i])
				break;
		}
		TripleBitQueryGraph::JoinVariableNodeID varid = query->joinVariables[i];
		TripleBitQueryGraph::JoinVariableNode& jnode = query->joinVariableNodes[j];
		for(size_t j = 0; j != jnode.appear_tpnodes.size(); j++) {
			TripleBitQueryGraph::TripleNodeID tid = jnode.appear_tpnodes[j].first;
			if(tnodes[tid] == true)
				continue;
			tnodes[tid] = true;
			for(k = 0; k != query->tripleNodes.size(); k++) {
				if(query->tripleNodes[k].tripleNodeID == tid)
					break;
			}
			TripleBitQueryGraph::TripleNode& tnode = query->tripleNodes[k];
			generateScanOperator(tnode, varid);
		}
	}

	query->selectivityMap.clear();
	map<TripleBitQueryGraph::JoinVariableNodeID,int>::iterator iter = selectivityMap.begin();
	for(; iter != selectivityMap.end(); iter++)
	{
		query->selectivityMap[iter->first] = iter->second;
	}
	return OK;
}

Status PlanGenerator::generateScanOperator(TripleBitQueryGraph::TripleNode& node, TripleBitQueryGraph::JoinVariableNodeID varID)
{
	bool subjectUnused, predicateUnused, objectUnused;

	subjectUnused = (!node.constSubject) && isUnused(*graph, node, node.subject);
	predicateUnused = (!node.constPredicate) && isUnused(*graph, node, node.predicate);
	objectUnused = (!node.constObject) && isUnused(*graph, node, node.object);

	unsigned unusedSum = subjectUnused + predicateUnused + objectUnused;
	unsigned variableCnt = (!node.constObject) + (!node.constPredicate) + (!node.constSubject);

	if (variableCnt == unusedSum) {
		node.scanOperation = TripleBitQueryGraph::TripleNode::NOOP;
		return OK;
	}

	if (unusedSum == 3) {
		// the query pattern content is all variables; but they are not used in other patterns
	} else if (unusedSum == 2) {
		if (!subjectUnused)
			node.scanOperation = TripleBitQueryGraph::TripleNode::FINDS;
		if (!predicateUnused)
			node.scanOperation = TripleBitQueryGraph::TripleNode::FINDP;
		if (!objectUnused)
			node.scanOperation = TripleBitQueryGraph::TripleNode::FINDO;
	} else if (unusedSum == 1) {
		if (subjectUnused) {
			if (node.constObject)
				node.scanOperation = TripleBitQueryGraph::TripleNode::FINDPBYO;
			else if(node.constPredicate)
				node.scanOperation = TripleBitQueryGraph::TripleNode::FINDOBYP;
			else{
				if(node.object == varID)
					node.scanOperation = TripleBitQueryGraph::TripleNode::FINDOPBYNONE;
				else
					node.scanOperation = TripleBitQueryGraph::TripleNode::FINDPOBYNONE;
			}
		} else if (predicateUnused) {
			if (node.constObject)
				node.scanOperation = TripleBitQueryGraph::TripleNode::FINDSBYO;
			else if(node.constSubject)
				node.scanOperation = TripleBitQueryGraph::TripleNode::FINDOBYS;
			else{
				if(node.subject == varID)
					node.scanOperation = TripleBitQueryGraph::TripleNode::FINDSOBYNONE;
				else
					node.scanOperation = TripleBitQueryGraph::TripleNode::FINDOSBYNONE;
			}
		} else {
			if (node.constPredicate)
				node.scanOperation = TripleBitQueryGraph::TripleNode::FINDSBYP;
			else if(node.constSubject)
				node.scanOperation = TripleBitQueryGraph::TripleNode::FINDPBYS;
			else{
				if(node.subject == varID)
					node.scanOperation = TripleBitQueryGraph::TripleNode::FINDSPBYNONE;
				else
					node.scanOperation = TripleBitQueryGraph::TripleNode::FINDPSBYNONE;
			}
		}
	} else {
		if (variableCnt == 2) {
			if (node.constSubject) {
				if(node.predicate == varID)
					node.scanOperation = TripleBitQueryGraph::TripleNode::FINDPOBYS;
				else
					node.scanOperation = TripleBitQueryGraph::TripleNode::FINDOPBYS;
			} else if (node.constPredicate) {
				if(node.subject == varID)
					node.scanOperation = TripleBitQueryGraph::TripleNode::FINDSOBYP;
				else
					node.scanOperation = TripleBitQueryGraph::TripleNode::FINDOSBYP;
			} else if (node.constObject) {
				if(node.subject == varID)
					node.scanOperation = TripleBitQueryGraph::TripleNode::FINDSPBYO;
				else
					node.scanOperation = TripleBitQueryGraph::TripleNode::FINDPSBYO;
			}
		} else if (variableCnt == 1) {
			if (!node.constSubject)
				node.scanOperation = TripleBitQueryGraph::TripleNode::FINDSBYPO;
			if (!node.constPredicate)
				node.scanOperation = TripleBitQueryGraph::TripleNode::FINDPBYSO;
			if (!node.constObject)
				node.scanOperation = TripleBitQueryGraph::TripleNode::FINDOBYSP;
		} else {
			/// all are variable
			node.scanOperation = TripleBitQueryGraph::TripleNode::FINDSPO;
		}
	}
	return OK;
}

TripleBitQueryGraph::JoinVariableNode::JoinType PlanGenerator::getJoinType(TripleBitQueryGraph::JoinVariableNode& node)
{
	vector<std::pair<TripleBitQueryGraph::TripleNodeID,
			TripleBitQueryGraph::JoinVariableNode::DimType> >::iterator iter =
			node.appear_tpnodes.begin();
	char dim = 0;
	for (; iter != node.appear_tpnodes.end(); iter++) {
		dim = dim | iter->second;
	}

	switch (dim) {
	case 1: //SUBJECT:
		return TripleBitQueryGraph::JoinVariableNode::SS;
		break;
	case 4: //OBJECT:
		return TripleBitQueryGraph::JoinVariableNode::OO;
		break;
	case 2: //PREDICATE:
		return TripleBitQueryGraph::JoinVariableNode::PP;
		break;
	case 5: //SUBJECT | OBJECT:
		return TripleBitQueryGraph::JoinVariableNode::SO;
		break;
	case 3: //SUBJECT | PREDICATE:
		return TripleBitQueryGraph::JoinVariableNode::SP;
		break;
	case 6: //OBJECT | PREDICATE:
		return TripleBitQueryGraph::JoinVariableNode::OP;
		break;
	}

	return TripleBitQueryGraph::JoinVariableNode::UNKNOWN;

}

PlanGenerator* PlanGenerator::getInstance()
{
	//need to design;
	return self;
}

bool compareSelectivity(pair<TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> left,
						pair<TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> right)
{
	int leftSelectivity = PlanGenerator::getInstance()->getSelectivity(left.first);
	int rightSelectivity = PlanGenerator::getInstance()->getSelectivity(right.first);
	return leftSelectivity < rightSelectivity;
}

Status PlanGenerator::generateSelectivity(TripleBitQueryGraph::JoinVariableNode& node, map<TripleBitQueryGraph::JoinVariableNodeID,int>& selectivityMap)
{
	TripleBitQueryGraph::TripleNodeID selNodeID;
	std::sort(node.appear_tpnodes.begin(), node.appear_tpnodes.end(), compareSelectivity);
	selNodeID = node.appear_tpnodes[0].first;
	vector<TripleBitQueryGraph::TripleNode>::iterator iter = query->tripleNodes.begin(), limit = query->tripleNodes.end();
	for(; iter != limit; iter++) {
		if(iter->tripleNodeID == selNodeID)
			break;
	}

	selectivityMap[node.value] = iter->selectivity;
//	cout <<"node value:" << node.value <<"  select:" << iter->selectivity<< endl;
	return OK;
}

int PlanGenerator::getSelectivity(TripleBitQueryGraph::TripleNodeID& tripleID)
{
	vector<TripleBitQueryGraph::TripleNode>::iterator iter = query->tripleNodes.begin();
	for(; iter != query->tripleNodes.end(); iter++)
	{
		if(iter->tripleNodeID == tripleID)
			break;
	}

	if(iter->selectivity != -1) {
		return iter->selectivity;
	}

	int selectivity = INT_MAX;

	if(iter->constPredicate){
		if ( iter->constObject) {
			if (iter->constSubject)
				selectivity = 1;
			else selectivity = repo.get_object_predicate_count(iter->object, iter->predicate);
		} else {
			if (iter->constSubject)
				selectivity = repo.get_subject_predicate_count(iter->subject, iter->predicate);
			else selectivity = repo.get_predicate_count(iter->predicate);
		}
	} else {
		if ( iter->constObject) {
			if (iter->constSubject)
				selectivity = repo.get_subject_object_count(iter->subject, iter->object);
			else selectivity = repo.get_object_count(iter->object);
		} else {
			if (iter->constSubject)
				selectivity = repo.get_subject_count(iter->subject);
			else selectivity = TripleBitRepository::colNo;
		}
	}
	iter->selectivity = selectivity;
	//cout <<"tripleID:" << tripleID <<"  select:" << selectivity << endl;
	return selectivity;
}

int PlanGenerator::getSelectivity(vector<TripleBitQueryGraph::TripleNode>::iterator iter)
{
	/*vector<TripleBitQueryGraph::TripleNode>::iterator iter = query->tripleNodes.begin();
	for(; iter != query->tripleNodes.end(); iter++)
	{
		if(iter->tripleNodeID == tripleID)
			break;
	}*/

	if(iter->selectivity != -1) {
		return iter->selectivity;
	}

	int selectivity = INT_MAX;

	if(iter->constPredicate){
		if ( iter->constObject) {
			if (iter->constSubject)
				selectivity = 1;
			else selectivity = repo.get_object_predicate_count(iter->object, iter->predicate);
		} else {
			if (iter->constSubject)
				selectivity = repo.get_subject_predicate_count(iter->subject, iter->predicate);
			else selectivity = repo.get_predicate_count(iter->predicate);
		}
	} else {
		if ( iter->constObject) {
			if (iter->constSubject)
				selectivity = repo.get_subject_object_count(iter->subject, iter->object);
			else selectivity = repo.get_object_count(iter->object);
		} else {
			if (iter->constSubject)
				selectivity = repo.get_subject_count(iter->subject);
			else selectivity = TripleBitRepository::colNo;
		}
	}
	iter->selectivity = selectivity;
	//cout <<"tripleID:" << tripleID <<"  select:" << selectivity << endl;
	return selectivity;
}

Status PlanGenerator::bfsTraverseVariableNode()
{
	vector<bool> visited;
	vector<TripleBitQueryGraph::JoinVariableNodeID> temp;
	vector<TripleBitQueryGraph::JoinVariableNodeID> leafNode;
	queue<TripleBitQueryGraph::JoinVariableNodeID> idQueue;
	queue<TripleBitQueryGraph::JoinVariableNodeID> parentQueue;
	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator iter;
	vector<TripleBitQueryGraph::JoinVariableNodeID> adjNode;
	TripleBitQueryGraph::JoinVariableNodeID tempID, parentID;
	Status s;

	visited.resize(query->joinVariableNodes.size());
	unsigned int i, size = query->joinVariables.size(), j;
	int pos;
	bool flag;

	// find the root node's position.
	iter = find(query->joinVariables.begin(), query->joinVariables.end(),
			query->rootID);
	// exchange the root node's id with the first node's id.
	tempID = *iter;
	*iter = query->joinVariables[0];
	query->joinVariables[0] = tempID;

	query->joinGraph = TripleBitQueryGraph::ACYCLIC;

	// breadth first traverse the join variable.
	for (i = 0; i != size; i++)
		visited[i] = false;

	for (i = 0; i != size; i++) {
		if (!visited[i]) {
			visited[i] = true;
			temp.push_back(query->joinVariables[i]);
			idQueue.push(query->joinVariables[i]);
			parentQueue.push(0);
			while (!idQueue.empty()) {
				flag = false;
				tempID = idQueue.front();
				idQueue.pop();

				parentID = parentQueue.front();
				parentQueue.pop();

				// get the adj. variable nodes
				s = getAdjVariableByID(tempID, adjNode);
				if (s != OK)
					continue;

				for (j = 0; j < adjNode.size(); j++) {
					// get the variable position in the vector.
					iter = find(query->joinVariables.begin(),
							query->joinVariables.end(), adjNode[j]);
					pos = iter - query->joinVariables.begin();
					if (!visited[pos]) {
						visited[pos] = true;
						temp.push_back(*iter);
						idQueue.push(*iter);
						flag = true;
						parentQueue.push(tempID);
					} else {
						if (adjNode[j] != parentID) {
							query->joinGraph = TripleBitQueryGraph::CYCLIC;
						}
					}
				}

				//
				if (flag == false) {
					leafNode.push_back(tempID);
				}
			}
		}
	}

//	query->joinVariables.assign(temp.begin(),temp.end());
	query->leafNodes.assign(leafNode.begin(), leafNode.end());
	return OK;
}

Status PlanGenerator::getAdjVariableByID(TripleBitQueryGraph::JoinVariableNodeID id, vector<TripleBitQueryGraph::JoinVariableNodeID>& nodes)
{
	nodes.clear();
	vector<TripleBitQueryGraph::JoinVariableNodesEdge>::iterator iter = query->joinVariableEdges.begin();
	for (; iter != query->joinVariableEdges.end(); iter++) {
		if (iter->to == id) {
			nodes.push_back(iter->from);
		}
		if(iter->from == id){
			nodes.push_back(iter->to);
		}
	}

	if (nodes.size() == 0)
		return ERROR;

	return OK;
}
