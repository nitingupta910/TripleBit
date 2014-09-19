#ifndef PLANGENERATOR_H_
#define PLANGENERATOR_H_

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

class IRepository;
class TripleBitQueryGraph;

#include "IRepository.h"
#include "TripleBitQueryGraph.h"
#include "TripleBit.h"

class PlanGenerator {
private:
	IRepository& repo;
	TripleBitQueryGraph::SubQuery* query;
	TripleBitQueryGraph* graph;
	static PlanGenerator* self;
public:
	PlanGenerator(IRepository& _repo);
	Status generatePlan(TripleBitQueryGraph& _graph);
	virtual ~PlanGenerator();
	static PlanGenerator* getInstance();
	int	getSelectivity(TripleBitQueryGraph::TripleNodeID& tripleID);
	int getSelectivity(vector<TripleBitQueryGraph::TripleNode>::iterator iter);
private:
	/// Generate the scan operator for the query pattern.
	Status generateScanOperator(TripleBitQueryGraph::TripleNode& node, TripleBitQueryGraph::JoinVariableNodeID varID);
	Status generateSelectivity(TripleBitQueryGraph::JoinVariableNode& node, map<TripleBitQueryGraph::JoinVariableNodeID,int>& selectivityMap);
	TripleBitQueryGraph::JoinVariableNode::JoinType getJoinType(TripleBitQueryGraph::JoinVariableNode& node);
	Status bfsTraverseVariableNode();
	Status getAdjVariableByID(TripleBitQueryGraph::JoinVariableNodeID id, vector<TripleBitQueryGraph::JoinVariableNodeID>& nodes);
};

#endif /* PLANGENERATOR_H_ */
