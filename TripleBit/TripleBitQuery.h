#ifndef TRIPLEBITQUERY_H_
#define TRIPLEBITQUERY_H_

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

class BitmapBuffer;
class URITable;
class PredicateTable;
class FindEntityID;
class TripleBitRepository;
class TripleBitQueryGraph;
class EntityIDBuffer;
class HashJoin;

#include "TripleBitQueryGraph.h"
#include "TripleBit.h"
#include "ThreadPool.h"
#include "util/HashJoin.h"
#include "util/SortMergeJoin.h"
#include <boost/timer.hpp>
#include <boost/progress.hpp>

using namespace std;

typedef map<ID,EntityIDBuffer*> EntityIDListType;
typedef map<ID,EntityIDBuffer*>::iterator EntityIDListIterType;

class TripleBitQuery {
private:
	BitmapBuffer* bitmap;
	URITable* UriTable;
	PredicateTable* preTable;
	FindEntityID* entityFinder;

	TripleBitQueryGraph* _queryGraph;

	TripleBitQueryGraph::SubQuery* _query;

	EntityIDListType EntityIDList;
	vector<TripleBitQueryGraph::JoinVariableNodeID> idTreeBFS;
	vector<TripleBitQueryGraph::JoinVariableNodeID> leafNode;
	vector<TripleBitQueryGraph::JoinVariableNodeID> varVec;

	/// used to get the results;
	vector<int> varPos;
	vector<int> keyPos;
	vector<int> resultPos;
	vector<int>	verifyPos;
	vector<ID> resultVec;
	vector<size_t> bufPreIndexs;
	bool needselect;

	HashJoin hashJoin;
	SortMergeJoin mergeJoin;

	vector<string>* resultPtr;

	float istringtime;
    unsigned resultnum;

public:
	TripleBitQuery(TripleBitRepository& repo);
	virtual ~TripleBitQuery();
	Status query(TripleBitQueryGraph* queryGraph, vector<string>& resultSet);
	void   releaseBuffer();
private:
	Status findEntitiesAndJoin(TripleBitQueryGraph::JoinVariableNodeID id,
			vector< pair < TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes, bool firstTime);
	Status findEntitiesAndJoinFirstTime(vector< pair < TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes,
			ID tripleID, map<ID,bool>& firstInsertFlag, TripleBitQueryGraph::JoinVariableNodeID id);
	Status modifyEntitiesAndJoin(vector< pair < TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes,
			ID tripleID, TripleBitQueryGraph::JoinVariableNodeID id);
	Status getTripleNodeByID(TripleBitQueryGraph::TripleNode*& triple, TripleBitQueryGraph::TripleNodeID nodeID);
	EntityType getDimInTriple(vector< pair < TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes,
			TripleBitQueryGraph::TripleNodeID tripleID);
	Status getVariableNodeByID(TripleBitQueryGraph::JoinVariableNode*& node, TripleBitQueryGraph::JoinVariableNodeID id);
	int	   getVariableCount(TripleBitQueryGraph::TripleNodeID id);
	int	   getVariableCount(TripleBitQueryGraph::TripleNode* triple);
	Status singleVariableJoin();
	Status acyclicJoin();
	Status cyclicJoin();
	bool   nodeIsLeaf(TripleBitQueryGraph::JoinVariableNodeID varID);
	int    getVariablePos(EntityType type, TripleBitQueryGraph::TripleNode* triple);
	Status findEntityIDByTriple( TripleBitQueryGraph::TripleNode * triple, EntityIDBuffer* buffer, ID minID, ID maxID,unsigned maxNum=INT_MAX);
	Status findEntityIDByKnowBuffer( TripleBitQueryGraph::TripleNode * triple, EntityIDBuffer* buffer, EntityIDBuffer* knowBuffer,EntityType knowElement);
	int    getVariablePos(TripleBitQueryGraph::JoinVariableNodeID id, TripleBitQueryGraph::TripleNodeID tripleID);
	int	   getVariablePos(TripleBitQueryGraph::JoinVariableNodeID id, TripleBitQueryGraph::TripleNode* triple);

	bool getResult(ID key, std::vector<EntityIDBuffer* >& bufferlist, size_t buf_index);
	bool getResult_with_flags(ID key, std::vector<EntityIDBuffer*>& bufferlist, size_t buflist_index, vector<unsigned> flags);
	void getResult_join(ID key, std::vector<EntityIDBuffer*>& buffetlist, size_t buf_index);
	void getResult_join_with_flags(ID key, std::vector<EntityIDBuffer*>& bufferlist, size_t buflist_index, vector<unsigned> flags);
	EntityType getKnowBufferType(TripleBitQueryGraph::TripleNode* node1,TripleBitQueryGraph::TripleNode* node2);

	void clearTimeandCount();
    void printIDtoStringTime();
    void displayAllTriples();
    void onePatternWithThreeVariables();
};
#endif /* TRIPLEBITQUERY_H_ */
