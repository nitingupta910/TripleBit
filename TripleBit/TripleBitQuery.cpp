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

#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "TripleBitQuery.h"
#include "TripleBitRepository.h"
#include "URITable.h"
#include "PredicateTable.h"
#include "util/FindEntityID.h"
#include "TripleBitQueryGraph.h"
#include "EntityIDBuffer.h"
#include "util/BufferManager.h"

#include <algorithm>
#include <math.h>
#include <sys/time.h>
#include <set>

//#define PRINT_BUFFERSIZE 1
#define PRINT_RESULT 1
//#define TEST_IDtoStr_TIME 2
using namespace std;

TripleBitQuery::TripleBitQuery(TripleBitRepository& repo) {
	// TODO Auto-generated constructor stub
	bitmap = repo.getBitmapBuffer();
	UriTable = repo.getURITable();
	preTable = repo.getPredicateTable();

	entityFinder = new FindEntityID(&repo);
}

TripleBitQuery::~TripleBitQuery() {
	// TODO Auto-generated destructor stub
	if(entityFinder != NULL)
		delete entityFinder;
	entityFinder = NULL;

	size_t i;

	for( i = 0; i < EntityIDList.size(); i++)
	{
		if(EntityIDList[i] != NULL)
			BufferManager::getInstance()->freeBuffer(EntityIDList[i]);
	}

	EntityIDList.clear();
}

void TripleBitQuery::releaseBuffer()
{
	idTreeBFS.clear();
	leafNode.clear();
	varVec.clear();

	EntityIDListIterType iter = EntityIDList.begin();

	for( ; iter != EntityIDList.end(); iter++)
	{
		if(iter->second != NULL)
			BufferManager::getInstance()->freeBuffer(iter->second);
	}

	BufferManager::getInstance()->reserveBuffer();
	EntityIDList.clear();
}

void TripleBitQuery::displayAllTriples()
{
	ID predicateCount = preTable->getPredicateCount();
	ID tripleCount = 0;
	for(ID pid = 1;pid < predicateCount;pid++)tripleCount += bitmap->getChunkManager(pid, 0)->getTripleCount();
	cout<<"The total tripleCount is : "<<tripleCount<<endl<<"Do you really want to display all (Y / N) ?"<<endl;
	char displayFlag;
	cin>>displayFlag;

	if((displayFlag == 'y') || (displayFlag == 'Y')){
		EntityIDBuffer *buffer = new EntityIDBuffer();
		string URI1,URI2;
		for(ID pid = 1;pid < predicateCount;pid++){
			buffer->empty();
			entityFinder->findSubjectIDAndObjectIDByPredicate(pid, buffer);
			size_t bufSize = buffer->getSize();
			if(bufSize == 0)continue;
			ID *p = buffer->getBuffer();
			preTable->getPredicateByID(URI1, pid);

			for (size_t i = 0; i < bufSize; i++) {
				UriTable->getURIById(URI2, p[i * 2]);
				cout << URI2 << " ";
				cout << URI1 << " ";
				UriTable->getURIById(URI2, p[i * 2 + 1]);
				cout << URI2 << endl;
			}
		}

		delete buffer;
	}
}

void TripleBitQuery::onePatternWithThreeVariables()
{
	if(_query->tripleNodes[0].scanOperation == TripleBitQueryGraph::TripleNode::FINDSPO){
		displayAllTriples();
	}else if(_query->tripleNodes[0].scanOperation == TripleBitQueryGraph::TripleNode::FINDSPBYNONE){
		if(_queryGraph->getProjection().front() == _query->tripleNodes[0].subject){
			unsigned predicateCount = preTable->getPredicateCount();
			EntityIDBuffer *buffer = new EntityIDBuffer();
			string URI1,URI2;
			for(ID pid = 1;pid < predicateCount;pid++){
				buffer->empty();
				entityFinder->findSubjectIDByPredicate(pid, buffer, 0, UINT_MAX);
				size_t bufSize = buffer->getSize();
				ID *p = buffer->getBuffer();
				preTable->getPredicateByID(URI2, pid);
				for (size_t i = 0; i < bufSize; i++) {
					UriTable->getURIById(URI1, p[i]);
					cout<<URI1<<" "<<URI2<<endl;
				}
			}
			delete buffer;
		}else{
			unsigned predicateCount = preTable->getPredicateCount();
			EntityIDBuffer *buffer = new EntityIDBuffer();
			string URI1,URI2;
			for(ID pid = 1;pid < predicateCount;pid++){
				buffer->empty();
				entityFinder->findSubjectIDByPredicate(pid, buffer, 0, UINT_MAX);
				size_t bufSize = buffer->getSize();
				ID *p = buffer->getBuffer();
				preTable->getPredicateByID(URI2, pid);
				for (size_t i = 0; i < bufSize; i++) {
					UriTable->getURIById(URI1, p[i]);
					cout<<URI2<<" "<<URI1<<endl;
				}
			}
			delete buffer;
		}
	}else if(_query->tripleNodes[0].scanOperation == TripleBitQueryGraph::TripleNode::FINDPOBYNONE){
		if(_queryGraph->getProjection().front() == _query->tripleNodes[0].predicate){
			unsigned predicateCount = preTable->getPredicateCount();
			EntityIDBuffer *buffer = new EntityIDBuffer();
			string URI1,URI2;
			for(ID pid = 1;pid < predicateCount;pid++){
				buffer->empty();
				entityFinder->findObjectIDByPredicate(pid, buffer, 0, UINT_MAX);
				size_t bufSize = buffer->getSize();
				ID *p = buffer->getBuffer();
				preTable->getPredicateByID(URI2, pid);
				for (size_t i = 0; i < bufSize; i++) {
					UriTable->getURIById(URI1, p[i]);
					cout<<URI2<<" "<<URI1<<endl;
				}
			}
			delete buffer;
		}else{
			unsigned predicateCount = preTable->getPredicateCount();
			EntityIDBuffer *buffer = new EntityIDBuffer();
			string URI1,URI2;
			for(ID pid = 1;pid < predicateCount;pid++){
				buffer->empty();
				entityFinder->findObjectIDByPredicate(pid, buffer, 0, UINT_MAX);
				size_t bufSize = buffer->getSize();
				ID *p = buffer->getBuffer();
				preTable->getPredicateByID(URI2, pid);
				for (size_t i = 0; i < bufSize; i++) {
					UriTable->getURIById(URI1, p[i]);
					cout<<URI1<<" "<<URI2<<endl;
				}
			}
			delete buffer;
		}
	}else if(_query->tripleNodes[0].scanOperation == TripleBitQueryGraph::TripleNode::FINDSOBYNONE){
		if(_queryGraph->getProjection().front() == _query->tripleNodes[0].subject){
			unsigned predicateCount = preTable->getPredicateCount();
			EntityIDBuffer *buffer = new EntityIDBuffer();
			string URI1,URI2;
			for(ID pid = 1;pid < predicateCount;pid++){
				buffer->empty();
				entityFinder->findSubjectIDAndObjectIDByPredicate(pid, buffer, 0, UINT_MAX);
				size_t bufSize = buffer->getSize();
				ID *p = buffer->getBuffer();
				for (size_t i = 0; i < bufSize; i++) {
					UriTable->getURIById(URI1, p[i*2]);
					UriTable->getURIById(URI2, p[i*2+1]);
					cout<<URI1<<" "<<URI2<<endl;
				}
			}
			delete buffer;
		}else{
			unsigned predicateCount = preTable->getPredicateCount();
			EntityIDBuffer *buffer = new EntityIDBuffer();
			string URI1,URI2;
			for(ID pid = 1;pid < predicateCount;pid++){
				buffer->empty();
				entityFinder->findObjectIDAndSubjectIDByPredicate(pid, buffer, 0, UINT_MAX);
				size_t bufSize = buffer->getSize();
				ID *p = buffer->getBuffer();
				for (size_t i = 0; i < bufSize; i++) {
					UriTable->getURIById(URI1, p[i*2]);
					UriTable->getURIById(URI2, p[i*2+1]);
					cout<<URI1<<" "<<URI2<<endl;
				}
			}
			delete buffer;
		}
	}else if(_query->tripleNodes[0].scanOperation == TripleBitQueryGraph::TripleNode::FINDS){
		EntityIDBuffer *buffer = new EntityIDBuffer();
		entityFinder->findSubject(buffer, 0, INT_MAX);
		size_t bufSize = buffer->getSize();
		ID *p = buffer->getBuffer();
		string URI;
		for(size_t i = 0;i < bufSize;i++){
			UriTable->getURIById(URI, p[i]);
			cout<<URI<<endl;
		}
		delete buffer;
	}else if(_query->tripleNodes[0].scanOperation == TripleBitQueryGraph::TripleNode::FINDP){
		unsigned predicateCount = preTable->getPredicateCount();
		string URI;
		for(ID pid = 1;pid < predicateCount;pid++){
			preTable->getPredicateByID(URI, pid);
			cout<<URI<<endl;
		}
	}else if(_query->tripleNodes[0].scanOperation == TripleBitQueryGraph::TripleNode::FINDO){
		EntityIDBuffer *buffer = new EntityIDBuffer();
		entityFinder->findObject(buffer, 0, INT_MAX);
		size_t bufSize = buffer->getSize();
		ID *p = buffer->getBuffer();
		string URI;
		for(size_t i = 0;i < bufSize;i++){
			UriTable->getURIById(URI, p[i]);
			cout<<URI<<endl;
		}
		delete buffer;
	}
}

Status TripleBitQuery::query(TripleBitQueryGraph* queryGraph, vector<string>& resultSet)
{
	this->_queryGraph = queryGraph;
	this->_query = &(queryGraph->getQuery());
	this->resultPtr = &resultSet;
	clearTimeandCount();

	if(_query->tripleNodes.size() == 1 && _query->joinVariables.size() == 3){
		//the query has only one pattern with three variables
		onePatternWithThreeVariables();
	}
	else if(_query->joinVariables.size() == 1){
		singleVariableJoin();
	}else {
		if(_query->joinGraph == TripleBitQueryGraph::ACYCLIC){
			acyclicJoin();
		}else if(_query->joinGraph == TripleBitQueryGraph::CYCLIC){
			cyclicJoin();
		}
	}
#ifdef TEST_TIME
	entityFinder->printTime();
#endif
#ifdef TEST_IDtoStr_TIME
    printIDtoStringTime();
#endif
	return OK;
}

static void generateProjectionBitVector(uint& bitv, std::vector<ID>& project)
{
	bitv = 0;
	for(size_t i = 0; i != project.size(); i++) {
		bitv |= 1 << project[i];
	}
}

static void generateTripleNodeBitVector(uint& bitv, TripleBitQueryGraph::TripleNode& node)
{
	bitv = 0;
	if(!node.constSubject)
		bitv = bitv | (1 << node.subject);
	if(!node.constPredicate)
		bitv = bitv | (1 << node.predicate);
	if(!node.constObject)
		bitv = bitv | (1 << node.object);
}

static size_t countOneBits(uint bitv)
{
	size_t count = 0;
	while(bitv) {
		bitv = bitv & (bitv - 1);
		count++;
	}

	return count;
}

static ID bitVtoID(uint bitv)
{
	uint mask = 0x1;
	ID count = 0;
	while(true) {
		if((mask & bitv) == mask)
			break;
		bitv = bitv>>1;
		count++;
	}

	return count;
}

static int insertVarID(ID key, std::vector<ID>& idvec, TripleBitQueryGraph::TripleNode& node, ID& sortID)
{
	int ret = 0;
	switch(node.scanOperation) {
	case TripleBitQueryGraph::TripleNode::FINDO:
	case TripleBitQueryGraph::TripleNode::FINDOBYP:
	case TripleBitQueryGraph::TripleNode::FINDOBYS:
	case TripleBitQueryGraph::TripleNode::FINDOBYSP:
		if(key != node.object ) idvec.push_back(node.object);
		sortID = node.object;
		break;
	case TripleBitQueryGraph::TripleNode::FINDOPBYS:
	case TripleBitQueryGraph::TripleNode::FINDOPBYNONE:
		if(key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 0;
		}
		if(key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 1;
		}
		break;
	case TripleBitQueryGraph::TripleNode::FINDOSBYP:
	case TripleBitQueryGraph::TripleNode::FINDOSBYNONE:
		if(key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 0;
		}
		if(key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 1;
		}
		break;
	case TripleBitQueryGraph::TripleNode::FINDP:
	case TripleBitQueryGraph::TripleNode::FINDPBYO:
	case TripleBitQueryGraph::TripleNode::FINDPBYS:
	case TripleBitQueryGraph::TripleNode::FINDPBYSO:
		if(key != node.predicate) idvec.push_back(node.predicate);
		sortID = node.predicate;
		break;
	case TripleBitQueryGraph::TripleNode::FINDPOBYS:
	case TripleBitQueryGraph::TripleNode::FINDPOBYNONE:
		if(key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 0;
		}

		if(key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 1;
		}
		break;
	case TripleBitQueryGraph::TripleNode::FINDPSBYO:
	case TripleBitQueryGraph::TripleNode::FINDPSBYNONE:
		if(key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 0;
		}

		if(key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 1;
		}
		break;
	case TripleBitQueryGraph::TripleNode::FINDS:
	case TripleBitQueryGraph::TripleNode::FINDSBYO:
	case TripleBitQueryGraph::TripleNode::FINDSBYP:
	case TripleBitQueryGraph::TripleNode::FINDSBYPO:
		if(key != node.subject) idvec.push_back(node.subject);
		sortID = node.subject;
		break;
	case TripleBitQueryGraph::TripleNode::FINDSOBYP:
	case TripleBitQueryGraph::TripleNode::FINDSOBYNONE:
		if(key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 0;
		}

		if(key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 1;
		}
		break;
	case TripleBitQueryGraph::TripleNode::FINDSPBYO:
	case TripleBitQueryGraph::TripleNode::FINDSPBYNONE:
		if(key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 0;
		}
		if(key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 1;
		}
		break;
	case TripleBitQueryGraph::TripleNode::FINDSPO:
		if(key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 0;
		}
		if(key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 1;
		}
		if(key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 2;
		}
		break;
	case TripleBitQueryGraph::TripleNode::NOOP:
		break;
	}

	return ret;
}

static void generateResultPos(std::vector<ID>& idVec, std::vector<ID>& projection, std::vector<int>& resultPos)
{
	resultPos.clear();
	std::vector<ID>::iterator iter;
	for(size_t i = 0; i != projection.size(); i++) {
		iter = find(idVec.begin(), idVec.end(), projection[i]);
		resultPos.push_back(iter - idVec.begin());
	}
}

static void generateVerifyPos(std::vector<ID>& idVec, std::vector<int>& verifyPos)
{
	verifyPos.clear();
	size_t i, j;
	size_t size = idVec.size();
	for(i = 0; i != size; i++) {
		for(j = i + 1; j != size; j++) {
			if(idVec[i] == idVec[j]) {
				verifyPos.push_back(i);
				verifyPos.push_back(j);
				return;
			}
		}
	}
}

Status TripleBitQuery::singleVariableJoin()
{
	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator joinNodeIter = _query->joinVariables.begin();
	vector<pair<TripleBitQueryGraph::TripleNodeID,TripleBitQueryGraph::JoinVariableNode::DimType> >::iterator nodePatternIter;

	TripleBitQueryGraph::JoinVariableNode* node;
	EntityIDBuffer *buffer;
	TripleBitQueryGraph::TripleNode* triple ,* lasttriple;

	//TODO Initialize the first query pattern's triple of the pattern group which has the same variable;
	getVariableNodeByID(node, *joinNodeIter);
	nodePatternIter = node->appear_tpnodes.begin();

	buffer = BufferManager::getInstance()->getNewBuffer();
	getTripleNodeByID(triple, nodePatternIter->first);
	findEntityIDByTriple(triple, buffer, 0, INT_MAX);
	if(buffer->getSize() == 0) {
#ifdef PRINT_RESULT
        cout<<"empty result"<<endl;
#else
		resultPtr->push_back("-1");
		resultPtr->push_back("Empty Result");
#endif
        return OK;
    }

	lasttriple = triple;
	EntityIDList[nodePatternIter->first] = buffer;
	nodePatternIter++;

	EntityIDBuffer* tempBuffer;
	ID minID, maxID;
	int varCount;

	if(_queryGraph->getProjection().size() == 1) {
		tempBuffer = BufferManager::getInstance()->getNewBuffer();
		for( ; nodePatternIter != node->appear_tpnodes.end(); nodePatternIter++ )
		{
			tempBuffer->empty();
			buffer->getMinMax(minID, maxID);
			getTripleNodeByID(triple, nodePatternIter->first);
			varCount = this->getVariableCount(triple);

			if(varCount == 3){
				EntityType knowElement;
				ID key = _query->joinVariables.front();
				if(key == triple->subject)knowElement = SUBJECT;
				else if(key == triple->object)knowElement = OBJECT;
				else knowElement = PREDICATE;
				findEntityIDByKnowBuffer(triple,tempBuffer,buffer,knowElement);
			}else{
				if(findEntityIDByTriple(triple,tempBuffer, minID, maxID,buffer->getSize()) == TOO_MUCH){
					EntityType knowElement = this->getKnowBufferType(lasttriple,triple);
					lasttriple = triple;
					findEntityIDByKnowBuffer(triple, tempBuffer,buffer, knowElement);
				}
			}

			mergeJoin.Join(buffer,tempBuffer,1,1,false);
			if(buffer->getSize() == 0) {
#ifdef PRINT_RESULT
				cout<<"empty result"<<endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
		BufferManager::getInstance()->freeBuffer(tempBuffer);
	} else {
		for( ; nodePatternIter != node->appear_tpnodes.end(); nodePatternIter++ ) {
			tempBuffer = BufferManager::getInstance()->getNewBuffer();
			tempBuffer->empty();
			buffer->getMinMax(minID, maxID);
			getTripleNodeByID(triple, nodePatternIter->first);
			varCount = this->getVariableCount(triple);

			if(varCount == 3){
				EntityType knowElement;
				ID key = _query->joinVariables.front();
				if(key == triple->subject)knowElement = SUBJECT;
				else if(key == triple->object)knowElement = OBJECT;
				else knowElement = PREDICATE;
				findEntityIDByKnowBuffer(triple,tempBuffer,buffer,knowElement);
				mergeJoin.Join(buffer,tempBuffer,1,1,false);
			}else{
				if (findEntityIDByTriple(triple, tempBuffer, minID, maxID,buffer->getSize()) == TOO_MUCH) {
					EntityType knowElement = this->getKnowBufferType(lasttriple,triple);
					lasttriple = triple;
					findEntityIDByKnowBuffer(triple, tempBuffer, buffer,knowElement);
					mergeJoin.Join(buffer,tempBuffer,1,1,false);
				} else	if(tempBuffer->getIDCount() == 1)
					mergeJoin.Join(buffer,tempBuffer,1,1,false);
				else
					mergeJoin.Join(buffer,tempBuffer,1,1,true);
			}
			if(buffer->getSize() == 0) {
#ifdef PRINT_RESULT
				cout<<"empty result"<<endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
			EntityIDList[nodePatternIter->first] = tempBuffer;
		}
	}
	//TODO materialization the result;
	size_t i;
	size_t size = buffer->getSize();
	std::string URI;
	ID* p = buffer->getBuffer();
	size_t projectNo = _queryGraph->getProjection().size();
	keyPos.clear();
	resultPos.clear();
	verifyPos.clear();
	
#ifndef PRINT_RESULT
	char temp[2];
	sprintf(temp, "%d", projectNo);
	resultPtr->push_back(temp);
#endif

	if(projectNo == 1) {
#ifdef TEST_IDtoStr_TIME
        struct timeval start_time, end_time;
        gettimeofday(&start_time, NULL);
#endif
        vector<unsigned> flags = _queryGraph->getPredicateFlag();
		if(flags[0] == 0){
			for (i = 0; i < size; i++) {
				if (UriTable->getURIById(URI, p[i]) == OK)
#ifdef PRINT_RESULT
					cout << URI << endl;
#else
					resultPtr->push_back(URI);
#endif
				else
#ifdef PRINT_RESULT
					cout << p[i] << " " << "not found" << endl;
#else
					resultPtr->push_back("NULL");
#endif
			}
		}
		else{
			for (i = 0; i < size; i++) {
				if (preTable->getPredicateByID(URI, p[i]) == OK)
#ifdef PRINT_RESULT
					cout << URI << endl;
#else
					resultPtr->push_back(URI);
#endif
				else
#ifdef PRINT_RESULT
					cout << p[i] << " " << "not found" << endl;
#else
					resultPtr->push_back("NULL");
#endif
			}
		}
#ifdef TEST_IDtoStr_TIME
        gettimeofday(&end_time, NULL);
        istringtime +=  ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + (end_time.tv_usec - start_time.tv_usec));
        resultnum += size;
#endif
	} else {
		std::vector<EntityIDBuffer*> bufferlist;
		std::vector<ID> resultVar;
		resultVar.resize(0);
		uint projBitV, nodeBitV, resultBitV, tempBitV;
		resultBitV = 0;
		ID sortID;
		int keyp;
		keyPos.clear();
		projBitV= nodeBitV=resultBitV= tempBitV=0;
		generateProjectionBitVector(projBitV, _queryGraph->getProjection());
	//	projBitV |= (1<<*joinNodeIter);
		for( i = 0; i != _query->tripleNodes.size(); i++) {
			// generate the bit vector of query pattern.
			generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
			// get the common bit
			tempBitV = projBitV & nodeBitV;
			if(tempBitV) {
				// the query pattern which contains two or more variables is better.
				if(EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0 || (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1))
					continue;
				bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);
				if(countOneBits(resultBitV) == 0)
					// the first time, last joinKey should be set as UINT_MAX
					keyp = insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
				else {
					keyp = insertVarID(*joinNodeIter, resultVar, _query->tripleNodes[i], sortID);
					keyPos.push_back(keyp);
				}
				resultBitV = resultBitV | nodeBitV;
				// the buffer of query pattern is enough.
				if((resultBitV & projBitV) == projBitV)
					break;
			}
		}

		if(bufferlist.size() > 1){
			generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
			needselect = false;

			EntityIDBuffer* buf = bufferlist[0];
			size_t bufsize = buf->getSize();
			bufPreIndexs.clear();
			bufPreIndexs.resize(bufferlist.size(), 0);
			ID key; int IDCount = buf->getIDCount(); ID* ids = buf->getBuffer(); int sortKey = buf->getSortKey();
			if(_queryGraph->getHasPredicate() == false){
				for(i = 0; i != bufsize; i++) {
					resultVec.resize(0);
					key = ids[i * IDCount + sortKey];
					for(int j = 0; j < IDCount ; j++)
						resultVec.push_back(ids[i * IDCount + j]);
					//cout << "buffer list size:" << bufferlist.size() << endl;
					bool ret = getResult(key, bufferlist, 1);
					if(ret == false) {
						while(i < bufsize && ids[i * IDCount + sortKey] == key) {
							i++;
						}
						i--;
					}
				}
			}else{
				vector<unsigned> flags = _queryGraph->getPredicateFlag();
				for(i = 0; i != bufsize; i++) {
					resultVec.resize(0);
					key = ids[i * IDCount + sortKey];
					for(int j = 0; j < IDCount ; j++)
						resultVec.push_back(ids[i * IDCount + j]);
					//cout << "buffer list size:" << bufferlist.size() << endl;
					bool ret = getResult_with_flags(key, bufferlist, 1, flags);
					if(ret == false) {
						while(i < bufsize && ids[i * IDCount + sortKey] == key) {
							i++;
						}
						i--;
					}
				}
			}
		}else{
			EntityIDBuffer* temp = bufferlist[0];
            size_t total = temp->getSize();
            size_t i;
            int k;
            unsigned IDCount = temp->getIDCount();
            ID * p = temp->getBuffer();
#ifdef PRINT_RESULT
#ifdef TEST_IDtoStr_TIME
            struct timeval start_time, end_time;
            gettimeofday(&start_time, NULL);
#endif
            if(_queryGraph->getHasPredicate() == false){
            	for (i = 0; i != total; i++) {
            		for (k = 0; k < temp->IDCount; k++) {
            			UriTable->getURIById(URI, p[i * IDCount + k]);
#ifdef PRINT_RESULT
            		    cout <<URI <<" " ;
#else
						resultPtr->push_back(URI);
#endif
            		}
#ifdef PRINT_RESULT
            		cout <<endl;
#endif
            	}
            }
            else {
            	vector<unsigned> flags = _queryGraph->getPredicateFlag();
            	for (i = 0; i != total; i++) {
            		for (k = 0; k < temp->IDCount; k++) {
            			if(flags[k] == 0)UriTable->getURIById(URI, p[i * IDCount + k]);
            			else preTable->getPredicateByID(URI, p[i * IDCount + k]);
#ifdef PRINT_RESULT
            	        cout <<URI <<" " ;
#else
						resultPtr->push_back(URI);
#endif
            	    }
#ifdef PRINT_RESULT
            	    cout <<endl;
#endif
            	}
            }
#ifdef TEST_IDtoStr_TIME
            gettimeofday(&end_time, NULL);
            istringtime +=  ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + (end_time.tv_usec - start_time.tv_usec));
            resultnum += total;
#endif
#endif
		}
	}

	return OK;
}

bool TripleBitQuery::getResult(ID key, std::vector<EntityIDBuffer*>& bufferlist, size_t buflist_index) {
	if(buflist_index == bufferlist.size())
		return true;

	EntityIDBuffer* entBuf = bufferlist[buflist_index];
	size_t currentIndex = bufPreIndexs[buflist_index];
	size_t bufsize = entBuf->getSize();
	while(currentIndex < bufsize && (*entBuf)[currentIndex] < key) {
		currentIndex++;
	}
	bufPreIndexs[buflist_index] = currentIndex;
	if(currentIndex >= bufsize || (*entBuf)[currentIndex] > key)
		return false;

	bool ret;
	ID* buf = entBuf->getBuffer();
	int IDCount = entBuf->getIDCount();
	int sortKey = entBuf->getSortKey();

	ret = true;
	size_t resultVecSize = resultVec.size();
	std::string URI;
	while(currentIndex < bufsize && buf[currentIndex * IDCount + sortKey] == key) {
		for(int i = 0; i < IDCount; i++) {
			if(i != sortKey)
				resultVec.push_back(buf[currentIndex * IDCount + i]);
		}
		if(buflist_index == (bufferlist.size() - 1)) {
			if(needselect == true) {
				if(resultVec[verifyPos[0]] != resultVec[verifyPos[1]]) {
					currentIndex++;
					continue;
				}
			}
#ifdef TEST_IDtoStr_TIME
            struct timeval start_time, end_time;
            gettimeofday(&start_time, NULL);
#endif
			for(size_t j = 0; j != resultPos.size(); j++) {
				UriTable->getURIById(URI, resultVec[resultPos[j]]);
#ifdef PRINT_RESULT
				std::cout<<URI<<" ";
#else
				resultPtr->push_back(URI);
#endif
			}
#ifdef TEST_IDtoStr_TIME
            gettimeofday(&end_time, NULL);
            istringtime +=  ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + (end_time.tv_usec - start_time.tv_usec));
            resultnum += resultPos.size();
#endif
#ifdef PRINT_RESULT
			std::cout<<std::endl;
#endif
		} else {
			ret = getResult(resultVec[keyPos[buflist_index]], bufferlist, buflist_index + 1);
			if(ret != true)
				break;
		}

		currentIndex++;
		resultVec.resize(resultVecSize);
	}

	return ret;
}

bool TripleBitQuery::getResult_with_flags(ID key, std::vector<EntityIDBuffer*>& bufferlist, size_t buflist_index, vector<unsigned> flags) {
	if(buflist_index == bufferlist.size())
		return true;

	EntityIDBuffer* entBuf = bufferlist[buflist_index];
	size_t currentIndex = bufPreIndexs[buflist_index];
	size_t bufsize = entBuf->getSize();
	while(currentIndex < bufsize && (*entBuf)[currentIndex] < key) {
		currentIndex++;
	}
	bufPreIndexs[buflist_index] = currentIndex;
	if(currentIndex >= bufsize || (*entBuf)[currentIndex] > key)
		return false;

	bool ret;
	ID* buf = entBuf->getBuffer();
	int IDCount = entBuf->getIDCount();
	int sortKey = entBuf->getSortKey();

	ret = true;
	size_t resultVecSize = resultVec.size();
	std::string URI;
	while(currentIndex < bufsize && buf[currentIndex * IDCount + sortKey] == key) {
		for(int i = 0; i < IDCount; i++) {
			if(i != sortKey)
				resultVec.push_back(buf[currentIndex * IDCount + i]);
		}
		if(buflist_index == (bufferlist.size() - 1)) {
			if(needselect == true) {
				if(resultVec[verifyPos[0]] != resultVec[verifyPos[1]]) {
					currentIndex++;
					continue;
				}
			}
#ifdef TEST_IDtoStr_TIME
            struct timeval start_time, end_time;
            gettimeofday(&start_time, NULL);
#endif
			for(size_t j = 0; j != resultPos.size(); j++) {
				if(flags[j] == 0)UriTable->getURIById(URI, resultVec[resultPos[j]]);
				else preTable->getPredicateByID(URI, resultVec[resultPos[j]]);
#ifdef PRINT_RESULT
				std::cout<<URI<<" ";
#else
				resultPtr->push_back(URI);
#endif
			}
#ifdef TEST_IDtoStr_TIME
            gettimeofday(&end_time, NULL);
            istringtime +=  ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + (end_time.tv_usec - start_time.tv_usec));
            resultnum += resultPos.size();
#endif
#ifdef PRINT_RESULT
			std::cout<<std::endl;
#endif
		} else {
			ret = getResult_with_flags(resultVec[keyPos[buflist_index]], bufferlist, buflist_index + 1, flags);
			if(ret != true)
				break;
		}

		currentIndex++;
		resultVec.resize(resultVecSize);
	}

	return ret;
}

void TripleBitQuery::getResult_join(ID key, std::vector<EntityIDBuffer*>& bufferlist, size_t buflist_index)
{
	if (buflist_index == bufferlist.size())
		return;

	EntityIDBuffer* entBuf = bufferlist[buflist_index];
	size_t currentIndex = entBuf->getEntityIDPos(key);//bufPreIndexs[buflist_index];
	if(currentIndex == size_t(-1))
		return;
	size_t bufsize = entBuf->getSize();

	ID* buf = entBuf->getBuffer();
	int IDCount = entBuf->getIDCount();
	int sortKey = entBuf->getSortKey();

	size_t resultVecSize = resultVec.size();
	std::string URI;
	while (currentIndex < bufsize && buf[currentIndex * IDCount + sortKey] == key) {
		for (int i = 0; i < IDCount; i++) {
			if (i != sortKey)
				resultVec.push_back(buf[currentIndex * IDCount + i]);
		}
		if (buflist_index == (bufferlist.size() - 1)) {
			static vector<ID> lastRes(resultPos.size(),0);
			if(_queryGraph->getDuplicateHandling() == TripleBitQueryGraph::NoDuplicates){
				//compare with the last
				unsigned j =0;

				for( j = 0; j< resultPos.size() ; j++){
					if(lastRes[j] != resultVec[resultPos[j]])
						break;
				}
				if(j == resultPos.size()){
					return;
				}
				else{
					for (; j < resultPos.size(); j++) {
						lastRes[j] = resultVec[resultPos[j]];
					}
				}
			}

			if (needselect == true) {
				if (resultVec[verifyPos[0]] != resultVec[verifyPos[1]]) {
					currentIndex++;
					continue;
				}
			}

#ifdef TEST_IDtoStr_TIME
            struct timeval start_time, end_time;
            gettimeofday(&start_time, NULL);
#endif

			for (size_t j = 0; j != resultPos.size(); j++) {
				UriTable->getURIById(URI, resultVec[resultPos[j]]);
#ifdef PRINT_RESULT
				std::cout << URI << " ";
#else
				resultPtr->push_back(URI);
#endif
			}
#ifdef TEST_IDtoStr_TIME
            gettimeofday(&end_time, NULL);
            istringtime +=  ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + (end_time.tv_usec - start_time.tv_usec));
            resultnum += resultPos.size();
#endif

#ifdef PRINT_RESULT
			std::cout << std::endl;
#endif
		} else {
			getResult_join(resultVec[keyPos[buflist_index]], bufferlist, buflist_index + 1);
		}

		currentIndex++;
		resultVec.resize(resultVecSize);
	}
}

void TripleBitQuery::getResult_join_with_flags(ID key, std::vector<EntityIDBuffer*>& bufferlist, size_t buflist_index, vector<unsigned> flags)
{
	if (buflist_index == bufferlist.size())
		return;

	EntityIDBuffer* entBuf = bufferlist[buflist_index];
	size_t currentIndex = entBuf->getEntityIDPos(key);//bufPreIndexs[buflist_index];
	if(currentIndex == size_t(-1))
		return;
	size_t bufsize = entBuf->getSize();

	ID* buf = entBuf->getBuffer();
	int IDCount = entBuf->getIDCount();
	int sortKey = entBuf->getSortKey();

	size_t resultVecSize = resultVec.size();
	std::string URI;
	while (currentIndex < bufsize && buf[currentIndex * IDCount + sortKey] == key) {
		for (int i = 0; i < IDCount; i++) {
			if (i != sortKey)
				resultVec.push_back(buf[currentIndex * IDCount + i]);
		}
		if (buflist_index == (bufferlist.size() - 1)) {
			static vector<ID> lastRes(resultPos.size(),0);
			if(_queryGraph->getDuplicateHandling() == TripleBitQueryGraph::NoDuplicates){
				//compare with the last
				unsigned j =0;

				for( j = 0; j< resultPos.size() ; j++){
					if(lastRes[j] != resultVec[resultPos[j]])
						break;
				}
				if(j == resultPos.size()){
					return;
				}
				else{
					for (; j < resultPos.size(); j++) {
						lastRes[j] = resultVec[resultPos[j]];
					}
				}
			}

			if (needselect == true) {
				if (resultVec[verifyPos[0]] != resultVec[verifyPos[1]]) {
					currentIndex++;
					continue;
				}
			}
#ifdef TEST_IDtoStr_TIME
            struct timeval start_time, end_time;
            gettimeofday(&start_time, NULL);
#endif

			for (size_t j = 0; j != resultPos.size(); j++) {
				if(flags[j] == 0)UriTable->getURIById(URI, resultVec[resultPos[j]]);
				else preTable->getPredicateByID(URI, resultVec[resultPos[j]]);
#ifdef PRINT_RESULT
				std::cout << URI << " ";
#else
				resultPtr->push_back(URI);
#endif
			}
#ifdef TEST_IDtoStr_TIME
            gettimeofday(&end_time, NULL);
            istringtime +=  ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + (end_time.tv_usec - start_time.tv_usec));
            resultnum += resultPos.size();
#endif

#ifdef PRINT_RESULT
			std::cout << std::endl;
#endif
		} else {
			getResult_join_with_flags(resultVec[keyPos[buflist_index]], bufferlist, buflist_index + 1, flags);
		}

		currentIndex++;
		resultVec.resize(resultVecSize);
	}
}

EntityType TripleBitQuery::getKnowBufferType(TripleBitQueryGraph::TripleNode* node1,TripleBitQueryGraph::TripleNode* node2){
	if(!node1->constSubject){
		if(!node2->constSubject && node1->subject == node2->subject)
			return SUBJECT;
		if(!node2->constObject && node1->subject == node2->object)
			return OBJECT;
		if(!node2->constPredicate && node1->subject == node2->predicate)
			return PREDICATE;
	}
	if(!node1->constObject){
		if (!node2->constSubject && node1->object == node2->subject)
			return SUBJECT;
		if (!node2->constObject && node1->object == node2->object)
			return OBJECT;
		if(!node2->constPredicate && node1->object == node2->predicate)
			return PREDICATE;
	}
	if(!node1->constPredicate){
		if (!node2->constSubject && node1->predicate == node2->subject)
			return SUBJECT;
		if (!node2->constObject && node1->predicate == node2->object)
			return OBJECT;
		if(!node2->constPredicate && node1->predicate == node2->predicate)
			return PREDICATE;
	}
	return DEFAULT;
}

Status TripleBitQuery::findEntityIDByTriple(TripleBitQueryGraph::TripleNode* triple, EntityIDBuffer* buffer, ID minID, ID maxID,unsigned maxNum)
{
	if(maxNum > 9000)maxNum = INT_MAX;
//	 struct timeval start, end;
//	 gettimeofday(&start,NULL);
	Status status;
	switch(triple->scanOperation)
	{
	case TripleBitQueryGraph::TripleNode::FINDSBYPO:
		status = entityFinder->findSubjectIDByPredicateAndObject(triple->predicate,triple->object,buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDOBYSP:
		status = entityFinder->findObjectIDByPredicateAndSubject(triple->predicate,triple->subject,buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDSOBYP:
		status = entityFinder->findSubjectIDAndObjectIDByPredicate(triple->predicate, buffer, minID, maxID,maxNum);
		break;
	case TripleBitQueryGraph::TripleNode::FINDOSBYP:
		status = entityFinder->findObjectIDAndSubjectIDByPredicate(triple->predicate, buffer, minID, maxID,maxNum);
		break;
	case TripleBitQueryGraph::TripleNode::FINDS:
		status = entityFinder->findSubject(buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDSBYP:
		status = entityFinder->findSubjectIDByPredicate(triple->predicate, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDSBYO:
		status = entityFinder->findSubjectIDByObject(triple->object, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDSPBYO:
		status = entityFinder->findSubjectIDAndPredicateIDByObject(triple->object, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDPSBYO:
		status = entityFinder->findPredicateIDAndSubjectIDByObject(triple->object, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDO:
		status = entityFinder->findObject(buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDOBYP:
		status = entityFinder->findObjectIDByPredicate(triple->predicate, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDOBYS:
		status = entityFinder->findObjectIDBySubject(triple->subject, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDP:
		status = entityFinder->findPredicate(buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDPBYO:
		status = entityFinder->findPredicateIDByObject(triple->object, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDPBYS:
		status = entityFinder->findPredicateIDBySubject(triple->subject, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDPBYSO:
		status = entityFinder->findPredicateIDBySubjectAndObject(triple->subject, triple->object, buffer);
		break;
	case TripleBitQueryGraph::TripleNode::FINDPOBYS:
		status = entityFinder->findPredicateIDAndObjectIDBySubject(triple->subject, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::FINDOPBYS:
		status = entityFinder->findObjectIDAndPredicateIDBySubject(triple->subject, buffer, minID, maxID);
		break;
	case TripleBitQueryGraph::TripleNode::NOOP:
		break;
	default:
		cerr<<"unsupported operation "<<triple->scanOperation<<endl;
		break;
	}
//	gettimeofday(&end,NULL);
//	cout<<"find pattern " << triple->tripleNodeID <<" time elapsed: "<<((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec ) / 1000000.0<<endl;
//	cout <<"minID:" << minID <<"  maxID:" <<maxID <<" maxNUm:" << maxNum<<"  buffersize:" << buffer->getSize()<< endl;

	return status;
}
Status TripleBitQuery::findEntityIDByKnowBuffer(TripleBitQueryGraph::TripleNode * triple, EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer,EntityType knowElement) {
	// knowElement means the the join position(s? p? o?) in the unkown triple. s(1),p(2),o(3)
	//cout <<"triple scanop" << triple->scanOperation <<"   knowElement:" << knowElement << endl;	
	buffer->empty();
	switch (triple->scanOperation) {
	case TripleBitQueryGraph::TripleNode::FINDSOBYNONE:
		entityFinder->findSOByKnowBuffer(buffer,knowBuffer,knowElement);
		break;
	case TripleBitQueryGraph::TripleNode::FINDOSBYNONE:
        entityFinder->findOSByKnowBuffer(buffer,knowBuffer,knowElement);
        break;
	case TripleBitQueryGraph::TripleNode::FINDSPBYNONE:
		entityFinder->findSPByKnowBuffer(buffer,knowBuffer,knowElement);
		break;
	case TripleBitQueryGraph::TripleNode::FINDPSBYNONE:
	    entityFinder->findPSByKnowBuffer(buffer,knowBuffer,knowElement);
	    break;
	case TripleBitQueryGraph::TripleNode::FINDOPBYNONE:
		entityFinder->findOPByKnowBuffer(buffer,knowBuffer,knowElement);
		break;
	case TripleBitQueryGraph::TripleNode::FINDPOBYNONE:
	    entityFinder->findPOByKnowBuffer(buffer,knowBuffer,knowElement);
	    break;
	case TripleBitQueryGraph::TripleNode::FINDSPO:
		//entityFinder->findSPO(buffer,knowBuffer,knowElement);
		cerr<<"unsupported operation "<<triple->scanOperation<<endl;
		break;
	case TripleBitQueryGraph::TripleNode::FINDSOBYP:
		entityFinder->findSOByKnowBuffer(triple->predicate,buffer,knowBuffer,knowElement);
		break;
	case TripleBitQueryGraph::TripleNode::FINDOSBYP:
        entityFinder->findOSByKnowBuffer(triple->predicate,buffer,knowBuffer,knowElement);
        break;
    /*case TripleBitQueryGraph::TripleNode::FINDSBYP:
        entityFinder->findSByKnowBuffer(triple->predicate,buffer,knowBuffer,knowElement);
        break;*/
	default:
		cerr<<"error" << endl;
		exit(-1);
	}
	return OK;
}

Status TripleBitQuery::acyclicJoin()
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator joinNodeIter = _query->joinVariables.begin();
	vector<pair<TripleBitQueryGraph::TripleNodeID,TripleBitQueryGraph::JoinVariableNode::DimType> >::iterator nodePatternIter;

	TripleBitQueryGraph::JoinVariableNode* node;
	EntityIDBuffer *buffer;
	TripleBitQueryGraph::TripleNode* triple;

	// initialize the patterns which are related to the first variable.
	getVariableNodeByID(node, *joinNodeIter);
	nodePatternIter = node->appear_tpnodes.begin();
	buffer = BufferManager::getInstance()->getNewBuffer();
	EntityIDList[nodePatternIter->first] = buffer;
	getTripleNodeByID(triple, nodePatternIter->first);
	findEntityIDByTriple(triple, buffer, 0, INT_MAX);

	buffer = EntityIDList[nodePatternIter->first];
	if ( this->findEntitiesAndJoin(*joinNodeIter, node->appear_tpnodes,true) == NULL_RESULT) {
#ifdef PRINT_RESULT
		cout<<"empty result"<<endl;
#else
		resultPtr->push_back("-1");
		resultPtr->push_back("Empty Result");
#endif
		return OK;
	}

	joinNodeIter++;
	//iterate the join variables.
	for (; joinNodeIter != _query->joinVariables.end(); joinNodeIter++) {
		getVariableNodeByID(node, *joinNodeIter);
		if ( node->appear_tpnodes.size() > 1 ) {
			if ( this->findEntitiesAndJoin(*joinNodeIter, node->appear_tpnodes,true) == NULL_RESULT) {
#ifdef PRINT_RESULT
				cout<<"empty result"<<endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
	}
	/// iterate reverse.
	TripleBitQueryGraph::JoinVariableNodeID varID;
	bool isLeafNode;
	size_t size = _query->joinVariables.size();
	size_t i;
	for( i = 0; i <size; i++)
	{
		varID = _query->joinVariables[i];
		isLeafNode = nodeIsLeaf(varID);

		if( isLeafNode == false){
			getVariableNodeByID(node,varID);
			if ( this->findEntitiesAndJoin(varID,node->appear_tpnodes,false) == NULL_RESULT) {
#ifdef PRINT_RESULT
				cout<<"empty result"<<endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}

	}
	//TODO materialize.
	keyPos.clear();
	resultPos.clear();
	verifyPos.clear();
	size_t projectionSize = _queryGraph->getProjection().size();
	if(projectionSize == 1) {
		uint projBitV, nodeBitV, tempBitV;
		std::vector<ID> resultVar;
		resultVar.resize(0);
		ID sortID;

		generateProjectionBitVector(projBitV, _queryGraph->getProjection());
		TripleBitQueryGraph::TripleNodeID tid;
		size_t bufsize = UINT_MAX;
		for( i = 0; i != _query->tripleNodes.size(); i++) {
			if(EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0)
				continue;
			generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
			tempBitV = projBitV & nodeBitV;
			if(tempBitV == projBitV) {
				//TODO output the result.
				if(EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1) {
					insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
					tid = _query->tripleNodes[i].tripleNodeID;
					break;
				}else {
					if(EntityIDList[_query->tripleNodes[i].tripleNodeID]->getSize() < bufsize) {
						insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
						bufsize = EntityIDList[_query->tripleNodes[i].tripleNodeID]->getSize();
						tid = _query->tripleNodes[i].tripleNodeID;
					}
				}
			}
		}

		std::vector<EntityIDBuffer*> bufferlist;
		bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);
		generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
		needselect = false;

		std::string URI;
		ID* p = EntityIDList[tid]->getBuffer();
		int IDCount = EntityIDList[tid]->getIDCount();
		if(_query->tripleNodes.size() == 1)bufsize = EntityIDList[tid]->getSize();
		ID curID ,lastID;
		curID = lastID = 0;
#ifdef TEST_IDtoStr_TIME
        struct timeval start_time, end_time;
        gettimeofday(&start_time, NULL);
#endif
		if(_queryGraph->getHasPredicate() == false){
			for(i = 0; i != bufsize; i++) {
				curID = p[i * IDCount + resultPos[0]];
				if(_queryGraph->getDuplicateHandling() == TripleBitQueryGraph::NoDuplicates){
					if(curID == lastID)
						continue;
					else
						lastID = curID;
				}

				UriTable->getURIById(URI, curID);
#ifdef PRINT_RESULT
				std::cout<<URI<<std::endl;
#else
				resultPtr->push_back(URI);
#endif
			}
		}else{
			for(i = 0; i != bufsize; i++) {
				curID = p[i * IDCount + resultPos[0]];
				if(_queryGraph->getDuplicateHandling() == TripleBitQueryGraph::NoDuplicates){
					if(curID == lastID)
						continue;
					else
						lastID = curID;
				}

				preTable->getPredicateByID(URI, curID);
#ifdef PRINT_RESULT
				std::cout<<URI<<std::endl;
#else
				resultPtr->push_back(URI);
#endif
			}
		}
#ifdef TEST_IDtoStr_TIME
        gettimeofday(&end_time, NULL);
        istringtime +=  ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + (end_time.tv_usec - start_time.tv_usec));
        resultnum += bufsize;
#endif

	} else {
		std::vector<EntityIDBuffer*> bufferlist;
		//std::vector<TripleBitQueryGraph::TripleNode*> patternlist, patternlist2;
		std::vector<ID> resultVar;
		ID sortID = 0;
		resultVar.resize(0);
		uint projBitV, nodeBitV, resultBitV, tempBitV;
		resultBitV = 0;
		generateProjectionBitVector(projBitV, _queryGraph->getProjection());
		i = 0;
		int sortKey, cycles = 0;//cycles is used to avoid endless loop
		size_t tnodesize = _query->tripleNodes.size();
		bool firstTripleNode = true;
		
		while(true) {
			if(EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0 || (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1) 
				|| (find(bufferlist.begin(),bufferlist.end(),EntityIDList[_query->tripleNodes[i].tripleNodeID]) != bufferlist.end())) {
				i++;
				if(i == tnodesize){
					cycles++;
					if(cycles == 2)break;
				}
				i = i % tnodesize;
				continue;
			}
			generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
			if(firstTripleNode == true){
				firstTripleNode = false;
				insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
				sortKey = EntityIDList[_query->tripleNodes[i].tripleNodeID]->getSortKey();
			}else{
				tempBitV = nodeBitV & resultBitV;
				if(countOneBits(tempBitV) == 1) {
					//ID key = ID(log((double)tempBitV) / log(2.0));
					ID key = bitVtoID(tempBitV);
					sortKey = insertVarID(key, resultVar,_query->tripleNodes[i], sortID);
					vector<ID>::iterator iter = find(resultVar.begin(), resultVar.end(), sortID);
					keyPos.push_back(iter - resultVar.begin());
				} else{
					i++;
					if(i == tnodesize){
						cycles++;
						if(cycles == 2)break;
					}
					i = i % tnodesize;
					continue;
				}
			}

			resultBitV = resultBitV | nodeBitV;
			EntityIDList[_query->tripleNodes[i].tripleNodeID]->setSortKey(sortKey);
			bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);
			//cout<<"OP = "<<_query->tripleNodes[i].scanOperation<<" , tripleNodeID = "<<_query->tripleNodes[i].tripleNodeID<<endl;
			//if((resultBitV & projBitV) == projBitV)break;

			i++;
			if(i == tnodesize){
				cycles++;
				if(cycles == 2)break;
			}
			i = i % tnodesize;
		}

		//cout <<"buffer size:" << bufferlist.size() << endl;
		//for(size_t k = 0;k < keyPos.size();k++)cout<<"keyPos["<<k<<"] = "<<keyPos[k]<<endl;
		//for(size_t k = 0;k < bufferlist.size();k++)cout<<"sortKey = "<<bufferlist[k]->getSortKey()<<endl;
		for (i = 0; i < bufferlist.size(); i++) {
			bufferlist[i]->sort();
		}

		generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
		needselect = false;

		std::string URI;
		if(bufferlist.size() > 1){
			EntityIDBuffer* buf = bufferlist[0];
			size_t bufsize = buf->getSize();
			bufPreIndexs.resize(bufferlist.size(), 0);
			int IDCount = buf->getIDCount();
			ID* ids = buf->getBuffer();

			if(_queryGraph->getHasPredicate() == false){
				for(i = 0; i != bufsize; i++) {
					resultVec.resize(0);
					for(int j = 0; j < IDCount ; j++)
						resultVec.push_back(ids[i * IDCount + j]);
					getResult_join(resultVec[keyPos[0]], bufferlist,1);
				}
			}
			else{
				vector<unsigned> flags = _queryGraph->getPredicateFlag();
				for(i = 0; i != bufsize; i++) {
					resultVec.resize(0);
					for(int j = 0; j < IDCount ; j++)
						resultVec.push_back(ids[i * IDCount + j]);
					getResult_join_with_flags(resultVec[keyPos[0]], bufferlist,1,flags);
				}
			}

		}else{
			EntityIDBuffer* temp = bufferlist[0];
			size_t total = temp->getSize();
			size_t i;
			unsigned IDCount = temp->getIDCount();
			ID* p = temp->getBuffer();
			int k,id;
#ifdef PRINT_RESULT
#ifdef TEST_IDtoStr_TIME
            struct timeval start_time, end_time;
            gettimeofday(&start_time, NULL);
#endif

			if(_queryGraph->getHasPredicate() == false){
				for (i = 0; i != total; i++) {
					for (k = 0; k < temp->IDCount; k++) {
						id = p[i * IDCount + resultPos[k]] ;
						UriTable->getURIById(URI, id);
#ifdef PRINT_RESULT
						cout <<URI <<" " ;
#else
						resultPtr->push_back(URI);
#endif
					}
#ifdef PRINT_RESULT
					cout << endl;
#endif
				}
			}
			else{
				vector<unsigned> flags = _queryGraph->getPredicateFlag();
				for (i = 0; i != total; i++) {
					for (k = 0; k < temp->IDCount; k++) {
						id = p[i * IDCount + resultPos[k]] ;
						if(flags[k] == 0)UriTable->getURIById(URI, id);
						else preTable->getPredicateByID(URI, id);
#ifdef PRINT_RESULT
						cout <<URI <<" " ;
#else
						resultPtr->push_back(URI);
#endif
					}
#ifdef PRINT_RESULT
					cout << endl;
#endif
				}
			}
#ifdef TEST_IDtoStr_TIME
            gettimeofday(&end_time, NULL);
            istringtime +=  ((end_time.tv_sec - start_time.tv_sec) * 1000000.0 + (end_time.tv_usec - start_time.tv_usec));
            resultnum += total;
#endif

#endif
		}
	}
	return OK;
}

bool TripleBitQuery::nodeIsLeaf(TripleBitQueryGraph::JoinVariableNodeID varID)
{
	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator iter;
	iter = find(_query->leafNodes.begin(),_query->leafNodes.end(),varID);
	if( iter != _query->leafNodes.end())
		return true;
	else
		return false;
}


Status TripleBitQuery::cyclicJoin() {
	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator joinNodeIter = _query->joinVariables.begin();
	vector<pair<TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >::iterator nodePatternIter;

	TripleBitQueryGraph::JoinVariableNode* node;
	EntityIDBuffer *buffer;
	TripleBitQueryGraph::TripleNode* triple;

	//initialize the patterns of the first variable
	getVariableNodeByID(node, *joinNodeIter);
	nodePatternIter = node->appear_tpnodes.begin();
	buffer = BufferManager::getInstance()->getNewBuffer();
	EntityIDList[nodePatternIter->first] = buffer;
	getTripleNodeByID(triple, nodePatternIter->first);
	findEntityIDByTriple(triple, buffer, 0, INT_MAX);

	if ( this->findEntitiesAndJoin(*joinNodeIter, node->appear_tpnodes, true) == NULL_RESULT ) {
#ifdef PRINT_RESULT
		cout<<"empty result"<<endl;
#else
		resultPtr->push_back("-1");
		resultPtr->push_back("Empty Result");
#endif
		return OK;
	}

	joinNodeIter++;
	for (; joinNodeIter != _query->joinVariables.end(); joinNodeIter++) {
		//node = &(*joinNodeIter);
		getVariableNodeByID(node, *joinNodeIter);
		if (node->appear_tpnodes.size() > 1)
			if ( this->findEntitiesAndJoin(*joinNodeIter, node->appear_tpnodes, true) == NULL_RESULT ) {
#ifdef PRINT_RESULT
				cout<<"empty result"<<endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
	}

	// iterate reverse.
	TripleBitQueryGraph::JoinVariableNodeID varID;
	bool isLeafNode;
	size_t size = (int)_query->joinVariables.size();
	size_t i;

	for (i = size - 1; i != (size_t(-1)); i--) {
		varID = _query->joinVariables[i];
		isLeafNode = nodeIsLeaf(varID);
		if (isLeafNode == false) {
			getVariableNodeByID(node, varID);
			if ( this->findEntitiesAndJoin(varID, node->appear_tpnodes, false) == NULL_RESULT ) {
#ifdef PRINT_RESULT
				cout<<"empty result"<<endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
	}

	for( i = 0; i < size; i++)
	{
		varID = _query->joinVariables[i];
		isLeafNode = nodeIsLeaf(varID);
		getVariableNodeByID(node, varID);
		if ( this->findEntitiesAndJoin(varID, node->appear_tpnodes, false) == NULL_RESULT ) {
#ifdef PRINT_RESULT
			cout<<"empty result"<<endl;
#else
			resultPtr->push_back("-1");
			resultPtr->push_back("Empty Result");
#endif
			return OK;
		}
	}

/*	for (i = size - 1; i != (size_t(-1)); i--) {
		varID = _query->joinVariables[i];
		isLeafNode = nodeIsLeaf(varID);
		if (isLeafNode == false) {
			getVariableNodeByID(node, varID);
			if (  this->findEntitiesAndJoin(varID, node->appear_tpnodes, false) == NULL_RESULT ) {
#ifdef PRINT_RESULT
				cout<<"empty result"<<endl;
#else
				resultPtr->push_back("-1");
				resultPtr->push_back("Empty Result");
#endif
				return OK;
			}
		}
	}*/

	//TODO materialize
	std::vector<EntityIDBuffer*> bufferlist;
	std::vector<ID> resultVar;
	ID sortID;

	resultVar.resize(0);
	uint projBitV, nodeBitV, resultBitV, tempBitV;
	resultBitV = 0;
	generateProjectionBitVector(projBitV, _queryGraph->getProjection());

	int sortKey;
	size_t tnodesize = _query->tripleNodes.size();
	std::set<ID> tids;
	ID tid;
	bool complete = true;
	i = 0;
	vector<ID>::iterator iter;

	keyPos.clear();
	resultPos.clear();
	verifyPos.clear();

	while(true) {
		//if the pattern has no buffer, then skip it;
		if(EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0) {
			i++;
			i = i % tnodesize;
			continue;
		}
		generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
		if(countOneBits(nodeBitV) == 1) {
			i++;
			i = i % tnodesize;
			continue;
		}

		tid = _query->tripleNodes[i].tripleNodeID;
		if(tids.count(tid) == 0) {
			if(countOneBits(resultBitV) == 0) {
				insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
				sortKey = EntityIDList[tid]->getSortKey();
			} else {
				tempBitV = nodeBitV & resultBitV;
				if(countOneBits(tempBitV) == 1) {
					ID key = bitVtoID(tempBitV);//ID(log((double)tempBitV) / log(2.0));
					sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
					iter = find(resultVar.begin(), resultVar.end(), sortID);
					keyPos.push_back(iter - resultVar.begin());
				} else if(countOneBits(tempBitV) == 2){
					// verify buffers;
					ID key = bitVtoID(tempBitV); //ID(log((double)tempBitV) / log(2.0));
					sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
					iter = find(resultVar.begin(), resultVar.end(), sortID);
					keyPos.push_back(iter - resultVar.begin());
				} else {
					complete = false;
					i++;
					i = i % tnodesize;
					continue;
				}
			}
			resultBitV = resultBitV | nodeBitV;
			EntityIDList[tid]->setSortKey(sortKey);
			bufferlist.push_back(EntityIDList[tid]);
			tids.insert(tid);
		}

		i++;
		if( i == tnodesize) {
			if( complete == true)
				break;
			else {
				complete = true;
				i = i % tnodesize;
			}
		}
	}

	for (i = 0; i < bufferlist.size(); i++) {
		bufferlist[i]->sort();
	}

	generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
	/// generate verify pos vector;
	generateVerifyPos(resultVar, verifyPos);
	needselect = true;

	EntityIDBuffer* buf = bufferlist[0];
	size_t bufsize = buf->getSize();

	int IDCount = buf->getIDCount(); ID* ids = buf->getBuffer();
	if(_queryGraph->getHasPredicate() == false){
		for(i = 0; i != bufsize; i++) {
			resultVec.resize(0);
			for(int j = 0; j < IDCount ; j++)
				resultVec.push_back(ids[i * IDCount + j]);
			getResult_join(resultVec[keyPos[0]], bufferlist, 1);
		}
	}
	else{
		vector<unsigned> flags = _queryGraph->getPredicateFlag();
		for(i = 0; i != bufsize; i++) {
			resultVec.resize(0);
			for(int j = 0; j < IDCount ; j++)
				resultVec.push_back(ids[i * IDCount + j]);
			getResult_join_with_flags(resultVec[keyPos[0]], bufferlist, 1, flags);
		}
	}
	return OK;
}

int TripleBitQuery::getVariablePos(TripleBitQueryGraph::JoinVariableNodeID id, TripleBitQueryGraph::TripleNode* triple)
{
	int pos;

	switch(triple->scanOperation) {
	case TripleBitQueryGraph::TripleNode::FINDO:
	case TripleBitQueryGraph::TripleNode::FINDOBYP:
	case TripleBitQueryGraph::TripleNode::FINDOBYS:
	case TripleBitQueryGraph::TripleNode::FINDOBYSP:
		pos = 1;
		break;
	case TripleBitQueryGraph::TripleNode::FINDOPBYS:
		if(id == triple->object) pos = 1;
		else pos = 2;
		break;
	case TripleBitQueryGraph::TripleNode::FINDOSBYP:
		if(id == triple->object) pos = 1;
		else pos = 2;
		break;
	case TripleBitQueryGraph::TripleNode::FINDP:
	case TripleBitQueryGraph::TripleNode::FINDPBYO:
	case TripleBitQueryGraph::TripleNode::FINDPBYS:
	case TripleBitQueryGraph::TripleNode::FINDPBYSO:
		pos = 1;
		break;
	case TripleBitQueryGraph::TripleNode::FINDPOBYS:
		if(id == triple->predicate) pos = 1;
		else pos = 2;
		break;
	case TripleBitQueryGraph::TripleNode::FINDPSBYO:
		if(id == triple->predicate) pos = 1;
		else pos =2 ;
		break;
	case TripleBitQueryGraph::TripleNode::FINDS:
	case TripleBitQueryGraph::TripleNode::FINDSBYO:
	case TripleBitQueryGraph::TripleNode::FINDSBYP:
	case TripleBitQueryGraph::TripleNode::FINDSBYPO:
		pos = 1;
		break;
	case TripleBitQueryGraph::TripleNode::FINDSOBYP:
		if(id == triple->subject) pos = 1;
		else pos = 2;
		break;
	case TripleBitQueryGraph::TripleNode::FINDSPBYO:
		if(id == triple->subject) pos = 1;
		else pos = 2;
		break;
	case TripleBitQueryGraph::TripleNode::FINDSOBYNONE:
		if (id == triple->subject) pos = 1;
		else pos = 2;
		break;
	case TripleBitQueryGraph::TripleNode::FINDOSBYNONE:
		if (id == triple->object) pos = 1;
		else pos = 2;
		break;
	case TripleBitQueryGraph::TripleNode::FINDSPBYNONE:
		if (id == triple->subject) pos = 1;
		else pos = 2;
		break;
	case TripleBitQueryGraph::TripleNode::FINDPSBYNONE:
		if (id == triple->predicate) pos = 1;
		else pos = 2;
		break;
	case TripleBitQueryGraph::TripleNode::FINDOPBYNONE:
		if (id == triple->object) pos = 1;
		else pos = 2;
		break;
	case TripleBitQueryGraph::TripleNode::FINDPOBYNONE:
		if (id == triple->predicate) pos = 1;
		else pos = 2;
		break;
	case TripleBitQueryGraph::TripleNode::FINDSPO:
		if(id == triple->subject) pos = 1;
		else if(id == triple->predicate) pos = 2;
		else pos = 3;
		break;
	case TripleBitQueryGraph::TripleNode::NOOP:
		pos = -1;
		break;
	}
	return pos;
}

Status TripleBitQuery::getTripleNodeByID(TripleBitQueryGraph::TripleNode*& triple, TripleBitQueryGraph::TripleNodeID nodeID)
{
	vector<TripleBitQueryGraph::TripleNode>::iterator iter = _query->tripleNodes.begin();

	for(; iter != _query->tripleNodes.end(); iter++)
	{
		if(iter->tripleNodeID == nodeID){
			triple = &(*iter);
			return OK;
		}
	}

	return NOT_FOUND;
}

Status TripleBitQuery::findEntitiesAndJoin(TripleBitQueryGraph::JoinVariableNodeID id,
		vector< pair < TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes,bool firstTime)
{
	size_t minSize;
	ID minSizeID;
	size_t size;
	EntityIDBuffer* buffer;
	map<ID,bool> firstInsertFlag;
	Status s;

	minSize = INT_MAX;
	minSizeID = tpnodes[0].first;;

	size = EntityIDList.size();

	EntityIDListIterType iter;
	size_t i;
	if( firstTime == true){
		for (i = 0; i < tpnodes.size(); i++) {
			firstInsertFlag[tpnodes[i].first] = false;
			iter = EntityIDList.find(tpnodes[i].first);
			if (iter != EntityIDList.end()) {
				if ((size = iter->second->getSize()) < minSize) {
					minSize = size;
					minSizeID = tpnodes[i].first;
				}
			} else if (getVariableCount(tpnodes[i].first) >= 2) {
				buffer = BufferManager::getInstance()->getNewBuffer();
				EntityIDList[tpnodes[i].first] = buffer;
				firstInsertFlag[tpnodes[i].first] = true;
			}
		}
	}else{
		for(i = 0; i < tpnodes.size(); i++) {
			firstInsertFlag[tpnodes[i].first] = false;
			if(getVariableCount(tpnodes[i].first) >= 2){
				if(EntityIDList[tpnodes[i].first]->getSize() < minSize) {
					minSize = EntityIDList[tpnodes[i].first]->getSize();
					minSizeID = tpnodes[i].first;
				}
			}
		}
	}

	iter = EntityIDList.find(minSizeID);
	if( iter == EntityIDList.end())
	{
		TripleBitQueryGraph::TripleNode* triple;
		getTripleNodeByID(triple,minSizeID);
		buffer = BufferManager::getInstance()->getNewBuffer();
		EntityIDList[minSizeID] = buffer;
		findEntityIDByTriple(triple,buffer,0, INT_MAX);
	}

	if(firstTime == true)
		s = findEntitiesAndJoinFirstTime(tpnodes,minSizeID,firstInsertFlag, id);
	else
		s = modifyEntitiesAndJoin(tpnodes, minSizeID, id);

	return s;
}

EntityType TripleBitQuery::getDimInTriple(vector< pair < TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes,
			TripleBitQueryGraph::TripleNodeID tripleID)
{
	size_t i;
	TripleBitQueryGraph::JoinVariableNode::DimType type = TripleBitQueryGraph::JoinVariableNode::SUBJECT;

	for( i = 0 ; i < tpnodes.size(); i++)
	{
		if ( tpnodes[i].first == tripleID)
		{
			type = tpnodes[i].second;
			break;
		}
	}
	if(type == TripleBitQueryGraph::JoinVariableNode::SUBJECT)
		return SUBJECT;
	else if(type == TripleBitQueryGraph::JoinVariableNode::PREDICATE)
		return PREDICATE;
	else
		return OBJECT;
}

Status TripleBitQuery::findEntitiesAndJoinFirstTime(vector< pair < TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes,
			ID tripleID, map<ID,bool>& firstInsertFlag, TripleBitQueryGraph::JoinVariableNodeID id)
{
	EntityIDBuffer* buffer = EntityIDList[tripleID];
#ifdef PRINT_BUFFERSIZE
	cout<<__FUNCTION__<<endl;
	cout <<"var id:" << id <<"  tripleID:"<< tripleID << endl;
#endif

	EntityIDBuffer* temp;
	TripleBitQueryGraph::TripleNode * triple,*lasttriple;
	int joinKey , joinKey2;
	bool insertFlag;
	size_t i;
	ID tripleNo;
	int varCount;

	ID maxID,minID;
	if( tpnodes.size() == 1 )
		return OK;

	joinKey = this->getVariablePos(id,tripleID);
	buffer->sort(joinKey);

	getTripleNodeByID(lasttriple,tripleID );
	for ( i = 0; i < tpnodes.size(); i++)
	{
		tripleNo = tpnodes[i].first;

		if (tripleNo != tripleID) {
			joinKey2 = this->getVariablePos(id, tripleNo);
			//cout<<"tripleID = "<<tripleID<<" , tipleNo = "<<tripleNo<<endl;
			//cout<<"joinKey = "<<joinKey<<" , joinKey2 = "<<joinKey2<<endl;
			insertFlag = firstInsertFlag[tripleNo];
			getTripleNodeByID(triple, tripleNo);
			varCount = this->getVariableCount(triple);
			if (insertFlag == false && varCount == 1) {
				buffer->getMinMax(minID, maxID);
				temp = BufferManager::getInstance()->getNewBuffer();
				if(findEntityIDByTriple(triple, temp, minID, maxID,buffer->getSize()) == TOO_MUCH){
					EntityType entityType = this->getKnowBufferType(lasttriple,triple);
					lasttriple = triple;
					this->findEntityIDByKnowBuffer(triple,temp,buffer,entityType);
				}
#ifdef PRINT_BUFFERSIZE
				cout<<"case 1"<<endl;
				cout<<"pattern "<<tripleNo<<" temp buffer size: "<<temp->getSize()<<endl;
				cout<<"pattern "<<tripleID<<" buffer size: "<<buffer->getSize()<<endl;
#endif
				mergeJoin.Join(buffer,temp,joinKey,1,false);
#ifdef PRINT_BUFFERSIZE
				cout<<"pattern "<<tripleNo<<" temp buffer size: "<<temp->getSize()<<endl;
				cout<<"pattern "<<tripleID<<" buffer size: "<<buffer->getSize()<<endl;
#endif
				BufferManager::getInstance()->freeBuffer(temp);
			} else if (insertFlag == false && varCount == 2) {
				temp = EntityIDList[tripleNo];
				mergeJoin.Join(buffer,temp,joinKey,joinKey2,true);
#ifdef PRINT_BUFFERSIZE
				cout<<"case 2"<<endl;
				cout<<"pattern "<<tripleID<<" buffer size: "<<buffer->getSize()<<endl;
				cout<<"pattern "<<tripleNo<<" buffer size: "<<temp->getSize()<<endl;
#endif
			} else {
				buffer->getMinMax(minID, maxID);
				temp = EntityIDList[tripleNo];
				if(varCount == 3){
					if(insertFlag == true){
						EntityType knowElement ;
						if(id == triple->subject){	knowElement =SUBJECT;}
						else if(id == triple->object){knowElement = OBJECT;}
						else if(id == triple->predicate){knowElement = PREDICATE;}
						this->findEntityIDByKnowBuffer(triple,temp,buffer,knowElement);
					}else{
						temp->sort(joinKey2);
						mergeJoin.Join(buffer,temp,joinKey,joinKey2,true);
					}
				}
				else{
					if (findEntityIDByTriple(triple, temp, minID, maxID,buffer->getSize()) == TOO_MUCH) {
						EntityType entityType = this->getKnowBufferType(lasttriple, triple);
						lasttriple = triple;
						this->findEntityIDByKnowBuffer(triple, temp, buffer,entityType);
#ifdef PRINT_BUFFERSIZE
                        cout<<"case 3 too much"<<endl;
                        cout<<"pattern "<<tripleNo<<" buffer size: "<<temp->getSize()<<endl;
						cout<<"pattern "<<tripleID<<" buffer size: "<<buffer->getSize()<<endl;
#endif
						mergeJoin.Join(buffer,temp,joinKey,joinKey2,false);
				
					}else{
#ifdef PRINT_BUFFERSIZE
                        cout<<"case 3"<<endl;
                        cout<<"pattern "<<tripleNo<<" buffer size: "<<temp->getSize()<<endl;
#endif
						//hashJoin.Join(buffer,temp,joinKey, joinKey2);
						mergeJoin.Join(buffer,temp,joinKey,joinKey2,true);
					}
				}
#ifdef PRINT_BUFFERSIZE
				cout<<"pattern "<<tripleNo<<" buffer size: "<<temp->getSize()<<endl;
				cout<<"pattern "<<tripleID<<" buffer size: "<<buffer->getSize()<<endl;
#endif
			}
		}

		if ( buffer->getSize() == 0)
			return NULL_RESULT;
	}

	if ( buffer->getSize() == 0 ) {
		return NULL_RESULT;
	}
	return OK;
}


int TripleBitQuery::getVariablePos(TripleBitQueryGraph::JoinVariableNodeID id,
		TripleBitQueryGraph::TripleNodeID tripleID)
{
	TripleBitQueryGraph::TripleNode * triple;
	getTripleNodeByID(triple,tripleID);
	return getVariablePos(id, triple);
}


Status TripleBitQuery::modifyEntitiesAndJoin(vector< pair < TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes,
		ID tripleID, TripleBitQueryGraph::JoinVariableNodeID id)
{
	EntityIDBuffer* buffer = EntityIDList[tripleID];
	EntityIDBuffer* temp;
	TripleBitQueryGraph::TripleNode * triple;
	int joinKey, joinKey2;
	size_t i;
	ID tripleNo;
	int varCount;
	bool sizeChanged = false;

#ifdef PRINT_BUFFERSIZE
	cout<<__FUNCTION__<<endl;
	cout<<"id:" << id <<"  tripleID:" << tripleID << endl;
#endif
	joinKey = this->getVariablePos(id, tripleID);
	buffer->setSortKey(joinKey - 1);
	size_t size = buffer->getSize();
	ID lastNO = size_t(-1);
	ID lastID = size_t(-1);
	for (i = 0; i < tpnodes.size(); i++) {
		tripleNo = tpnodes[i].first;
		this->getTripleNodeByID(triple, tripleNo);
		if (tripleNo != tripleID) {
			if (lastNO == tripleNo && lastID == tripleID)
				continue;
			else if (lastNO == tripleID && lastID == tripleNo)
				continue;
			else {
				lastNO = tripleNo;
				lastID = tripleID;
			}
			varCount = getVariableCount(triple);
//			cout <<"xxx:"<<tripleNo <<"  " <<tripleID << "  vc" << varCount <<endl;
			if ( varCount >= 2) {
				joinKey2 = this->getVariablePos(id, tripleNo);
				temp = EntityIDList[tripleNo];
#ifdef PRINT_BUFFERSIZE
				cout<<"--------------------------------------"<<endl;
				cout<<"pattern "<<tripleNo<<" buffer size: "<<temp->getSize()<<endl;
				cout<<"pattern "<<tripleID<<" buffer size: "<<buffer->getSize()<<endl;
#endif
				hashJoin.Join(buffer,temp,joinKey, joinKey2);
#ifdef PRINT_BUFFERSIZE
				cout<<"pattern "<<tripleNo<<" buffer size: "<<temp->getSize()<<endl;
				cout<<"pattern "<<tripleID<<" buffer size: "<<buffer->getSize()<<endl;
#endif
			}
		}
		if( buffer->getSize() == 0 ) {
			return NULL_RESULT;
		}
	}

	if ( size != buffer->getSize() || sizeChanged == true )
		return BUFFER_MODIFIED;
	else
		return OK;
}

Status TripleBitQuery::getVariableNodeByID(TripleBitQueryGraph::JoinVariableNode*& node, TripleBitQueryGraph::JoinVariableNodeID id)
{
	int i, size = _query->joinVariableNodes.size();

	for( i = 0; i < size; i++)
	{
		if( _query->joinVariableNodes[i].value == id){
			node = &(_query->joinVariableNodes[i]);
			break;
		}
	}

	return OK;
}

int TripleBitQuery::getVariableCount(TripleBitQueryGraph::TripleNode* node)
{
	switch(node->scanOperation) {
	case TripleBitQueryGraph::TripleNode::FINDO:
	case TripleBitQueryGraph::TripleNode::FINDOBYP:
	case TripleBitQueryGraph::TripleNode::FINDOBYS:
	case TripleBitQueryGraph::TripleNode::FINDOBYSP:
		return 1;
	case TripleBitQueryGraph::TripleNode::FINDP:
	case TripleBitQueryGraph::TripleNode::FINDPBYO:
	case TripleBitQueryGraph::TripleNode::FINDPBYS:
	case TripleBitQueryGraph::TripleNode::FINDPBYSO:
		return 1;
	case TripleBitQueryGraph::TripleNode::FINDS:
	case TripleBitQueryGraph::TripleNode::FINDSBYO:
	case TripleBitQueryGraph::TripleNode::FINDSBYP:
	case TripleBitQueryGraph::TripleNode::FINDSBYPO:
		return 1;
	case TripleBitQueryGraph::TripleNode::FINDSOBYP:
	case TripleBitQueryGraph::TripleNode::FINDOSBYP:
	case TripleBitQueryGraph::TripleNode::FINDSPBYO:
	case TripleBitQueryGraph::TripleNode::FINDPSBYO:
	case TripleBitQueryGraph::TripleNode::FINDOPBYS:
	case TripleBitQueryGraph::TripleNode::FINDPOBYS:
		return 2;
	case TripleBitQueryGraph::TripleNode::FINDSOBYNONE:
	case TripleBitQueryGraph::TripleNode::FINDOSBYNONE:
	case TripleBitQueryGraph::TripleNode::FINDSPBYNONE:
	case TripleBitQueryGraph::TripleNode::FINDPSBYNONE:
	case TripleBitQueryGraph::TripleNode::FINDOPBYNONE:
	case TripleBitQueryGraph::TripleNode::FINDPOBYNONE:
		return 3;
	case TripleBitQueryGraph::TripleNode::FINDSPO:
		return 3;
	}

	return -1;
}

int TripleBitQuery::getVariableCount(TripleBitQueryGraph::TripleNodeID id)
{
	TripleBitQueryGraph::TripleNode* triple;
	getTripleNodeByID(triple,id);
	return getVariableCount(triple);
}

void TripleBitQuery::clearTimeandCount(){
    istringtime = 0;
    resultnum = 0;
}

void TripleBitQuery::printIDtoStringTime(){
//  fprintf(stderr,"total ID: %d\n",resultnum);
    fprintf(stderr,"total ID to string time: %f us.\n",istringtime);
}
