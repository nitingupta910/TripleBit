#ifndef FINDENTITYID_H_
#define FINDENTITYID_H_

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
class ColumnBuffer;
class TripleBitRepository;
class EntityIDBuffer;
class Chunk;
class TwoConstantStatisticsBuffer;
class OneConstantStatisticsBuffer;

#include "../TripleBit.h"
#include "../ThreadPool.h"

#ifdef TEST_TIME
#include "../TimeStamp.h"
#endif

class FindEntityID {
private:
	BitmapBuffer* bitmap;
	URITable* UriTable;
	PredicateTable* preTable;
	
	TwoConstantStatisticsBuffer* spStatBuffer, *opStatBuffer;
	OneConstantStatisticsBuffer* sStatBuffer, *oStatBuffer;

	EntityIDBuffer *XTemp, *XYTemp, *tempBuffer1, *tempBuffer2;
	pthread_mutex_t mergeBufferMutex;
#ifdef TEST_TIME
	TimeStamp indexTimer, readTimer;
#endif

public:
	FindEntityID(TripleBitRepository* repo);
	virtual ~FindEntityID();
	Status findSubjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findObjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findObjectIDAndSubjectIDByPredicate(ID predicate, EntityIDBuffer* buffer, ID min, ID max,unsigned maxNum= INT_MAX);
	Status findSubjectIDAndObjectIDByPredicate(ID predicate, EntityIDBuffer* buffer);
	Status findSubjectIDAndObjectIDByPredicate(ID predicate, EntityIDBuffer* buffer, ID min, ID max,unsigned maxNum= INT_MAX);

	Status findSubjectIDByPredicateAndObject(ID predicateID, ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID);
	Status findObjectIDByPredicateAndSubject(ID predicateID, ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findObjectIDAndPredicateIDBySubject(ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID);
	Status findPredicateIDAndObjectIDBySubject(ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findPredicateIDAndSubjectIDByObject(ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID);
	Status findSubjectIDAndPredicateIDByObject(ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findPredicateIDBySubjectAndObject(ID subject, ID object, EntityIDBuffer* buffer);

	Status findPredicateIDBySubject(ID subject, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findPredicateIDByObject(ID object, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findSubjectIDByObject(ID object, EntityIDBuffer* buffer, ID minID, ID maxID);
	Status findObjectIDBySubject(ID subject, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findSubject(EntityIDBuffer* buffer, ID minID, ID maxID);
	Status findPredicate(EntityIDBuffer* buffer, ID minID, ID maxID);
	Status findObject(EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findSOByKnowBuffer(EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement);
	Status findOSByKnowBuffer(EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement);
    Status findSPByKnowBuffer(EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement);
    Status findPSByKnowBuffer(EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement);
    Status findOPByKnowBuffer(EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement);
    Status findPOByKnowBuffer(EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement);

	Status findSOByKnowBuffer(ID preID,EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement);
	Status findOSByKnowBuffer(ID preID,EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement);
    Status findSByKnowBuffer(ID preID,EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement);


private:
	Status findSubjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer);

	Status findObjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer);

	Status findObjectIDAndSubjectIDByPredicate(ID predicate, EntityIDBuffer* buffer);

	Status findSubjectIDByPredicateAndObject(ID predicateID, ID objectID, EntityIDBuffer* buffer);
	Status findObjectIDByPredicateAndSubject(ID predicateID, ID subjectID, EntityIDBuffer* buffer);

	Status findSubjectIDAndPredicateIDByPredicateAndObject(ID predicateID, ID objectID, EntityIDBuffer* buffer);
	Status findSubjectIDAndPredicateIDByPredicateAndObject(ID predicateID, ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID);
	Status findPredicateIDAndSubjectIDByPredicateAndObject(ID predicateID, ID objectID, EntityIDBuffer* buffer);

	Status findPredicateIDAndObjectIDByPredicateAndSubject(ID predicateID, ID subjectID, EntityIDBuffer* buffer);
	Status findObjectIDAndPredicateIDByPredicateAndSubject(ID predicateID, ID subjectID, EntityIDBuffer* buffer);
	Status findObjectIDAndPredicateIDByPredicateAndSubject(ID predicateID, ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID);

	Status findSubjectIDAndPredicateIDByObject(ID objectID, EntityIDBuffer* buffer);
	Status findObjectIDAndPredicateIDBySubject(ID subjectID, EntityIDBuffer *buffer);

	Status findSubjectIDByObject(ID object, EntityIDBuffer* buffer);
	Status findObjectIDBySubject(ID subject, EntityIDBuffer* buffer);

	void findSubjectIDAndPredicateIDByPredicateAndObjectTask(ID predicateID, ID objectID);
	void findSubjectIDAndPredicateIDByPredicateAndObjectTask(ID predicateID, ID objectID, ID minID, ID maxID);
	void findPredicateIDAndSubjectIDByPredicateAndObjectTask(ID predicateID, ID objectID);

	void findObjectIDAndPredicateIDByPredicateAndSubjectTask(ID predicateID, ID subjectID);
	void findObjectIDAndPredicateIDByPredicateAndSubjectTask(ID predicateID, ID subjectID, ID minID, ID maxID);
	void findPredicateIDAndObjectIDByPredicateAndSubjectTask(ID predicateID, ID subjectID);

	Status findSOByKnowBufferTask(ID* p, size_t length , EntityType knowElement,EntityIDBuffer* resultBuffer);
	Status findOSByKnowBufferTask(ID* p, size_t length , EntityType knowElement,EntityIDBuffer* resultBuffer);
	Status findSPByKnowBufferTask(ID* p, size_t length , EntityType knowElement,EntityIDBuffer* resultBuffer);
	Status findPSByKnowBufferTask(ID* p, size_t length , EntityType knowElement,EntityIDBuffer* resultBuffer);
	Status findOPByKnowBufferTask(ID* p, size_t length , EntityType knowElement,EntityIDBuffer* resultBuffer);
	Status findPOByKnowBufferTask(ID* p, size_t length , EntityType knowElement,EntityIDBuffer* resultBuffer);

	Status findSOByKnowBufferTask1(ID preID,ID* p, size_t length , EntityType knowElement,EntityIDBuffer* resultBuffer);
	Status findOSByKnowBufferTask1(ID preID,ID* p, size_t length , EntityType knowElement,EntityIDBuffer* resultBuffer);
	Status findSByKnowBufferTask1(ID preID,ID* p, size_t length ,EntityIDBuffer* resultBuffer);
#ifdef TEST_TIME 
	void printTime() {
		indexTimer.printTime("index timer");
		readTimer.printTime("read time");
	}

#endif
};
#endif /* FINDENTITYID_H_ */
