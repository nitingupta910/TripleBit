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

#include "FindEntityID.h"
#include "SortMergeJoin.h"
#include "../MemoryBuffer.h"
#include "../URITable.h"
#include "../PredicateTable.h"
#include "../TripleBitRepository.h"
#include "../BitmapBuffer.h"
#include "../EntityIDBuffer.h"
#include "../HashIndex.h"
#include "../StatisticsBuffer.h"

FindEntityID::FindEntityID(TripleBitRepository* repo) {
	// TODO Auto-generated constructor stub
	bitmap = repo->getBitmapBuffer();
	UriTable = repo->getURITable();
	preTable = repo->getPredicateTable();
	spStatBuffer = (TwoConstantStatisticsBuffer*)repo->getStatisticsBuffer(StatisticsBuffer::SUBJECTPREDICATE_STATIS);
	opStatBuffer = (TwoConstantStatisticsBuffer*)repo->getStatisticsBuffer(StatisticsBuffer::OBJECTPREDICATE_STATIS);
	sStatBuffer = (OneConstantStatisticsBuffer*)repo->getStatisticsBuffer(StatisticsBuffer::SUBJECT_STATIS);
	oStatBuffer = (OneConstantStatisticsBuffer*)repo->getStatisticsBuffer(StatisticsBuffer::OBJECT_STATIS);

	XTemp = new EntityIDBuffer();
	XYTemp = new EntityIDBuffer();
	tempBuffer1 = new EntityIDBuffer();
	tempBuffer2 = new EntityIDBuffer();
	pthread_mutex_init(&mergeBufferMutex,NULL);
}

FindEntityID::~FindEntityID() {
	// TODO Auto-generated destructor stub
	if(XTemp != NULL)
		delete XTemp;
	XTemp = NULL;

	if(XYTemp != NULL)
		delete XYTemp;
	XYTemp = NULL;

	if(tempBuffer1 != NULL)
		delete tempBuffer1;
	tempBuffer1 = NULL;

	if(tempBuffer2 != NULL)
		delete tempBuffer2;
	tempBuffer2 = NULL;

	pthread_mutex_destroy(&mergeBufferMutex);
}

Status FindEntityID::findSubjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	if(minID == 0 && maxID == UINT_MAX)
		return this->findSubjectIDByPredicate(predicateID, buffer);
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	register ID x, y;
	register ID key;
	const uchar* startPtr, *limit, *reader;

	unsigned offset;
	Status s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(minID, 1, offset);
	if(s == OK) {
		startPtr =  bitmap->getChunkManager(predicateID, 0)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
		reader = startPtr;
		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if(x < minID) {
				continue;
			} else if( x <= maxID ) {
				XTemp->insertID(x);
			} else {
				break;
			}
			startPtr = Chunk::skipId(startPtr, 1); //skip y;
		}
	}

	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(minID, 2, offset);
	if(s == OK) {
		startPtr =  bitmap->getChunkManager(predicateID, 0)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);
		reader = startPtr;
		for (; reader < limit;) {
			reader = Chunk::readXId(reader, x);
			reader = Chunk::readYId(reader, y);
			key = x + y;
			if(key < minID) {
				continue;
			} else if( key <= maxID) {
				XYTemp->insertID(key);
			} else {
				break;
			}
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);

	return OK;
}

Status FindEntityID::findSubjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer)
{
#ifdef DEBUG
	cout << __FUNCTION__ << endl;
#endif
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit, *reader;

	startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1);
	limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
	reader = startPtr;
	for (; startPtr < limit;) {
		startPtr = Chunk::readXId(startPtr, x);
		XTemp->insertID(x);
		startPtr = Chunk::skipId(startPtr, 1); //skip y;
	}

	startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2);
	limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);
	reader = startPtr;
	for (; reader < limit;) {
		reader = Chunk::readXId(reader, x);
		reader = Chunk::readYId(reader, y);
		XYTemp->insertID(x + y);
	}

	buffer->mergeBuffer(XTemp, XYTemp);

	return OK;
}

Status FindEntityID::findObjectIDAndSubjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer)
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1);
	limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);
	for (;startPtr < limit;) {
		startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
		XTemp->insertID(x);
		XTemp->insertID(x + y);
	}

	startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2);
	limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);
	for (; startPtr < limit; ) {
		startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
		XYTemp->insertID(x + y);
		XYTemp->insertID(x);
	}
	buffer->mergeBuffer(XTemp, XYTemp);

	return OK;
}

Status FindEntityID::findSubjectIDAndObjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer)
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1);
	limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
	for (;startPtr < limit;) {
		startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
		XTemp->insertID(x);
		XTemp->insertID(x + y);
	}
	startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2);
	limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

	for (; startPtr < limit; ) {
		startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
		XYTemp->insertID(x + y);
		XYTemp->insertID(x);
	}

	buffer->mergeBuffer(XTemp, XYTemp);

	return OK;
}

Status FindEntityID::findObjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	if(minID == 0 && maxID == UINT_MAX)
		return this->findObjectIDByPredicate(predicateID, buffer);
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	unsigned offset;

	Status s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(minID, 1, offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);
			if (x > maxID)
				break;
			else if (x <= maxID)
				XTemp->insertID(x);
			startPtr = Chunk::skipId(startPtr, 1); //skip y;
		}
	}

	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(minID, 2, offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);
			startPtr = Chunk::readYId(startPtr, y);
			if (x + y > maxID)
				break;
			else if (x + y <= maxID)
				XYTemp->insertID(x + y);
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);

	return OK;
}

Status FindEntityID::findObjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer)
{
	buffer->setIDCount(1);
	buffer->setSortKey(0);
	XTemp->empty();
	XYTemp->empty();

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1);
	limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

	for (; startPtr < limit;) {
		startPtr = Chunk::readXId(startPtr, x);
		XTemp->insertID(x);
		startPtr = Chunk::skipId(startPtr, 1); //skip y;
	}

	startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2);
	limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

	for (; startPtr < limit;) {
		startPtr = Chunk::readXId(startPtr, x);
		startPtr = Chunk::readYId(startPtr, y);
		XYTemp->insertID(x + y);
	}

	buffer->mergeBuffer(XTemp, XYTemp);

	return OK;
}

Status FindEntityID::findSubjectIDByPredicateAndObject(ID predicateID, ID objectID, EntityIDBuffer* buffer)
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	Status s;
	register ID x, y;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XYTemp->setIDCount(1);

	unsigned int offset;
	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 1, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

		for(;startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if(x != objectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			XYTemp->insertID(x + y);
		}
	}

	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 2, offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if(x + y != objectID)
				break;
			XTemp->insertID(x);
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
	return OK;
}

Status FindEntityID::findSubjectIDByPredicateAndObject(ID predicateID, ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	Status s;
	register ID x, y;
	register ID key;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XYTemp->setIDCount(1);

	unsigned int offset;
	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 1, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

		for(;startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if(x != objectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			key = x + y;
			if(key < minID) {
				continue;
			}else if (key <= maxID) {
				XYTemp->insertID(key);
			} else if (key > maxID) {
				break;
			}
		}
	}

	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 2, offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if(x + y != objectID)
				break;
			if(x < minID) {
				continue;
			} else if (x <= maxID) {
				XTemp->insertID(x);
			} else if (x > maxID) {
				break;
			}
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
	return OK;
}

Status FindEntityID::findObjectIDByPredicateAndSubject(ID predicateID, ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	register ID x, y, key;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	Status s;

	unsigned int typeID;
	unsigned int offset;

	typeID = 1;

	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID, typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
		for(;startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if(x != subjectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			key = x + y;
			if (key <  minID) {
				continue;
			} else if (key <= maxID) {
				XYTemp->insertID(x + y);
			} else {
				break;
			}
		}
	}

	typeID = 2;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID, typeID, offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if(x + y != subjectID)
				break;
			if (x < minID) {
				continue;
			} else if ( x <= maxID) {
				XTemp->insertID(x);
			} else {
				break;
			}
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
	return OK;
}

Status FindEntityID::findObjectIDByPredicateAndSubject(ID predicateID, ID subjectID, EntityIDBuffer* buffer)
{
	register ID x, y;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);

	XTemp->setIDCount(1);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(1);
	XYTemp->setSortKey(0);

	Status s;

	unsigned int typeID;
	unsigned int offset;

	typeID = 1;

	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID, typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
		for(;startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if(x != subjectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			XYTemp->insertID(x + y);
		}
	}

	typeID = 2;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID, typeID, offset);
	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if(x + y != subjectID)
				break;
			XTemp->insertID(x);
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
	return OK;
}

Status FindEntityID::findSubjectIDAndObjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer, ID min, ID max,unsigned maxNum)
{
	if(min == 0 && max == UINT_MAX)
		return this->findSubjectIDAndObjectIDByPredicate(predicateID, buffer);
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	XTemp->empty();
	XYTemp->empty();

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	unsigned int typeIDMin;
	unsigned offsetMin;
	Status s;

	ChunkManager *manager;
	typeIDMin = 1;
	manager = bitmap->getChunkManager(predicateID, 0);

	s = manager->getChunkPosByID(min, typeIDMin, offsetMin);

	if (s == OK) {
		startPtr = manager->getStartPtr(1) + offsetMin;
		limit = manager->getEndPtr(1);
		for (; startPtr < limit; ) {
			startPtr = Chunk::readXId(startPtr, x);
			if (x > max)
				goto END;

			startPtr = Chunk::readYId(startPtr, y);
			XTemp->insertID(x);
			XTemp->insertID(x + y);
			if(XTemp->getSize() > maxNum){
				return TOO_MUCH;
			}
		}
	}

END:
	typeIDMin = 2;

	s = manager->getChunkPosByID(min, typeIDMin, offsetMin);
	if (s == OK) {
		startPtr = manager->getStartPtr(2) + offsetMin;
		limit = manager->getEndPtr(2);
		for (; startPtr < limit; ) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if (x + y > max)
				goto END1;

			XYTemp->insertID(x + y);
			XYTemp->insertID(x);
			if (XYTemp->getSize() > maxNum) {
				return TOO_MUCH;
			}
		}
	}
END1:
	buffer->mergeBuffer(XTemp, XYTemp);

	return OK;
}

Status FindEntityID::findObjectIDAndSubjectIDByPredicate(ID predicateID, EntityIDBuffer* buffer, ID minID, ID maxID, unsigned maxNum)
{
	if(minID == 0 && maxID == UINT_MAX)
		return this->findObjectIDAndSubjectIDByPredicate(predicateID, buffer);
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	XTemp->empty();
	XYTemp->empty();

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	register ID x, y;
	const uchar* startPtr, *limit;

	unsigned int typeIDMin;
	unsigned offsetMin;
	Status s;

	ChunkManager *manager;
	typeIDMin = 1;
	manager = bitmap->getChunkManager(predicateID, 1);

	s = manager->getChunkPosByID(minID, typeIDMin, offsetMin);

	if (s == OK) {
		startPtr = manager->getStartPtr(1) + offsetMin;
		limit = manager->getEndPtr(1);
		for (; startPtr < limit; ) {
			startPtr = Chunk::readXId(startPtr, x);
			if (x > maxID)
				goto END;
			startPtr = Chunk::readYId(startPtr, y);
			XTemp->insertID(x);
			XTemp->insertID(x + y);
			if(XTemp->getSize() > maxNum){
                return TOO_MUCH;
            }
		}
	}

END:
	typeIDMin = 2;

	s = manager->getChunkPosByID(minID, typeIDMin, offsetMin);
	if (s == OK) {
		startPtr = manager->getStartPtr(2) + offsetMin;
		limit = manager->getEndPtr(2);

		for (; startPtr < limit; ) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if (x + y > maxID)
				goto END1;

			XYTemp->insertID(x + y);
			XYTemp->insertID(x);
			if (XYTemp->getSize() > maxNum) {
                return TOO_MUCH;
            }
		}
	}
END1:
	buffer->mergeBuffer(XTemp, XYTemp);
	return OK;
}

Status FindEntityID::findSubjectIDAndPredicateIDByPredicateAndObject(ID predicateID, ID objectID, EntityIDBuffer* buffer)
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	Status s;
	register ID x, y;
	register ID key;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XYTemp->setIDCount(2);

	unsigned int offset;
	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 1, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

		for(;startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if(x != objectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			key = x + y;
			XYTemp->insertID(key);
			XYTemp->insertID(predicateID);
		}
	}

	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 2, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if(x + y != objectID)
				break;
			XTemp->insertID(x);
			XTemp->insertID(predicateID);
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
	return OK;
}

Status FindEntityID::findSubjectIDAndPredicateIDByPredicateAndObject(ID predicateID, ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	Status s;
	register ID x, y;
	register ID key;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XYTemp->setIDCount(2);

	unsigned int offset;
	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 1, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

		for(;startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if(x != objectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			key = x + y;
			if(key < minID) {
				continue;
			}else if (key <= maxID) {
				XYTemp->insertID(key);
				XYTemp->insertID(predicateID);
			} else if (key > maxID) {
				break;
			}
		}
	}

	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 2, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if(x + y != objectID)
				break;
			if(x < minID) {
				continue;
			} else if (x <= maxID) {
				XTemp->insertID(x);
				XTemp->insertID(predicateID);
			} else if (x > maxID) {
				break;
			}
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
	return OK;
}

Status FindEntityID::findPredicateIDAndSubjectIDByPredicateAndObject(ID predicateID, ID objectID, EntityIDBuffer* buffer)
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	Status s;
	register ID x, y;
	register ID key;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XYTemp->setIDCount(2);

	unsigned int offset;
	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 1, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(1);

		for(;startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if(x != objectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			key = x + y;
			XYTemp->insertID(predicateID);
			XYTemp->insertID(key);
		}
	}

	s = bitmap->getChunkManager(predicateID, 1)->getChunkPosByID(objectID, 2, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 1)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 1)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);
			if(x + y != objectID)
				break;
			XTemp->insertID(predicateID);
			XTemp->insertID(x);
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
	return OK;
}

Status FindEntityID::findPredicateIDAndObjectIDByPredicateAndSubject(ID predicateID, ID subjectID, EntityIDBuffer* buffer)
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	register ID x, y;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	Status s;
	unsigned int typeID, offset;
	typeID = 1;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID, typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
		for(;startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if(x != subjectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			XYTemp->insertID(predicateID);
			XYTemp->insertID(x + y);
		}
	}

	typeID = 2;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID, typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);

			if(x + y != subjectID)
				break;
			XTemp->insertID(predicateID);
			XTemp->insertID(x);
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
	return OK;
}

Status FindEntityID::findObjectIDAndPredicateIDByPredicateAndSubject(ID predicateID, ID subjectID, EntityIDBuffer* buffer)
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	register ID x, y;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	Status s;
	unsigned int typeID, offset;
	typeID = 1;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID, typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
		for(;startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if(x != subjectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			XYTemp->insertID(x + y);
			XYTemp->insertID(predicateID);
		}
	}

	typeID = 2;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID, typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);

			if(x + y != subjectID)
				break;
			XTemp->insertID(x);
			XTemp->insertID(predicateID);
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
	return OK;
}

Status FindEntityID::findObjectIDAndPredicateIDByPredicateAndSubject(ID predicateID, ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
#ifdef DEBUG
	cout<<__FUNCTION__<<endl;
#endif
	register ID x, y, key;
	const uchar* startPtr, *limit;
	EntityIDBuffer* XTemp = new EntityIDBuffer();
	EntityIDBuffer* XYTemp = new EntityIDBuffer();
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(2);
	buffer->setSortKey(0);

	XTemp->setIDCount(2);
	XTemp->setSortKey(0);
	XYTemp->setIDCount(2);
	XYTemp->setSortKey(0);

	Status s;
	unsigned int typeID, offset;
	typeID = 1;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID, typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(1) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(1);
		for(;startPtr < limit;) {
			startPtr = Chunk::readXId(startPtr, x);

			if(x != subjectID)
				break;
			startPtr = Chunk::readYId(startPtr, y);
			key = x + y;
			if(key < minID) {
				continue;
			}else if (key <= maxID) {
				XYTemp->insertID(key);
				XYTemp->insertID(predicateID);
			} else if (key > maxID) {
				break;
			}
		}
	}

	typeID = 2;
	s = bitmap->getChunkManager(predicateID, 0)->getChunkPosByID(subjectID, typeID, offset);

	if (s == OK) {
		startPtr = bitmap->getChunkManager(predicateID, 0)->getStartPtr(2) + offset;
		limit = bitmap->getChunkManager(predicateID, 0)->getEndPtr(2);

		for (; startPtr < limit;) {
			startPtr = Chunk::readYId(Chunk::readXId(startPtr, x), y);

			if(x + y != subjectID)
				break;
			if(x < minID) {
				continue;
			} else if (x <= maxID) {
				XTemp->insertID(x);
				XTemp->insertID(predicateID);
			} else if (x > maxID) {
				break;
			}
		}
	}

	buffer->mergeBuffer(XTemp, XYTemp);
	delete XTemp;
	delete XYTemp;
	return OK;
}

void FindEntityID::findSubjectIDAndPredicateIDByPredicateAndObjectTask(ID predicateID, ID objectID)
{
	EntityIDBuffer *curBuffer = new EntityIDBuffer();
	this->findSubjectIDAndPredicateIDByPredicateAndObject(predicateID, objectID, curBuffer);
	if (curBuffer->getSize() > 0) {
		pthread_mutex_lock(&mergeBufferMutex);
		tempBuffer2->mergeBuffer(tempBuffer1, curBuffer);
		EntityIDBuffer::swapBuffer(tempBuffer1, tempBuffer2);
		pthread_mutex_unlock(&mergeBufferMutex);
	}
	delete curBuffer;
}

void FindEntityID::findSubjectIDAndPredicateIDByPredicateAndObjectTask(ID predicateID, ID objectID, ID minID, ID maxID)
{
	EntityIDBuffer *curBuffer = new EntityIDBuffer();
	this->findSubjectIDAndPredicateIDByPredicateAndObject(predicateID, objectID, curBuffer, minID, maxID);
	if (curBuffer->getSize() > 0) {
		pthread_mutex_lock(&mergeBufferMutex);
		tempBuffer2->mergeBuffer(tempBuffer1, curBuffer);
		EntityIDBuffer::swapBuffer(tempBuffer1, tempBuffer2);
		pthread_mutex_unlock(&mergeBufferMutex);
	}
	delete curBuffer;
}

void FindEntityID::findPredicateIDAndSubjectIDByPredicateAndObjectTask(ID predicateID, ID objectID)
{
	EntityIDBuffer *curBuffer = new EntityIDBuffer();
	this->findPredicateIDAndSubjectIDByPredicateAndObject(predicateID, objectID, curBuffer);
	if (curBuffer->getSize() > 0) {
		pthread_mutex_lock(&mergeBufferMutex);
		tempBuffer2->mergeBuffer(tempBuffer1, curBuffer);
		EntityIDBuffer::swapBuffer(tempBuffer1, tempBuffer2);
		pthread_mutex_unlock(&mergeBufferMutex);
	}
	delete curBuffer;
}

void FindEntityID::findObjectIDAndPredicateIDByPredicateAndSubjectTask(ID predicateID, ID subjectID)
{
	EntityIDBuffer *curBuffer = new EntityIDBuffer();
	this->findObjectIDAndPredicateIDByPredicateAndSubject(predicateID, subjectID, curBuffer);
	if (curBuffer->getSize() > 0) {
		pthread_mutex_lock(&mergeBufferMutex);
		tempBuffer2->mergeBuffer(tempBuffer1, curBuffer);
		EntityIDBuffer::swapBuffer(tempBuffer1, tempBuffer2);
		pthread_mutex_unlock(&mergeBufferMutex);
	}
	delete curBuffer;
}

void FindEntityID::findObjectIDAndPredicateIDByPredicateAndSubjectTask(ID predicateID, ID subjectID, ID minID, ID maxID)
{
	EntityIDBuffer *curBuffer = new EntityIDBuffer();
	this->findObjectIDAndPredicateIDByPredicateAndSubject(predicateID, subjectID, curBuffer, minID, maxID);
	if (curBuffer->getSize() > 0) {
		pthread_mutex_lock(&mergeBufferMutex);
		tempBuffer2->mergeBuffer(tempBuffer1, curBuffer);
		EntityIDBuffer::swapBuffer(tempBuffer1, tempBuffer2);
		pthread_mutex_unlock(&mergeBufferMutex);
	}
	delete curBuffer;
}

void FindEntityID::findPredicateIDAndObjectIDByPredicateAndSubjectTask(ID predicateID, ID subjectID)
{
	EntityIDBuffer *curBuffer = new EntityIDBuffer();
	this->findPredicateIDAndObjectIDByPredicateAndSubject(predicateID, subjectID, curBuffer);
	if (curBuffer->getSize() > 0) {
		pthread_mutex_lock(&mergeBufferMutex);
		tempBuffer2->mergeBuffer(tempBuffer1, curBuffer);
		EntityIDBuffer::swapBuffer(tempBuffer1, tempBuffer2);
		pthread_mutex_unlock(&mergeBufferMutex);
	}
	delete curBuffer;
}

Status FindEntityID::findSubjectIDAndPredicateIDByObject(ID objectID, EntityIDBuffer* buffer)
{
	EntityIDBuffer preBuffer;
	opStatBuffer->getPredicatesByID(objectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize(), i = 0;
	if(size == 0)return NOT_FOUND;

	buffer->setIDCount(2);
	buffer->setSortKey(0);
	tempBuffer2->setIDCount(2);
	tempBuffer2->setSortKey(0);

	do{
		this->findSubjectIDAndPredicateIDByPredicateAndObject(preBuffer[i], objectID, tempBuffer1);
		i++;
	}while(tempBuffer1->getSize() == 0 && i != size);

	for (; i != size; i++) {
		CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findSubjectIDAndPredicateIDByPredicateAndObjectTask,this,preBuffer[i], objectID));
	}
	CThreadPool::getInstance().Wait();

	buffer->operator =(tempBuffer1);
	return OK;
}

Status FindEntityID::findSubjectIDAndPredicateIDByObject(ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	if(minID == 0 && maxID == UINT_MAX)
		return findSubjectIDAndPredicateIDByObject(objectID, buffer);

	EntityIDBuffer preBuffer;
	opStatBuffer->getPredicatesByID(objectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize(), i = 0;
	if(size == 0)return NOT_FOUND;

	buffer->setIDCount(2);
	buffer->setSortKey(0);
	tempBuffer2->setIDCount(2);
	tempBuffer2->setSortKey(0);

	do{
		this->findSubjectIDAndPredicateIDByPredicateAndObject(preBuffer[i], objectID, tempBuffer1, minID, maxID);
		i++;
	}while(tempBuffer1->getSize() == 0 && i != size);

	for (; i != size; i++) {
		CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findSubjectIDAndPredicateIDByPredicateAndObjectTask,this,preBuffer[i], objectID, minID, maxID));
	}
	CThreadPool::getInstance().Wait();

	buffer->operator =(tempBuffer1);
	return OK;
}

Status FindEntityID::findPredicateIDAndSubjectIDByObject(ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	EntityIDBuffer preBuffer;
	opStatBuffer->getPredicatesByID(objectID, &preBuffer, minID, maxID);
	size_t size = preBuffer.getSize(), i = 0;
	if(size == 0)return NOT_FOUND;

	buffer->setIDCount(2);
	buffer->setSortKey(0);
	tempBuffer2->setIDCount(2);
	tempBuffer2->setSortKey(0);

	do{
		this->findPredicateIDAndSubjectIDByPredicateAndObject(preBuffer[i], objectID, tempBuffer1);
		i++;
	}while(tempBuffer1->getSize() == 0 && i != size);

	for (; i != size; i++) {
		CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findPredicateIDAndSubjectIDByPredicateAndObjectTask,this,preBuffer[i], objectID));
	}
	CThreadPool::getInstance().Wait();

	buffer->operator =(tempBuffer1);
	return OK;
}

Status FindEntityID::findObjectIDAndPredicateIDBySubject(ID subjectID, EntityIDBuffer *buffer)
{
	EntityIDBuffer preBuffer;
	spStatBuffer->getPredicatesByID(subjectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize(), i = 0;
	if(size == 0)return NOT_FOUND;

	buffer->setIDCount(2);
	buffer->setSortKey(0);
	tempBuffer2->setIDCount(2);
	tempBuffer2->setSortKey(0);

	do{
		this->findObjectIDAndPredicateIDByPredicateAndSubject(preBuffer[i], subjectID, tempBuffer1);
		i++;
	}while(tempBuffer1->getSize() == 0 && i != size);

	for (; i != size; i++) {
		CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findObjectIDAndPredicateIDByPredicateAndSubjectTask,this,preBuffer[i], subjectID));
	}
	CThreadPool::getInstance().Wait();

	buffer->operator =(tempBuffer1);
	return OK;
}

Status FindEntityID::findObjectIDAndPredicateIDBySubject(ID subjectID, EntityIDBuffer *buffer, ID minID, ID maxID)
{
	if(minID == 0 && maxID == UINT_MAX)
		return findObjectIDAndPredicateIDBySubject(subjectID, buffer);

	EntityIDBuffer preBuffer;
	spStatBuffer->getPredicatesByID(subjectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize(), i = 0;
	if(size == 0)return NOT_FOUND;

	buffer->setIDCount(2);
	buffer->setSortKey(0);
	tempBuffer2->setIDCount(2);
	tempBuffer2->setSortKey(0);

	do{
		this->findObjectIDAndPredicateIDByPredicateAndSubject(preBuffer[i], subjectID, tempBuffer1, minID, maxID);
		i++;
	}while(tempBuffer1->getSize() == 0 && i != size);

	for (; i != size; i++) {
		CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findObjectIDAndPredicateIDByPredicateAndSubjectTask,this,preBuffer[i], subjectID, minID, maxID));
	}
	CThreadPool::getInstance().Wait();

	buffer->operator =(tempBuffer1);
	return OK;
}

Status FindEntityID::findPredicateIDAndObjectIDBySubject(ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	EntityIDBuffer preBuffer;
	spStatBuffer->getPredicatesByID(subjectID, &preBuffer, minID, maxID);
	size_t size = preBuffer.getSize(), i = 0;
	if(size == 0)return NOT_FOUND;

	buffer->setIDCount(2);
	buffer->setSortKey(0);
	tempBuffer2->setIDCount(2);
	tempBuffer2->setSortKey(0);

	do{
		this->findPredicateIDAndObjectIDByPredicateAndSubject(preBuffer[i], subjectID, tempBuffer1);
		i++;
	}while(tempBuffer1->getSize() == 0 && i != size);

	for (; i != size; i++) {
		CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findPredicateIDAndObjectIDByPredicateAndSubjectTask,this,preBuffer[i], subjectID));
	}
	CThreadPool::getInstance().Wait();

	buffer->operator =(tempBuffer1);
	return OK;
}

Status FindEntityID::findPredicateIDBySubjectAndObject(ID subjectID, ID objectID, EntityIDBuffer* buffer)
{
	XTemp->empty();
	XYTemp->empty();

	buffer->setIDCount(1);
	buffer->setSortKey(0);
	Status s;
	if( (s = spStatBuffer->getPredicatesByID(subjectID, XTemp, 0, UINT_MAX)) != OK )
		return s;
	if((s = opStatBuffer->getPredicatesByID(objectID, XYTemp, 0, UINT_MAX)) != OK)
		return s;

	SortMergeJoin join;
	join.Join(XTemp, XYTemp, 1, 1, false);
	buffer = XTemp;

	return s;
}

Status FindEntityID::findPredicateIDByObject(ID objectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	buffer->setIDCount(1);
	buffer->setSortKey(0);
	Status s = opStatBuffer->getPredicatesByID(objectID, buffer, minID, maxID);
	return s;
}

Status FindEntityID::findPredicateIDBySubject(ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	buffer->setIDCount(1);
	buffer->setSortKey(0);
	return spStatBuffer->getPredicatesByID(subjectID, buffer, minID, maxID);
}

Status FindEntityID::findObjectIDBySubject(ID subjectID, EntityIDBuffer *buffer)
{
	buffer->setIDCount(1);
	buffer->setSortKey(0);
	EntityIDBuffer preBuffer;
	spStatBuffer->getPredicatesByID(subjectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize();
	if(size == 0)return NOT_FOUND;

	size_t i = 0;
	EntityIDBuffer *tempBuffer, *curBuffer, *entBuffer;
	entBuffer = new EntityIDBuffer();
	tempBuffer = new EntityIDBuffer();
	curBuffer = new EntityIDBuffer();

	do {
		this->findObjectIDByPredicateAndSubject(preBuffer[i], subjectID,entBuffer);
	    i++;
	} while (entBuffer->getSize() == 0 && i != size);

	for (; i != size; i++) {
	    this->findObjectIDByPredicateAndSubject(preBuffer[i], subjectID,curBuffer);
	    if (curBuffer->getSize() > 0) {
	        EntityIDBuffer::mergeSingleBuffer(tempBuffer, entBuffer, curBuffer);
	        EntityIDBuffer::swapBuffer(entBuffer, tempBuffer);
	    }
	}

	entBuffer->uniqe();
 	buffer->operator =(entBuffer);
	delete tempBuffer;
	delete curBuffer;
	delete entBuffer;
	return OK;
}

Status FindEntityID::findObjectIDBySubject(ID subjectID, EntityIDBuffer* buffer, ID minID, ID maxID)
{
	buffer->setIDCount(1);
    buffer->setSortKey(0);
    EntityIDBuffer preBuffer;
    spStatBuffer->getPredicatesByID(subjectID, &preBuffer, 0, UINT_MAX);
    size_t size = preBuffer.getSize();
    if(size == 0)return NOT_FOUND;

    size_t i = 0;
    EntityIDBuffer *tempBuffer, *curBuffer, *entBuffer;
    entBuffer = new EntityIDBuffer();
    tempBuffer = new EntityIDBuffer();
    curBuffer = new EntityIDBuffer();

    do {
        this->findObjectIDByPredicateAndSubject(preBuffer[i], subjectID,entBuffer, minID, maxID);
        i++;
    } while (entBuffer->getSize() == 0 && i != size);

    for (; i != size; i++) {
        this->findObjectIDByPredicateAndSubject(preBuffer[i], subjectID,curBuffer, minID, maxID);
        if (curBuffer->getSize() > 0) {
            EntityIDBuffer::mergeSingleBuffer(tempBuffer, entBuffer, curBuffer);
            EntityIDBuffer::swapBuffer(entBuffer, tempBuffer);
        }
    }
    entBuffer->uniqe();
    buffer->operator =(entBuffer);
    delete tempBuffer;
    delete curBuffer;
    delete entBuffer;
    return OK;
}

Status FindEntityID::findSubjectIDByObject(ID objectID, EntityIDBuffer* buffer)
{
	buffer->setIDCount(1);
	buffer->setSortKey(0);
	EntityIDBuffer preBuffer;
	opStatBuffer->getPredicatesByID(objectID, &preBuffer, 0, UINT_MAX);
	size_t size = preBuffer.getSize();
	if(size == 0)return NOT_FOUND;

	size_t i = 0;
	EntityIDBuffer *tempBuffer, *curBuffer, *entBuffer;
	entBuffer = new EntityIDBuffer();
	tempBuffer = new EntityIDBuffer();
	curBuffer = new EntityIDBuffer();

	do {
		this->findSubjectIDByPredicateAndObject(preBuffer[i], objectID,entBuffer);
		i++;
	} while (entBuffer->getSize() == 0 && i != size);

	for (; i != size; i++) {
		this->findSubjectIDByPredicateAndObject(preBuffer[i], objectID,curBuffer);
		if (curBuffer->getSize() > 0) {
			EntityIDBuffer::mergeSingleBuffer(tempBuffer, entBuffer, curBuffer);
			EntityIDBuffer::swapBuffer(entBuffer, tempBuffer);
		}
	}

	entBuffer->uniqe();
	buffer->operator =(entBuffer);
	delete tempBuffer;
	delete curBuffer;
	delete entBuffer;
	return OK;
}

Status FindEntityID::findSubjectIDByObject(ID objectID, EntityIDBuffer* buffer,	ID minID, ID maxID) {
	buffer->setIDCount(1);
	buffer->setSortKey(0);
	EntityIDBuffer preBuffer;

	opStatBuffer->getPredicatesByID(objectID, &preBuffer, 0, UINT_MAX);

	size_t size = preBuffer.getSize();
	if(size == 0)return NOT_FOUND;

	size_t i = 0;
	EntityIDBuffer *tempBuffer, *curBuffer, *entBuffer;
	entBuffer = new EntityIDBuffer();
	tempBuffer = new EntityIDBuffer();
	curBuffer = new EntityIDBuffer();

	do {
		this->findSubjectIDByPredicateAndObject(preBuffer[i], objectID,entBuffer, minID, maxID);
		i++;
	} while (entBuffer->getSize() == 0 && i != size);

	for (; i != size; i++) {
		this->findSubjectIDByPredicateAndObject(preBuffer[i], objectID,curBuffer, minID, maxID);
		if (curBuffer->getSize() > 0) {
			EntityIDBuffer::mergeSingleBuffer(tempBuffer, entBuffer, curBuffer);
			EntityIDBuffer::swapBuffer(entBuffer, tempBuffer);
		}
	}
	entBuffer->uniqe();
	buffer->operator =(entBuffer);
	delete tempBuffer;
	delete curBuffer;
	delete entBuffer;
	return OK;
}

Status FindEntityID::findSubject(EntityIDBuffer* buffer, ID minID, ID maxID)
{
	buffer->setIDCount(1);
	buffer->setSortKey(0);

	//return sStatBuffer->getIDs(buffer, minID, maxID);
	return sStatBuffer->getEntityIDs(buffer, minID, maxID);
}

/**
 * the predicate's id is continuous.
 */
Status FindEntityID::findPredicate(EntityIDBuffer* buffer, ID minID, ID maxID)
{
	for(ID id = minID; id <= maxID; id++)
		buffer->insertID(id);
	return OK;
}

Status FindEntityID::findObject(EntityIDBuffer* buffer, ID minID, ID maxID)
{
	buffer->setIDCount(1);
	buffer->setSortKey(0);

	//return oStatBuffer->getIDs(buffer, minID, maxID);
	return oStatBuffer->getEntityIDs(buffer, minID, maxID);
}

//FINDSOBYNONE
Status FindEntityID::findSOByKnowBuffer(EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement){
	buffer->setIDCount(2);
	buffer->setSortKey(10);//unsorted
	if(knowBuffer->sorted == false)
		return ERROR;
	if( knowBuffer->getSize() == 0){
		return ERROR;
	}
	else{
		EntityIDBuffer transBuffer;
		ID key=0,lastKey=0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		for(unsigned i = 0;i < knowBuffer->getSize();i++){
			key = *(p+i*IDCount + sortKey);
			if(key > lastKey){
				transBuffer.insertID(key);
				lastKey = key;
			}
		}
		size_t size = transBuffer.getSize();
		unsigned chunkCount = THREAD_NUMBER;
		size_t chunkSize = size / chunkCount;
		size_t chunkLeft = size % chunkCount;
		p = transBuffer.getBuffer();
		
		if(chunkSize){
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft){
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findSOByKnowBufferTask,this,p, chunkSize+1,knowElement,&tempBuffer[i]));
					p += (chunkSize+1);
				}
				else{
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findSOByKnowBufferTask,this,p, chunkSize, knowElement,&tempBuffer[i]));
					p +=  chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkCount; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}else{
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findSOByKnowBufferTask,this,p, 1,knowElement,&tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkLeft; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
		buffer->sort(1);
	}
	return OK;
}

Status FindEntityID::findSOByKnowBufferTask(ID* p, size_t length , EntityType knowElement,EntityIDBuffer* resultBuffer){
	EntityIDBuffer tempBuffer;
	unsigned i=0,j=0;
	resultBuffer->setIDCount(2);
	resultBuffer->setSortKey(1);
	if(knowElement == SUBJECT){
		for (i=0; i < length; i++) {
			ID key = *(p + i);
	        findObjectIDBySubject(key, &tempBuffer, 0, UINT_MAX);
	        unsigned tempSize = tempBuffer.getSize();
	        for(j=0;j < tempSize; j++){
	            resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j]);
	        }
	    }
	}
	else if (knowElement == OBJECT) {
		for (i=0; i < length; i++) {
			ID key = *(p + i);
			findSubjectIDByObject(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for(j=0;j < tempSize; j++){
				resultBuffer->insertID((tempBuffer)[j]);
				resultBuffer->insertID(key);
			}
		}
	}

	return OK;
}

//FINDOSBYNONE
Status FindEntityID::findOSByKnowBuffer(EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement){
    buffer->setIDCount(2);
    buffer->setSortKey(10);//unsorted
    if(knowBuffer->sorted == false)
        return ERROR;
    if( knowBuffer->getSize() == 0){
        return ERROR;
    }
    else{
        EntityIDBuffer transBuffer;
        ID key=0,lastKey=0;
        ID * p = knowBuffer->getBuffer();
        int IDCount = knowBuffer->getIDCount();
        int sortKey = knowBuffer->getSortKey();
        for(unsigned i = 0;i < knowBuffer->getSize();i++){
            key = *(p+i*IDCount + sortKey);
            if(key > lastKey){
                transBuffer.insertID(key);
                lastKey = key;
            }
        }
        size_t size = transBuffer.getSize();
        unsigned chunkCount = THREAD_NUMBER;
        size_t chunkSize = size / chunkCount;
        size_t chunkLeft = size % chunkCount;
        p = transBuffer.getBuffer();
		
		if(chunkSize){
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft){
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findOSByKnowBufferTask,this,p, chunkSize+1,knowElement,&tempBuffer[i]));
					p += (chunkSize+1);
				}
				else{
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findOSByKnowBufferTask,this,p, chunkSize, knowElement,&tempBuffer[i]));
					p +=  chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkCount; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}else{
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findOSByKnowBufferTask,this,p, 1,knowElement,&tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkLeft; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
        buffer->sort(1);
    }
    return OK;
}

Status FindEntityID::findOSByKnowBufferTask(ID* p, size_t length , EntityType knowElement,EntityIDBuffer* resultBuffer){
    EntityIDBuffer tempBuffer;
    unsigned i=0,j=0;
    resultBuffer->setIDCount(2);
    resultBuffer->setSortKey(1);
    if (knowElement == OBJECT) {
        for (i=0; i < length; i++) {
            ID key = *(p + i);
            findSubjectIDByObject(key, &tempBuffer, 0, UINT_MAX);
            unsigned tempSize = tempBuffer.getSize();
            for(j=0;j < tempSize; j++){
                resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j]);
            }
        }
    }else if(knowElement == SUBJECT){
		for (i=0; i < length; i++) {
            ID key = *(p + i);
            findObjectIDBySubject(key, &tempBuffer, 0, UINT_MAX);
            unsigned tempSize = tempBuffer.getSize();
            for(j=0;j < tempSize; j++){
                resultBuffer->insertID((tempBuffer)[j]);
				resultBuffer->insertID(key);
            }
        }
	}

    return OK;
}

//FINDSPBYNONE
Status FindEntityID::findSPByKnowBuffer(EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement){
	buffer->setIDCount(2);
	buffer->setSortKey(10);//unsorted
	if(knowBuffer->sorted == false)
		return ERROR;
	if( knowBuffer->getSize() == 0){
		return ERROR;
	}
	else{
		EntityIDBuffer transBuffer;
		ID key=0,lastKey=0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		for(unsigned i = 0;i < knowBuffer->getSize();i++){
			key = *(p+i*IDCount + sortKey);
			if(key > lastKey){
				transBuffer.insertID(key);
				lastKey = key;
			}
		}
		size_t size = transBuffer.getSize();
		unsigned chunkCount = THREAD_NUMBER;
		size_t chunkSize = size / chunkCount;
		size_t chunkLeft = size % chunkCount;
		p = transBuffer.getBuffer();
		
		if(chunkSize){
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft){
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findSPByKnowBufferTask,this,p, chunkSize+1,knowElement,&tempBuffer[i]));
					p += (chunkSize+1);
				}
				else{
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findSPByKnowBufferTask,this,p, chunkSize, knowElement,&tempBuffer[i]));
					p +=  chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkCount; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}else{
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findSPByKnowBufferTask,this,p, 1,knowElement,&tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkLeft; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
		buffer->sort(1);
	}
	return OK;
}

Status FindEntityID::findSPByKnowBufferTask(ID* p, size_t length , EntityType knowElement,EntityIDBuffer* resultBuffer){
	EntityIDBuffer tempBuffer;
	unsigned i=0,j=0;
	resultBuffer->setIDCount(2);
	resultBuffer->setSortKey(1);
	if (knowElement == SUBJECT) {
		for (i=0; i < length; i++) {
			ID key = *(p + i);
			findPredicateIDBySubject(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for(j=0;j < tempSize; j++){
				resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j]);
			}
		}
	}else if(knowElement == PREDICATE){
		for (i=0; i < length; i++) {
            ID key = *(p + i);
            findSubjectIDByPredicate(key, &tempBuffer, 0, UINT_MAX);
            unsigned tempSize = tempBuffer.getSize();
            for(j=0;j < tempSize; j++){
            	resultBuffer->insertID((tempBuffer)[j]);
                resultBuffer->insertID(key);
            }
        }
	}
	return OK;
}

//FINDPSBYNONE
Status FindEntityID::findPSByKnowBuffer(EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement){
	buffer->setIDCount(2);
	buffer->setSortKey(10);//unsorted
	if(knowBuffer->sorted == false)
		return ERROR;
	if( knowBuffer->getSize() == 0){
		return ERROR;
	}
	else{
		EntityIDBuffer transBuffer;
		ID key=0,lastKey=0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		for(unsigned i = 0;i < knowBuffer->getSize();i++){
			key = *(p+i*IDCount + sortKey);
			if(key > lastKey){
				transBuffer.insertID(key);
				lastKey = key;
			}
		}
		size_t size = transBuffer.getSize();
		unsigned chunkCount = THREAD_NUMBER;
		size_t chunkSize = size / chunkCount;
		size_t chunkLeft = size % chunkCount;
		p = transBuffer.getBuffer();
		
		if(chunkSize){
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft){
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findPSByKnowBufferTask,this,p, chunkSize+1,knowElement,&tempBuffer[i]));
					p += (chunkSize+1);
				}
				else{
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findPSByKnowBufferTask,this,p, chunkSize, knowElement,&tempBuffer[i]));
					p +=  chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkCount; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}else{
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findPSByKnowBufferTask,this,p, 1,knowElement,&tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkLeft; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
		buffer->sort(1);
	}
	return OK;
}

Status FindEntityID::findPSByKnowBufferTask(ID* p, size_t length , EntityType knowElement,EntityIDBuffer* resultBuffer){
	EntityIDBuffer tempBuffer;
	unsigned i=0,j=0;
	resultBuffer->setIDCount(2);
	resultBuffer->setSortKey(1);
	if (knowElement == PREDICATE) {
		for (i=0; i < length; i++) {
			ID key = *(p + i);
			findSubjectIDByPredicate(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for(j=0;j < tempSize; j++){
				resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j]);
			}
		}
	}else if(knowElement == SUBJECT){
		for (i=0; i < length; i++) {
            ID key = *(p + i);
            findPredicateIDBySubject(key, &tempBuffer, 0, UINT_MAX);
            unsigned tempSize = tempBuffer.getSize();
            for(j=0;j < tempSize; j++){
            	resultBuffer->insertID((tempBuffer)[j]);
                resultBuffer->insertID(key);
            }
        }
	}
	return OK;
}

//FINDOPBYNONE
Status FindEntityID::findOPByKnowBuffer(EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement){
	buffer->setIDCount(2);
	buffer->setSortKey(10);//unsorted
	if(knowBuffer->sorted == false)
		return ERROR;
	if( knowBuffer->getSize() == 0){
		return ERROR;
	}
	else{
		EntityIDBuffer transBuffer;
		ID key=0,lastKey=0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		for(unsigned i = 0;i < knowBuffer->getSize();i++){
			key = *(p+i*IDCount + sortKey);
			if(key > lastKey){
				transBuffer.insertID(key);
				lastKey = key;
			}
		}
		size_t size = transBuffer.getSize();
		unsigned chunkCount = THREAD_NUMBER;
		size_t chunkSize = size / chunkCount;
		size_t chunkLeft = size % chunkCount;
		p = transBuffer.getBuffer();
		
		if(chunkSize){
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft){
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findOPByKnowBufferTask,this,p, chunkSize+1,knowElement,&tempBuffer[i]));
					p += (chunkSize+1);
				}
				else{
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findOPByKnowBufferTask,this,p, chunkSize, knowElement,&tempBuffer[i]));
					p +=  chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkCount; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}else{
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findOPByKnowBufferTask,this,p, 1,knowElement,&tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkLeft; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
		buffer->sort(1);
	}
	return OK;
}

Status FindEntityID::findOPByKnowBufferTask(ID* p, size_t length , EntityType knowElement,EntityIDBuffer* resultBuffer){
	EntityIDBuffer tempBuffer;
	unsigned i=0,j=0;
	resultBuffer->setIDCount(2);
	resultBuffer->setSortKey(1);
	if (knowElement == OBJECT) {
		for (i=0; i < length; i++) {
			ID key = *(p + i);
			findPredicateIDByObject(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for(j=0;j < tempSize; j++){
				resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j]);
			}
		}
	}else if(knowElement == PREDICATE){
		for (i=0; i < length; i++) {
            ID key = *(p + i);
            findObjectIDByPredicate(key, &tempBuffer, 0, UINT_MAX);
            unsigned tempSize = tempBuffer.getSize();
            for(j=0;j < tempSize; j++){
            	resultBuffer->insertID((tempBuffer)[j]);
                resultBuffer->insertID(key);
            }
        }
	}
	return OK;
}

//FINDPOBYNONE
Status FindEntityID::findPOByKnowBuffer(EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement){
	buffer->setIDCount(2);
	buffer->setSortKey(10);//unsorted
	if(knowBuffer->sorted == false)
		return ERROR;
	if( knowBuffer->getSize() == 0){
		return ERROR;
	}
	else{
		EntityIDBuffer transBuffer;
		ID key=0,lastKey=0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		for(unsigned i = 0;i < knowBuffer->getSize();i++){
			key = *(p+i*IDCount + sortKey);
			if(key > lastKey){
				transBuffer.insertID(key);
				lastKey = key;
			}
		}
		size_t size = transBuffer.getSize();
		unsigned chunkCount = THREAD_NUMBER;
		size_t chunkSize = size / chunkCount;
		size_t chunkLeft = size % chunkCount;
		p = transBuffer.getBuffer();

		if(chunkSize){
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft){
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findPOByKnowBufferTask,this,p, chunkSize+1,knowElement,&tempBuffer[i]));
					p += (chunkSize+1);
				}
				else{
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findPOByKnowBufferTask,this,p, chunkSize, knowElement,&tempBuffer[i]));
					p +=  chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkCount; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}else{
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findPOByKnowBufferTask,this,p, 1,knowElement,&tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkLeft; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
		buffer->sort(1);
	}
	return OK;
}

Status FindEntityID::findPOByKnowBufferTask(ID* p, size_t length , EntityType knowElement,EntityIDBuffer* resultBuffer){
	EntityIDBuffer tempBuffer;
	unsigned i=0,j=0;
	resultBuffer->setIDCount(2);
	resultBuffer->setSortKey(1);
	if (knowElement == PREDICATE) {
		for (i=0; i < length; i++) {
			ID key = *(p + i);
			findObjectIDByPredicate(key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for(j=0;j < tempSize; j++){
				resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j]);
			}
		}
	}else if(knowElement == OBJECT){
		for (i=0; i < length; i++) {
            ID key = *(p + i);
            findPredicateIDByObject(key, &tempBuffer, 0, UINT_MAX);
            unsigned tempSize = tempBuffer.getSize();
            for(j=0;j < tempSize; j++){
            	resultBuffer->insertID((tempBuffer)[j]);
                resultBuffer->insertID(key);
            }
        }
	}
	return OK;
}

Status FindEntityID::findSOByKnowBuffer(ID preID,EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement){
	buffer->setIDCount(2);
	buffer->setSortKey(10);//unsorted
	if(knowBuffer->sorted == false)
		return ERROR;
	if( knowBuffer->getSize() == 0){
		return ERROR;
	}
	else{
		EntityIDBuffer transBuffer;
		ID key=0,lastKey=0;
		ID * p = knowBuffer->getBuffer();
		int IDCount = knowBuffer->getIDCount();
		int sortKey = knowBuffer->getSortKey();
		if (IDCount > 1) {
			for (unsigned i = 0; i < knowBuffer->getSize(); i++) {
				key = *(p + i * IDCount + sortKey);
				if (key > lastKey) {
					transBuffer.insertID(key);
					lastKey = key;
				}
			}
		}else{
			transBuffer= (knowBuffer);
		}
		size_t size = transBuffer.getSize();
		unsigned chunkCount = THREAD_NUMBER;
		size_t chunkSize = size / chunkCount;
		size_t chunkLeft = size % chunkCount;
		p = transBuffer.getBuffer();
		
		if(chunkSize){
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft){
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findSOByKnowBufferTask1,this,preID,p,chunkSize+1,knowElement,&tempBuffer[i]));
					p += (chunkSize+1);
				}
				else{
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findSOByKnowBufferTask1,this,preID,p, chunkSize, knowElement,&tempBuffer[i]));
					p +=  chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkCount; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}else{
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findSOByKnowBufferTask1,this,preID,p, 1,knowElement,&tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkLeft; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
		buffer->sort(1);

	}
	return OK;
}

Status FindEntityID::findSOByKnowBufferTask1(ID preID,ID* p, size_t length , EntityType knowElement,EntityIDBuffer* resultBuffer){
	EntityIDBuffer tempBuffer;
	unsigned i=0,j=0;
	resultBuffer->setIDCount(2);
	resultBuffer->setSortKey(1);
	if (knowElement == SUBJECT) {
		for (i=0; i < length; i++) {
			ID key = *(p + i);
			findObjectIDByPredicateAndSubject(preID,key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for(j=0;j < tempSize; j++){
				resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j]);
			}
		}
	}else if(knowElement == OBJECT){
		for (i = 0; i < length; i++) {
			ID key = *(p + i);
			findSubjectIDByPredicateAndObject(preID, key, &tempBuffer, 0,UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for (j = 0; j < tempSize; j++) {
				resultBuffer->insertID((tempBuffer)[j]);
				resultBuffer->insertID(key);
			}
		}
	}
	return OK;
}

Status FindEntityID::findOSByKnowBuffer(ID preID,EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement){
    buffer->setIDCount(2);
    buffer->setSortKey(10);//unsorted
    if(knowBuffer->sorted == false)
        return ERROR;
    if( knowBuffer->getSize() == 0){
        return ERROR;
    }
    else{
        EntityIDBuffer transBuffer;
        ID key=0,lastKey=0;
        ID * p = knowBuffer->getBuffer();
        int IDCount = knowBuffer->getIDCount();
        int sortKey = knowBuffer->getSortKey();
        if (IDCount > 1) {
            for (unsigned i = 0; i < knowBuffer->getSize(); i++) {
                key = *(p + i * IDCount + sortKey);
                if (key > lastKey) {
                    transBuffer.insertID(key);
                    lastKey = key;
                }
            }
        }else{
            transBuffer= (knowBuffer);
        }

        size_t size = transBuffer.getSize();
        unsigned chunkCount = THREAD_NUMBER;
        size_t chunkSize = size / chunkCount;
        size_t chunkLeft = size % chunkCount;
        p = transBuffer.getBuffer();
        
		if(chunkSize){
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft){
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findOSByKnowBufferTask1,this,preID,p, chunkSize+1,knowElement,&tempBuffer[i]));
					p += (chunkSize+1);
				}
				else{
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findOSByKnowBufferTask1,this,preID,p, chunkSize, knowElement,&tempBuffer[i]));
					p +=  chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkCount; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}else{
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findOSByKnowBufferTask1,this,preID,p, 1,knowElement,&tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkLeft; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
        buffer->sort(1);
    }
    return OK;
}

Status FindEntityID::findOSByKnowBufferTask1(ID preID,ID* p, size_t length , EntityType knowElement,EntityIDBuffer* resultBuffer){
    if(length == 0)
        return OK;
    EntityIDBuffer tempBuffer;
    unsigned i=0,j=0;
    resultBuffer->setIDCount(2);
    resultBuffer->setSortKey(1);
    if (knowElement == OBJECT) {
        for (i=0; i < length; i++) {
			ID key = *(p + i);
			findSubjectIDByPredicateAndObject(preID,key, &tempBuffer, 0, UINT_MAX);
			unsigned tempSize = tempBuffer.getSize();
			for(j=0;j < tempSize; j++){
				resultBuffer->insertID(key);
				resultBuffer->insertID((tempBuffer)[j]);
			}
		}
    }else if(knowElement == SUBJECT){
        for (i = 0; i < length; i++) {
            ID key = *(p + i);
            findObjectIDByPredicateAndSubject(preID, key, &tempBuffer, 0,UINT_MAX);
            unsigned tempSize = tempBuffer.getSize();
            for (j = 0; j < tempSize; j++) {
                resultBuffer->insertID((tempBuffer)[j]);
                resultBuffer->insertID(key);
            }
        }
    }
    return OK;
}

Status FindEntityID::findSByKnowBuffer(ID preID,EntityIDBuffer* buffer,EntityIDBuffer* knowBuffer, EntityType knowElement){
    buffer->setIDCount(2);
    buffer->setSortKey(10);//unsorted
    if(!knowBuffer->sorted ){
        return ERROR;
    }
    if( knowBuffer->getSize() == 0){
        return ERROR;
    }
    if(knowElement == SUBJECT){
        ID key=0,lastKey=0;
        ID * p = knowBuffer->getBuffer();
        int IDCount = knowBuffer->getIDCount();
        int sortKey = knowBuffer->getSortKey();
        if (IDCount > 1) {
            for (unsigned i = 0; i < knowBuffer->getSize(); i++) {
                key = *(p + i * IDCount + sortKey);
                if (key > lastKey) {
                    buffer->insertID(key);
                    lastKey = key;
                }
            }
        }else{
            (*buffer) = (knowBuffer);
        }

        return ERROR;
    }
    else{
		EntityIDBuffer transBuffer;
        ID key=0,lastKey=0;
        ID * p = knowBuffer->getBuffer();
        int IDCount = knowBuffer->getIDCount();
        int sortKey = knowBuffer->getSortKey();
        if (IDCount > 1) {
            for (unsigned i = 0; i < knowBuffer->getSize(); i++) {
                key = *(p + i * IDCount + sortKey);
                if (key > lastKey) {
                    transBuffer.insertID(key);
                    lastKey = key;
                }
            }
        }else{
            transBuffer= (knowBuffer);
        }
        transBuffer.print();
        size_t size = transBuffer.getSize();
        unsigned chunkCount = THREAD_NUMBER;
        size_t chunkSize = size / chunkCount;
        size_t chunkLeft = size % chunkCount;
        p = transBuffer.getBuffer();
    
		if(chunkSize){
			EntityIDBuffer tempBuffer[chunkCount];
			for (unsigned i = 0; i < chunkCount; i++) {
				if (i < chunkLeft){
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findSByKnowBufferTask1,this,preID,p, chunkSize+1,&tempBuffer[i]));
					p += (chunkSize+1);
				}
				else{
					CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findSByKnowBufferTask1,this,preID,p, chunkSize,&tempBuffer[i]));
					p +=  chunkSize;
				}
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkCount; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}else{
			EntityIDBuffer tempBuffer[chunkLeft];
			for (unsigned i = 0; i < chunkLeft; i++) {
				CThreadPool::getInstance().AddTask(boost::bind(&FindEntityID::findSByKnowBufferTask1,this,preID,p, 1,&tempBuffer[i]));
				p += 1;
			}
			CThreadPool::getInstance().Wait();
			for(unsigned i=0; i< chunkLeft; i++){
				buffer->appendBuffer(&tempBuffer[i]);
			}
		}
        buffer->sort(1);
    }
    return OK;
}
Status FindEntityID::findSByKnowBufferTask1(ID preID,ID* p, size_t length ,EntityIDBuffer* resultBuffer){
    EntityIDBuffer tempBuffer;
    unsigned i=0,j=0;
    resultBuffer->setIDCount(2);
    resultBuffer->setSortKey(1);
    for (i=0; i < length; i++) {
        ID key = *(p + i);
        findSubjectIDByPredicateAndObject(preID,key, &tempBuffer, 0, UINT_MAX);
        unsigned tempSize = tempBuffer.getSize();
        for(j=0;j < tempSize; j++){
            resultBuffer->insertID((tempBuffer)[j]);
        }
    }
    return OK;
}
