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
#include "TripleBitRepository.h"
#include "PredicateTable.h"
#include "URITable.h"
#include "TripleBitBuilder.h"
#include "BitmapBuffer.h"
#include "StatisticsBuffer.h"
#include "util/FindEntityID.h"
#include "EntityIDBuffer.h"
#include "MMapBuffer.h"
#include "OSFile.h"
#include "TripleBitQuery.h"
#include "RDFQuery.h"
#include <sys/time.h>

int TripleBitRepository::colNo = INT_MAX - 1;

TripleBitRepository::TripleBitRepository() {
	// TODO Auto-generated constructor stub
	this->UriTable = NULL;
	this->preTable = NULL;
	this->bitmapBuffer = NULL;
	columnFinder = NULL;
	buffer = NULL;

	subjectStat = NULL;
	subPredicateStat = NULL;
	objectStat = NULL;
	objPredicateStat = NULL;

	bitmapImage = NULL;
	bitmapIndexImage = NULL;
	bitmapPredicateImage = NULL;
}

TripleBitRepository::~TripleBitRepository() {
	// TODO Auto-generated destructor stub
	if(columnFinder != NULL) delete columnFinder;
	columnFinder = NULL;

	if(buffer != NULL) delete buffer;
	buffer = NULL;

	if(UriTable != NULL) delete UriTable;
	UriTable = NULL;
#ifdef DEBUG
	cout<<"uri table delete"<<endl;
#endif
	if(preTable != NULL) delete preTable;
	preTable = NULL;
#ifdef DEBUG
	cout<<"predicate table delete"<<endl;
#endif
	if(bitmapBuffer != NULL) delete bitmapBuffer;
	bitmapBuffer = NULL;
#ifdef DEBUG
	cout<<"bitmapBuffer delete"<<endl;
#endif
	if(subjectStat != NULL) delete subjectStat;
	subjectStat = NULL;

	if(subPredicateStat != NULL) delete subPredicateStat;
	subPredicateStat = NULL;

	if(objectStat != NULL) delete objectStat;
	objectStat = NULL;

	if(objPredicateStat != NULL) delete objPredicateStat;
	objPredicateStat = NULL;
//*/
	if(bitmapImage != NULL) delete bitmapImage;
	bitmapImage = NULL;

#ifdef DEBUG
	cout<<"bitmapImage delete"<<endl;
#endif

	if(bitmapIndexImage != NULL) delete bitmapIndexImage;
	bitmapIndexImage = NULL;

#ifdef DEBUG
	cout<<"bitmap index image delete"<<endl;
#endif

	if(bitmapPredicateImage != NULL) delete bitmapPredicateImage;
	bitmapPredicateImage = NULL;
#ifdef DEBUG
	cout<<"bitmap predicate image"<<endl;
#endif
}


bool TripleBitRepository::find_pid_by_string(PID& pid, const string& str)
{
	if(preTable->getIDByPredicate(str.c_str(),pid) != OK)
		return false;
	//pid  = 15000;
	return true;
}

bool TripleBitRepository::find_soid_by_string(SOID& soid, const string& str)
{
	if(UriTable->getIdByURI(str.c_str(),soid) != URI_FOUND)
		return false;
	//soid = 10000;
	return true;
}

bool TripleBitRepository::find_string_by_pid(string& str, PID& pid)
{
	str = preTable->getPredicateByID(pid);

	if(str.length() == 0)
		return false;
	return true;
}

bool TripleBitRepository::find_string_by_soid(string& str, SOID& soid)
{
	//char temp[500];
	if(UriTable->getURIById(str,soid) == URI_NOT_FOUND)
		return false;
	//str.clear();
	//str.append(temp);

	return true;
}

int TripleBitRepository::get_predicate_count(PID pid)
{
	/*vector< Chunk* >* chunks = bitmapBuffer->getChunkManager(pid,0)->getVectorChunks();

	ID startColID = (*chunks)[0]->getColStart();
	ID endColID = startColID;

	int j = (*chunks).size();
	endColID = (*chunks)[j - 1]->getColEnd();*/
	int count1 = bitmapBuffer->getChunkManager(pid, 0)->getTripleCount();
	int count2 = bitmapBuffer->getChunkManager(pid, 1)->getTripleCount();

	return count1 + count2;
}

bool TripleBitRepository::lookup(const string& str, ID& id)
{
	if( preTable->getIDByPredicate(str.c_str(), id) != OK && UriTable->getIdByURI(str.c_str(), id) != URI_FOUND )
		return false;

	return true;
}
int TripleBitRepository::get_object_count(ID objectID)
{
	((OneConstantStatisticsBuffer*)objectStat)->getStatis(objectID);
	return objectID;
	//return 3;
}

int TripleBitRepository::get_subject_count(ID subjectID)
{
	((OneConstantStatisticsBuffer*)subjectStat)->getStatis(subjectID);
	return subjectID;
	//return 3;
}

int TripleBitRepository::get_subject_predicate_count(ID subjectID, ID predicateID)
{
	subPredicateStat->getStatis(subjectID, predicateID);
	return subjectID;
	//return 2;
}

int TripleBitRepository::get_object_predicate_count(ID objectID, ID predicateID)
{
	objPredicateStat->getStatis(objectID, predicateID);
	return objectID;
	//return 2;
}

int TripleBitRepository::get_subject_object_count(ID subjectID, ID objectID)
{
	return 1;
}

Status TripleBitRepository::getSubjectByObjectPredicate(ID oid, ID pid)
{
	pos = 0;
	return columnFinder->findSubjectIDByPredicateAndObject(pid, oid, buffer, 0, UINT_MAX);
}

ID TripleBitRepository::next()
{
	ID id;
	Status s = buffer->getID(id, pos);
	if ( s != OK )
		return 0;
	
	pos++;
	return id;
}

TripleBitRepository* TripleBitRepository::create(const string path)
{
	TripleBitRepository* repo = new TripleBitRepository();

	string filename = path + "/BitmapBuffer";

	if(OSFile::fileExists(filename) == false) {
		//file dose not exist, repository has not been build;
		fprintf(stderr, "database files dose not exist!");
		return NULL;
	}
	// load the repository from image files;
	//load bitmap
#ifdef DEBUG
	cout<<filename.c_str()<<endl;
#endif
	repo->bitmapImage = new MMapBuffer(filename.c_str(), 0);
	string predicateFile(filename);
	predicateFile.append("_predicate");
	string indexFile(filename);
	indexFile.append("_index");
	repo->bitmapPredicateImage = new MMapBuffer(predicateFile.c_str(), 0);
	repo->bitmapIndexImage = new MMapBuffer(indexFile.c_str(), 0);
	repo->bitmapBuffer = BitmapBuffer::load(repo->bitmapImage, repo->bitmapIndexImage, repo->bitmapPredicateImage);

	repo->UriTable = URITable::load(path);
	repo->preTable = PredicateTable::load(path);

#ifdef DEBUG
	cout<<"total triple count: "<<repo->bitmapBuffer->getTripleCount()<<endl;
	cout<<"URITableSize: "<<repo->UriTable->getSize()<<endl;
	cout<<"predicateTableSize: "<<repo->preTable->getSize()<<endl;
#endif
	
//	cout<<"begin load statistics buffer"<<endl;
	filename = path + "/statIndex";
	MMapBuffer* indexBufferFile = MMapBuffer::create(filename.c_str(), 0);
	char* indexBuffer = indexBufferFile->get_address();

	string statFilename = path + "/subject_statis";
	repo->subjectStat = OneConstantStatisticsBuffer::load(StatisticsBuffer::SUBJECT_STATIS, statFilename, indexBuffer);
	statFilename = path + "/object_statis";
	repo->objectStat = OneConstantStatisticsBuffer::load(StatisticsBuffer::OBJECT_STATIS, statFilename, indexBuffer);
	statFilename = path + "/subjectpredicate_statis";
	repo->subPredicateStat = TwoConstantStatisticsBuffer::load(StatisticsBuffer::SUBJECTPREDICATE_STATIS, statFilename, indexBuffer);
	statFilename = path + "/objectpredicate_statis";
	repo->objPredicateStat = TwoConstantStatisticsBuffer::load(StatisticsBuffer::OBJECTPREDICATE_STATIS, statFilename, indexBuffer);

#ifdef DEBUG
	cout<<"subject count: "<<((OneConstantStatisticsBuffer*)repo->subjectStat)->getEntityCount()<<endl;
	cout<<"object count: "<<((OneConstantStatisticsBuffer*)repo->objectStat)->getEntityCount()<<endl;
#endif

	repo->buffer = new EntityIDBuffer();
	repo->columnFinder = new FindEntityID(repo);

	cerr<<"load complete!"<<endl;
	repo->bitmapQuery = new TripleBitQuery(*repo);
	repo->query = new RDFQuery(repo->bitmapQuery, repo);

	return repo;
}

static void getQuery(string& queryStr, const char* filename)
{
	ifstream f;
	f.open(filename);

	queryStr.clear();

	if(f.fail() == true) {
		MessageEngine::showMessage("open query file error!", MessageEngine::WARNING);
		return;
	}

	char line[150];
	while (f.peek() != EOF) {
		f.getline(line, 150);

		queryStr.append(line);
		queryStr.append(" ");
	}

	f.close();
}

Status TripleBitRepository::nextResult(string& str)
{
	if(resBegin != resEnd) {
		str = *resBegin;
		resBegin++;
		return OK;
	}
	return ERROR;
}

Status TripleBitRepository::execute(string queryStr)
{
	resultSet.resize(0);
	query->Execute(queryStr, resultSet);

	bitmapQuery->releaseBuffer();
	bitmapQuery->releaseBuffer();
	return OK;
}

extern char* QUERY_PATH;
void TripleBitRepository::cmd_line(FILE* fin, FILE* fout)
{
	char cmd[256];
	while (true) {
		fflush(fin);
		fprintf(fout, ">>>");
		fscanf(fin, "%s", cmd);
		resultSet.resize(0);
		if (strcmp(cmd, "dp") == 0 || strcmp(cmd, "dumppredicate") == 0) {
			getPredicateTable()->dump();
		} else if (strcmp(cmd, "query") == 0) {

		} else if(strcmp(cmd, "source") == 0) {

			string queryStr;
			::getQuery(queryStr, string(QUERY_PATH).append("queryLUBM6").c_str());

			if(queryStr.length() == 0)
				continue;
			execute(queryStr);
		}else if (strcmp(cmd, "exit") == 0) {
			break;
		} else {
			string queryStr;
			::getQuery(queryStr, string(QUERY_PATH).append(cmd).c_str());

			if(queryStr.length() == 0)
				continue;
			execute(queryStr);
		}
	}
}
