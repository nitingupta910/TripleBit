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

#include "StringIDSegment.h"
#include "Hash.h"

const ID INVALID_ID = ID(-1);

StringIDSegment::StringIDSegment():fillRate(0.5),stringPool(NULL),stringHashTable(NULL),idStroffPool(NULL)
{
	// TODO Auto-generated constructor stub
}

StringIDSegment::~StringIDSegment() {
	// TODO Auto-generated destructor stub
	if(stringPool != NULL)
		delete stringPool;
	stringPool = NULL;

	if(stringHashTable != NULL)
		delete stringHashTable;
	stringHashTable = NULL;

	if(idStroffPool != NULL)
		delete idStroffPool;
	idStroffPool = NULL;
}

StringIDSegment* StringIDSegment::create(const string dir, const string segmentName)
{
	// string_pool,a var pool
	//64kb init size.
	StringIDSegment* segment = new StringIDSegment();

	string new_path = dir + "/" + segmentName + "_stringPool";
	segment->stringPool = ObjectPool<uint>::create(
		LengthType_UInt|ObjectPoolType_Normal,
		new_path.c_str(),
		1<<16);
	if(segment->stringPool==NULL){
		MessageEngine::showMessage("create string pool error", MessageEngine::ERROR);
		return NULL;
	}

	//id_stroff,fix pool,64kb init size.
	new_path = dir + "/" + segmentName + "_idStroffPool";
	segment->idStroffPool = FixedObjectPool::create(
			ObjectPoolType_Fixed,
			new_path.c_str(),
			1<<16,
			sizeof(IDStroffEntry));
	if(segment->idStroffPool==NULL){
		MessageEngine::showMessage("create id string offset error", MessageEngine::ERROR);
		delete segment->stringPool;
		return NULL;
	}

	// string_hash,memory buffer.
	new_path = dir + "/" + segmentName + "_stringHashTable";
	segment->stringHashTable = new MMapBuffer(new_path.c_str(),sizeof(OffsetType)*next_prime_number(segment->stringPool->size()*2+1));
	if(segment->stringHashTable==NULL){
		MessageEngine::showMessage("create string hash table error", MessageEngine::ERROR);
		delete segment->stringPool;
		delete segment->idStroffPool;
		return NULL;
	}

	segment->buildStringHashTable();

	return segment;
}

StringIDSegment* StringIDSegment::load(const string dir, const string segmentName)
{
	StringIDSegment* segment = new StringIDSegment();

	string new_path = dir + "/" + segmentName + "_stringPool";
	segment->stringPool = ObjectPool<uint>::load(new_path.c_str());
	if(segment->stringPool == NULL) {
		MessageEngine::showMessage("load string pool error!", MessageEngine::ERROR);
		return NULL;
	}

	new_path = dir + "/" + segmentName + "_idStroffPool";
	segment->idStroffPool = FixedObjectPool::load(new_path.c_str());
	if(segment->idStroffPool == NULL) {
		delete segment->stringPool;
		MessageEngine::showMessage("load is string offset pool error", MessageEngine::ERROR);
		return NULL;
	}

	new_path = dir + "/" + segmentName + "_stringHashTable";
	segment->stringHashTable = MMapBuffer::create(new_path.c_str(), 0);
	if(segment->stringHashTable == NULL) {
		delete segment->stringPool;
		delete segment->idStroffPool;
		MessageEngine::showMessage("load string hash talble error", MessageEngine::ERROR);
		return NULL;
	}

	return segment;
}

//add string to stringPool,and update the stringHashTable.
OffsetType StringIDSegment::addStringToStringPoolAndUpdateStringHashTable(LengthString * aStr, ID id)
{
	if( stringHashTable->get_length()/sizeof(OffsetType) * fillRate < stringPool->size() ){
		HashCodeType size = stringHashTable->get_length()/sizeof(OffsetType);
		size = next_hash_capacity(size);
		if(stringHashTable->resize(size*sizeof(OffsetType), false)!=OK){
			// TODO: log no memory
			return 0;
		}
		stringHashTable->memset(0);
		buildStringHashTable();
	}

	HashCodeType hashcode = get_hash_code(aStr);
	HashCodeType len = stringHashTable->get_length()/sizeof(OffsetType);
	hashcode%=len;
	OffsetType * array = (OffsetType*)(stringHashTable->get_address());
#ifndef USE_C_STRING
	OffsetType strLen=0;
#endif
	void * str;
	int i = 0;
	while( array[hashcode] ){
#ifdef USE_C_STRING
		Status ok = stringPool->get_by_offset(array[hashcode], &str);
#else
		Status ok = stringPool->get_by_offset(array[hashcode], &strLen, &str);
#endif
		assert(ok==OK);
#ifdef USE_C_STRING
		if(aStr->equals((char*)str)){
#else
//		if(aStr->equals((char*)str + sizeof(ID), strLen - sizeof(ID) )) {
		string sstr(((char*)str + sizeof(ID)));
		if(aStr->equals(sstr.c_str())){
#endif
			// already exists
			return 0;
		}

		if(hashcode==0) {
			hashcode = len-1;
			if(i==1)break;
			i++;
		}
		else
			hashcode--;
	}
	array[hashcode] = stringPool->append_object_with_id(id,aStr->length,aStr->str);
	return array[hashcode];
}

void StringIDSegment::buildStringHashTable()
{
	OffsetType size = stringHashTable->get_length()/sizeof(OffsetType);
	OffsetType off = stringPool->first_offset();
	OffsetType * array= (OffsetType*)(stringHashTable->get_address());
	ulonglong stringPoolSize = stringPool->size();
	while(stringPoolSize){
		void * pdata;
#ifdef USE_C_STRING
		stringPool->get_by_offset(off,&pdata);
		HashCodeType hashcode = get_hash_code((char*)pdata);
#else
		OffsetType len;
		stringPool->get_by_offset(off, &len, &pdata);
		HashCodeType hashcode = get_hash_code((char*)pdata + sizeof(ID), len - sizeof(ID));
#endif
		hashcode %= size;
		while( array[hashcode] ){
			if(hashcode==0)
				hashcode=size-1;
			else
				hashcode--;
		}
		array[hashcode] = off;
#ifdef USE_C_STRING
		off = off + sizeof(ID) + strlen((char*)pdata) + 1;//stringPool->next_offset(off);
#else
		off = stringPool->next_offset(off);
#endif
		stringPoolSize--;
	}
}

OffsetType StringIDSegment::findStringOffset(LengthString * aStr)
{
	HashCodeType hashcode = get_hash_code(aStr);
	HashCodeType len = stringHashTable->get_length()/sizeof(OffsetType);
	hashcode%=len;
	OffsetType * array = (OffsetType*)(stringHashTable->get_address());
	OffsetType strLen=0;
	void * str;
	int i = 0;
	while( array[hashcode] ){
		Status ok = stringPool->get_by_offset(array[hashcode],&strLen,&str);
		assert(ok==OK);
#ifdef USE_C_STRING
		if(aStr->equals((char*)str,strLen-sizeof(ID))){
			return array[hashcode];
		}
#else
		if(aStr->equals((char*)str + sizeof(ID),strLen-sizeof(ID))){
			return array[hashcode];
		}
#endif

		if(hashcode==0) {
			hashcode = len-1;
			if(i==1)break;
			i++;
		}
		else
			hashcode--;
	}
	return 0;
}

ID StringIDSegment::getMaxID()
{
	return idStroffPool->next_id();
}

//add string to StringIDSegment ,update stringPool,stringHashTable,idStroffPool.
ID StringIDSegment::addStringToSegment(LengthString * aStr)
{
	//get the next id .
	ID nextId = idStroffPool->next_id();
	OffsetType strOff = addStringToStringPoolAndUpdateStringHashTable(aStr,nextId);

	if(strOff){
		IDStroffEntry ise;
		//ise.id = nextId;
		ise.stroff = strOff;
		ID id = addIDStroffToIdStroffPool(&ise);
		assert ( id == nextId );
		return id;
	}
	// TODO: none id?
	return INVALID_ID;
}

//optimize memory usage.
Status StringIDSegment::optimize()
{
	Status retcode = NOT_SUPPORT;
	//retcode = stringPool->optimaze();
	//if(retcode == OK)retcode = idStroffPool->optimaze();
    return retcode;
}

//find string
bool StringIDSegment::findStringById(std::string& str, const ID& id)
{
	LengthString * lstr = new LengthString();
	bool flag =  findStringById(lstr, id);
	str = std::string(lstr->str,lstr->length);
	return flag;
}

//find id
bool StringIDSegment::findIdByString(ID& id, const std::string& str)
{
	LengthString * lstr = new LengthString(str);
	return findIdByString(id, lstr);
}

//find string
bool StringIDSegment::findStringById(LengthString * aStr, const ID& id)
{
	IDStroffEntry * ise = 0;
	OffsetType elength;
	void *pdata;
	if(idStroffPool->get_by_id(id,&elength,&pdata)==OK){
		assert(elength==sizeof(IDStroffEntry));
		ise = (IDStroffEntry *)pdata;
	}
	if(ise == NULL || ise->stroff == 0){
		return false;
	}

	OffsetType length;
	void * strdst;
	Status retcode = stringPool->get_by_offset(ise->stroff,&length,&strdst);
	if(retcode == OK){
		aStr->length = length-sizeof(ID);
#ifdef USE_C_STRING
		aStr->str =  ((char *)strdst);
#else
		aStr->str = (char*)strdst + sizeof(ID);
#endif
		return true;
	}else return false;
}

//find id
bool StringIDSegment::findIdByString(ID& id, LengthString * aStr)
{
	HashCodeType hashcode = get_hash_code(aStr);
	HashCodeType len = stringHashTable->get_length()/sizeof(OffsetType);
	hashcode%=len;
	OffsetType * array = (OffsetType*)(stringHashTable->get_address());

	void * str;
	int i = 0 ;
	while( array[hashcode] ){
#ifdef USE_C_STRING
		Status ok = stringPool->get_by_offset(array[hashcode], &str);
#else
		OffsetType strLen=0;
		Status ok = stringPool->get_by_offset(array[hashcode],&strLen,&str);
#endif
		assert(ok==OK);
#ifdef USE_C_STRING
		if(aStr->equals((char*)str)){
			id = *(ID*)((char*)str - sizeof(ID));
			return true;
		}
#else
		if(aStr->equals((char*)str + sizeof(ID),strLen-sizeof(ID))){
			id = *(ID*)(str);
			return true;
		}
#endif

		if(hashcode==0){
			hashcode = len-1;
			if(i==1)break;
			i++;
		} else {
			hashcode--;
		}
	}
	return false;
}

void StringIDSegment::dump()
{
	stringPool->dump_int_string(stdout);
}

//for test
void StringIDSegment::cmd_line(FILE * fin, FILE * fout) {
	char cmd[256];
	while (true) {
		fflush(fin);
		fprintf(fout,
				"\n>>1. dump stringPool \t 2.dump (id,stroff)Pool \t3.exit\n");
		fprintf(fout, ">>>");
		fscanf(fin, "%s", cmd);
		if (strcmp(cmd, "dtr") == 0 || strcmp(cmd, "1") == 0) {
			stringPool->dump_int_string(fout);
		} else if (strcmp(cmd, "dumpidoff") == 0 || strcmp(cmd, "2") == 0) {
			idStroffPool->dump_int_string(fout);
		} else if (strcmp(cmd, "3") == 0 || strcmp(cmd, "exit") == 0) {
			break;
		}

	}
}
