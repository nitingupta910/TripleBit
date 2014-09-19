#ifndef OBJECTPOOL_H_
#define OBJECTPOOL_H_

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

#include "TripleBit.h"
#include "MMapBuffer.h"
#include "MemoryBuffer.h"
#include "Hash.h"

#define USE_C_STRING 1

enum BufferType {
	BufferType_Memory = 0,
	//BufferType_MMapped = 1
};

const int BufferTypeMask = 1;

enum LengthType {
	LengthType_UChar  = 0,
	LengthType_UShort = 2,
	LengthType_UInt   = 4,
	LengthType_ULongLong = 6
};

const int LengthTypeMask = 6;

enum ObjectPoolType {
	ObjectPoolType_Normal = 1 << 4,
	ObjectPoolType_Fixed  = 2 << 4,
};

const int ObjectPoolTypeMask = 0xF0;

struct ObjectPoolMeta { 
	ulonglong type; //
	ulonglong size; //the total size
	ulonglong usage;
	ulonglong length;
	ulonglong entrysize; //
	ulonglong id_offset; //
	ulonglong classtype;
	ulonglong indextype;  // or id quota
};

/*
 * Object pool used to stored the ID-string table;
 * Storage format: ID-string-'\0'
 */
template < typename LengthType >
class ObjectPool {
private:
	MMapBuffer * data;

	ObjectPool():data(0){}

	ObjectPoolMeta * get_meta(){
		return (ObjectPoolMeta*)(data->get_address());
	}

public:
	virtual ~ObjectPool(){
		delete data;
	};
	
	virtual void flush(){
	}

	virtual ulonglong size(){
		return get_meta()->size;
	}

	virtual ulonglong usage() {
		return get_meta()->usage;
	}

	virtual Status optimaze(){
		Status retcode = data->resize(get_meta()->usage, false);
		if(retcode!=OK){
			printf("optimaze error\n");
			return retcode;
		}
		get_meta()->length = data->get_length();
		return OK;
	}

	virtual void clear(){
		ObjectPoolMeta * meta = get_meta();
		meta->size = 0;
		meta->usage = sizeof(ObjectPoolMeta);
		flush();
	}

	virtual void reserve(OffsetType size){
		if( size > data->get_length() ){
			Status ret = data->resize(size, false);
			if(ret!=OK)
				return;
			ObjectPoolMeta * header = get_meta();
			header->length = size;
		}
	}

	virtual OffsetType first_offset(){
		if(get_meta()->size>0){
			OffsetType ret = sizeof(ObjectPoolMeta);
			if( *(ID*)(data->get_address()+ret) == INVALID_ID )
				return next_offset(ret);
			return ret;
		}else
			return 0;
	}

	/*
	 * return the the offset of next object which start from 'offset'.
	 */
	virtual OffsetType next_offset(OffsetType offset){
#ifndef USE_C_STRING
		OffsetType ret = offset + *((LengthType*)(data->get_address()+offset)) + sizeof(LengthType);
#else
		OffsetType ret = offset + sizeof(ID) + strlen((char*)data->get_address() + offset + sizeof(ID)) + 1;
#endif
		/*
		while(  *(ID*)(data->get_address()+ret+sizeof(LengthType)) == INVALID_ID ){
			ret += *(LengthType*)(data->get_address()+ret) + sizeof(LengthType);
			if(ret >= get_meta()->usage)
				return 0;
		}*/
		/*
		while(*(ID*)(data->get_address() + ret) == INVALID_ID) {
			ret += sizeof(ID) + strlen((char*)data->get_address() + ret + sizeof(ID)) + 1;
			if(ret >= get_meta()->usage)
				return 0;
		}
		*/
		if(*(ID*)(data->get_address() + ret) == INVALID_ID) {
			return 0;
		}

		if( ret >= get_meta()->usage)
			return 0;
		else
			return ret;
	}

	/**
	 * *plength is the ID + lengthof(ppdata)
	 * *ppdata is the really data pointer;
	 */
	virtual Status get_by_offset(OffsetType offset, OffsetType * plength, void **ppdata){
#ifdef USE_C_STRING
		OffsetType usage = get_meta()->usage;
		assert( offset < usage );

		*ppdata = data->get_address()+offset + sizeof(ID);
		*plength = sizeof(ID) + strlen((char*)(*ppdata));
#else
		OffsetType usage = get_meta()->usage;
		assert( offset < usage);

		*plength = *((LengthType*)(data->get_address()+offset));
		*ppdata = data->get_address()+offset+sizeof(LengthType);
#endif
		return OK;
		// TODO:
	}

	virtual Status get_by_offset(OffsetType offset, void** ppdata) {
		OffsetType usage = get_meta()->usage;
		assert(offset < usage);

		*ppdata = data->get_address() + offset + sizeof(ID);
		return OK;
	}

	//
	OffsetType append_object_with_id( ID id, OffsetType length, const void * pdata){
#ifdef USE_C_STRING
		OffsetType alllength = length+sizeof(ID) + 1; //c type string;
#else
		OffsetType alllength = length + sizeof(ID);
#endif
		assert ( alllength < (LengthType(-1)));

		// TODO: check length
		ObjectPoolMeta * header = get_meta();
		if( header->usage + alllength > header->length ){
#ifdef USE_C_STRING
			OffsetType new_length = expand(header->length,alllength,header->classtype);
#else
			OffsetType new_length = expand(header->length, alllength + sizeof(LengthType), header->classtype);
#endif
			Status ret = data->resize(new_length, false);
			if(ret!=OK)
				return 0;
			header = get_meta();
			header->length = new_length;
		}
		char * top = data->get_address() + header->usage;

#ifdef USE_C_STRING
		*(ID*)top = id;
		memcpy(top + sizeof(ID), pdata, length);
		*(top + sizeof(ID) + length) = '\0';
		OffsetType ret = header->usage;
		header->usage += alllength;
		header->size++;
#else
		*(LengthType*)top = alllength;
		*(ID*)(top+sizeof(LengthType)) = id;
		memcpy(top+sizeof(LengthType)+sizeof(ID),pdata,length);
		OffsetType ret = header->usage;
		header->usage += (sizeof(LengthType) + alllength);
		header->size++;

#endif
		return ret;
	}

	void dump_int_string(FILE * out)
	{
		OffsetType off = first_offset();
		while(off){
			void * pdata;
			OffsetType length;
			get_by_offset(off,&length,&pdata);
			fprintf(out,"%u [",*(ID*)((char*)pdata - sizeof(ID)));
			LengthString((char*)pdata).dump(out);
			fprintf(out, "]\n");
			off = next_offset(off);
		}
	}

	static ObjectPool * create(int type,const char * name,ID init_capacity){
		ObjectPool * ret = new ObjectPool();
		ret->data = new MMapBuffer(name, sizeof(ObjectPoolMeta)+init_capacity);
		if(ret->data == NULL){
			delete ret;
			return NULL;
		}
		ObjectPoolMeta * meta = ret->get_meta();
		memset(meta,0,sizeof(ObjectPoolMeta));
		meta->entrysize = 0;
		meta->length = ret->data->get_length();
		meta->usage = sizeof(ObjectPoolMeta);
		meta->type = type;
		meta->size = 0;
		return ret;
	}

	static ObjectPool * load(const char * name){
		ObjectPool * ret = new ObjectPool();
		ret->data = MMapBuffer::create(name, 0);
		if(ret->data == NULL){
			delete ret;
			return NULL;
		}
		return ret;
	}
};

class FixedObjectPool {
protected:
	MMapBuffer * data;
	FixedObjectPool():data(0){}

	ObjectPoolMeta * get_meta(){
		return (ObjectPoolMeta*)(data->get_address());
	}

public:
	virtual ~FixedObjectPool(){
		delete data;
	}

	virtual void clear();

	virtual ID size() {
		return get_meta()->size;
	}

	virtual ulonglong usage() {
		return get_meta()->usage;
	}

	virtual Status optimaze(){
		Status retcode = data->resize(get_meta()->usage, false);
		if(retcode!=OK){
			printf("optimaze error\n");
			return retcode;
		}
		get_meta()->length = data->get_length();
		return OK;
	}

	virtual void reserve(OffsetType size){
		if( size*get_meta()->entrysize > data->get_length() ){
			Status ret = data->resize(size*get_meta()->entrysize, false);
			if(ret!=OK)
				return;
			ObjectPoolMeta * header = get_meta();
			header->length = size*header->entrysize;
		}
	}

	virtual OffsetType first_offset();

	virtual OffsetType next_offset(OffsetType offset);

	virtual Status get_by_offset(OffsetType offset, OffsetType * plength, void **ppdata);

	// id look up
	virtual Status get_by_id(ID id, OffsetType * plength, void **ppdata);

	ID append_object_get_id(const void * data);

	ID next_id(){
		ObjectPoolMeta * meta = get_meta();
		return meta->size + meta->id_offset;
	}

	void dump_int_string(FILE * out)
	{
		fprintf(out,"size: %u\n",size());
		/*
		OffsetType off = first_offset();
		while(off){
			void * pdata;
			OffsetType length;
			get_by_offset(off,&length,&pdata);
			fprintf(out,"%u [",*(OffsetType*)pdata);
			LengthString((char*)pdata+sizeof(ID),length-sizeof(ID)).dump(out);
			fprintf(out, "]\n");
			off = next_offset(off);
		}*/
	}

	bool initialize(int type,const char * name,ID init_capacity,OffsetType entrysize);

	static FixedObjectPool * create(int type,const char * name,ID init_capacity,OffsetType entrysize);

	static FixedObjectPool * load(const char * name);
};

#endif /* OBJECTPOOL_H_ */
