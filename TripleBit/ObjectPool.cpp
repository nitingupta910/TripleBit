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

#include "Hash.h"
#include "ObjectPool.h"

void FixedObjectPool::clear()
{
	ObjectPoolMeta * meta = get_meta();
	meta->size = 0;
	meta->usage = sizeof(ObjectPoolMeta);
}

OffsetType FixedObjectPool::next_offset( OffsetType offset )
{
	OffsetType ret = offset+1;
	return ret<=get_meta()->size?ret:0;
}

Status FixedObjectPool::get_by_offset( OffsetType offset, OffsetType * plength, void **ppdata )
{
	ObjectPoolMeta * meta = get_meta();
	assert(offset <= meta->size);
	if(plength)
		*plength = meta->entrysize;
	*ppdata = data->get_address() + sizeof(ObjectPoolMeta) + (offset-1)*(meta->entrysize);
	return OK;
}


Status FixedObjectPool::get_by_id( ID id, OffsetType * plength, void **ppdata )
{
	// TODO: optimaze
	ObjectPoolMeta * meta = get_meta();
	char * p = data->get_address() + sizeof(ObjectPoolMeta);
	char * end = data->get_address() + meta->usage;

	id = id - 1;
	*ppdata = p + id * meta->entrysize;
	*plength = meta->entrysize;
	if(*ppdata < end)
		return OK;
	return NOT_FOUND;
}

ID FixedObjectPool::append_object_get_id( const void * pdata )
{
	ObjectPoolMeta * meta = get_meta();

	if( meta->usage + meta->entrysize > meta->length ){
		OffsetType new_length = expand(meta->length,meta->entrysize,meta->classtype);
		Status ret = data->resize(new_length, false);
		if(ret!=OK)
			return 0;
		meta = get_meta();
		meta->length = new_length;
	}

	memcpy( data->get_address() + meta->usage , pdata, get_meta()->entrysize );
	meta->usage += meta->entrysize;
	return meta->size++ + meta->id_offset;
}

bool FixedObjectPool::initialize( int type,const char * name,ID init_capacity,OffsetType entrysize )
{
	if(data)
		delete data;
	data = new MMapBuffer(name, sizeof(ObjectPoolMeta)+init_capacity*entrysize);
	if(data == NULL){
		// TODO: log
		return false;
	}
	ObjectPoolMeta * meta = get_meta();
	memset(meta,0,sizeof(ObjectPoolMeta));
	meta->entrysize = entrysize;
	meta->length = data->get_length();
	meta->usage = sizeof(ObjectPoolMeta);
	meta->type = type;
	meta->size = 1;
	return true;
}

FixedObjectPool * FixedObjectPool::create( int type,const char * name,ID init_capacity,OffsetType entrysize )
{
	FixedObjectPool * ret = new FixedObjectPool();
	if(ret->initialize(type,name,init_capacity,entrysize)==false){
		delete ret;
		return NULL;
	}
	return ret;
}

FixedObjectPool* FixedObjectPool::load(const char* name)
{
	FixedObjectPool* ret = new FixedObjectPool();
	ret->data = MMapBuffer::create(name,0);
	if(ret->data == NULL){
		// TODO: log
		delete ret;
		return NULL;
	}
	return ret;
}

OffsetType FixedObjectPool::first_offset()
{
	if(get_meta()->size>0)
		return 1;
	else
		return 0;
}
