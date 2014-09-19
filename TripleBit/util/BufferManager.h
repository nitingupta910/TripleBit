#ifndef BUFFERMANAGER_H_
#define BUFFERMANAGER_H_

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

#define INIT_BUFFERS 5
#define INCREASE_BUFFERS 5

class EntityIDBuffer;

#include "../TripleBit.h"

class BufferManager {
private:
        vector<EntityIDBuffer*> bufferPool;
        vector<EntityIDBuffer*> usedBuffer;
        vector<EntityIDBuffer*> cleanBuffer;

        int bufferCnt;
protected:
        static BufferManager* instance;
        BufferManager();
        bool expandBuffer();
public:
        virtual ~BufferManager();
        EntityIDBuffer* getNewBuffer();
        Status freeBuffer(EntityIDBuffer* buffer);
        Status reserveBuffer();
        void destroyBuffers();
public:
        static BufferManager* getInstance();
};


#endif /* BUFFERMANAGER_H_ */
