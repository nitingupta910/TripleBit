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

#include "ThreadPool.h"
#include "TripleBit.h"

#include <string>
#include <iostream>

using namespace std;

void CTask::SetData(void * data) {
	m_ptrData = data;
}


//////////////////////////////////////////////////////////////////////
////// class CThreadPool
//////////////////////////////////////////////////////////////////////
CThreadPool* CThreadPool::instance = NULL;

CThreadPool::CThreadPool(int threadNum) {
	this->m_iThreadNum = threadNum;
	pthread_mutex_init(&m_pthreadMutex,NULL);
	pthread_mutex_init(&m_pthreadIdleMutex,NULL);
	pthread_mutex_init(&m_pthreadBusyMutex,NULL);
	pthread_cond_init(&m_pthreadCond,NULL);
	pthread_cond_init(&m_pthreadEmpty,NULL);
	pthread_cond_init(&m_pthreadBusyEmpty,NULL);
	shutdown = false;
	Create();
}

CThreadPool::~CThreadPool()
{
	this->StopAll();
	pthread_mutex_destroy(&m_pthreadMutex);
	pthread_mutex_destroy(&m_pthreadIdleMutex);
	pthread_mutex_destroy(&m_pthreadBusyMutex);
	pthread_cond_destroy(&m_pthreadCond);
	pthread_cond_destroy(&m_pthreadEmpty);
	pthread_cond_destroy(&m_pthreadBusyEmpty);
}

int CThreadPool::MoveToIdle(pthread_t tid) {
	pthread_mutex_lock(&m_pthreadBusyMutex);
	vector<pthread_t>::iterator busyIter = m_vecBusyThread.begin();
	while (busyIter != m_vecBusyThread.end()) {
		if (tid == *busyIter) {
			break;
		}
		busyIter++;
	}
	m_vecBusyThread.erase(busyIter);

	if ( m_vecBusyThread.size() == 0) {
		pthread_cond_broadcast(&m_pthreadBusyEmpty);
	}

	pthread_mutex_unlock(&m_pthreadBusyMutex);

	pthread_mutex_lock(&m_pthreadIdleMutex);
	m_vecIdleThread.push_back(tid);
	pthread_mutex_unlock(&m_pthreadIdleMutex);
	return 0;
}

int CThreadPool::MoveToBusy(pthread_t tid) {
	pthread_mutex_lock(&m_pthreadIdleMutex);
	vector<pthread_t>::iterator idleIter = m_vecIdleThread.begin();
	while (idleIter != m_vecIdleThread.end()) {
		if (tid == *idleIter) {
			break;
		}
		idleIter++;
	}
	m_vecIdleThread.erase(idleIter);
	pthread_mutex_unlock(&m_pthreadIdleMutex);

	pthread_mutex_lock(&m_pthreadBusyMutex);
	m_vecBusyThread.push_back(tid);
	pthread_mutex_unlock(&m_pthreadBusyMutex);
	return 0;
}

void* CThreadPool::ThreadFunc(void * threadData) {
	pthread_t tid = pthread_self();
	int rnt;
	ThreadPoolArg* arg = (ThreadPoolArg*)threadData;
	vector<Task>* taskList = arg->taskList;
	CThreadPool* pool = arg->pool;
	while (1) {
		rnt = pthread_mutex_lock(&pool->m_pthreadMutex);
		if ( rnt != 0){
			cout<<"Get mutex error"<<endl;
		}

		while( taskList->size() == 0 && pool->shutdown == false){
			pthread_cond_wait(&pool->m_pthreadCond, &pool->m_pthreadMutex);
		}

		if ( pool->shutdown == true){
			pthread_mutex_unlock(&pool->m_pthreadMutex);
			pthread_exit(NULL);
		}

		pool->MoveToBusy(tid);
		Task task = Task(taskList->front());
		taskList->erase(taskList->begin());

		if ( taskList->size() == 0 ) {
			pthread_cond_broadcast(&pool->m_pthreadEmpty);
		}
		pthread_mutex_unlock(&pool->m_pthreadMutex);
		task();
		pool->MoveToIdle(tid);
	}
	return (void*) 0;
}

int CThreadPool::AddTask(const Task& task) {
	pthread_mutex_lock(&m_pthreadMutex);
	this->m_vecTaskList.push_back(task);
	pthread_mutex_unlock(&m_pthreadMutex);
	pthread_cond_broadcast(&m_pthreadCond);
	return 0;
}

int CThreadPool::Create() {
	m_vecTaskList.clear();
	struct ThreadPoolArg* arg = new ThreadPoolArg;
	pthread_mutex_lock(&m_pthreadIdleMutex);
	for (int i = 0; i < m_iThreadNum; i++) {
		pthread_t tid = 0;
		arg->pool = this;
		arg->taskList = &m_vecTaskList;
		pthread_create(&tid, NULL, ThreadFunc, arg);
		m_vecIdleThread.push_back(tid);
	}
	pthread_mutex_unlock(&m_pthreadIdleMutex);
	return 0;
}

int CThreadPool::StopAll() {
	shutdown = true;
	pthread_mutex_unlock(&m_pthreadMutex);
	pthread_cond_broadcast(&m_pthreadCond);
	vector<pthread_t>::iterator iter = m_vecIdleThread.begin();
	while (iter != m_vecIdleThread.end()) {
		pthread_join(*iter, NULL);
		iter++;
	}
	
	iter = m_vecBusyThread.begin();
	while (iter != m_vecBusyThread.end()) {
		pthread_join(*iter, NULL);
		iter++;
	}

	return 0;
}

int CThreadPool::Wait()
{
	int rnt;

	rnt = pthread_mutex_lock(&m_pthreadMutex);
	while( m_vecTaskList.size() != 0 ) {
		pthread_cond_wait(&m_pthreadEmpty, &m_pthreadMutex);
	}
	rnt = pthread_mutex_unlock(&m_pthreadMutex);
	pthread_mutex_lock(&m_pthreadBusyMutex);
	while( m_vecBusyThread.size() != 0) {
		pthread_cond_wait(&m_pthreadBusyEmpty, &m_pthreadBusyMutex);
	}
	pthread_mutex_unlock(&m_pthreadBusyMutex);
	return 0;
}

CThreadPool& CThreadPool::getInstance()
{
	if(instance == NULL) {
		instance = new CThreadPool(THREAD_NUMBER);
	}
	return *instance;
}
