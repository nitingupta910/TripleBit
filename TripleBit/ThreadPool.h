#ifndef THREADPOOL_H_
#define THREADPOOL_H_

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

#include <vector>
#include <pthread.h>
#include <iostream>
#include <boost/function.hpp>

using namespace std;

class CThreadPool;

class CTask {
protected:
	string m_strTaskName; //the name of the task
	void* m_ptrData; //the data of the task to be executed
public:
	CTask() {}
	virtual ~CTask() {}
	CTask(string taskName) {
		this->m_strTaskName = taskName;
		m_ptrData = NULL;
	}
	virtual int Run()= 0;
	void SetData(void* data); //set task data
};

class CThreadPool {
public:
	typedef boost::function<void ()> Task;
private:
	vector<Task> m_vecTaskList; //task list
	int m_iThreadNum; //the No of threads
	vector<pthread_t> m_vecIdleThread; //idle thread list
	vector<pthread_t> m_vecBusyThread; //busy thread list

	static CThreadPool* instance;				//the thread pool instance;
protected:
	friend class CTask;
	static void* ThreadFunc(void * threadData); //new thread function
	int MoveToIdle(pthread_t tid); //move the idle when the task complete
	int MoveToBusy(pthread_t tid); //move the tid to busy list
	int Create(); //create task
public:
	static CThreadPool& getInstance();
	pthread_mutex_t m_pthreadMutex; //used to syn
	pthread_mutex_t m_pthreadIdleMutex;
	pthread_mutex_t m_pthreadBusyMutex;
	pthread_cond_t m_pthreadCond; //used to syn
	pthread_cond_t m_pthreadEmpty;
	pthread_cond_t m_pthreadBusyEmpty;
	bool shutdown;
	CThreadPool(int threadNum);
	~CThreadPool();
	int AddTask(const Task& task);	// Add the task to List
	int StopAll();
	int Wait();				 //waiting for task complete!
};

struct ThreadPoolArg
{
	CThreadPool* pool;
	vector<CThreadPool::Task>* taskList;
};
#endif /* THREADPOOL_H_ */
