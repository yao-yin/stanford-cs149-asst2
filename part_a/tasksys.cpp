#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), workers(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    int n = workers.size();

    for (int i = 0; i < num_total_tasks; i++) {
        int idx = i % n;
        if (workers[idx].joinable()) {
            workers[idx].join();
        }
        workers[idx] = std::thread(&IRunnable::runTask, runnable, i, num_total_tasks);
    }
    for (auto && t: workers) {
        if (t.joinable())
            t.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    done = false;
    for (int i = 0; i < num_threads; ++ i) {
        try {
            threads.push_back(std::thread(&TaskSystemParallelThreadPoolSpinning::worker_thread, this));
        } catch(...) {
            done = true;
            throw;
        }
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    for (auto && t: threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    done = true;
}

template<typename FunctionType>
void TaskSystemParallelThreadPoolSpinning::submit(FunctionType f) {
    std::lock_guard<std::mutex> locker(mtx_queue);
    tasks_queue.push(f);
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    done = false;
    finished_tasks_count = 0;
    //tot_tasks = 1;
    tot_tasks = num_total_tasks;
    for (int i = 0; i < tot_tasks; i++) {
    
        std::function<void()> task = std::bind(&IRunnable::runTask, runnable, i, num_total_tasks);
        submit(task);
    }
    //std::unique_lock<std::mutex> locker(mtx_queue);
    //cond_.wait(locker, [&]() {return finished_tasks_count == tot_tasks;});
    //std::this_thread::sleep_for(std::chrono::milliseconds(100));
    //finished_tasks_count = 0;
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    return;
}

void TaskSystemParallelThreadPoolSpinning::worker_thread() {
    while (!done) {
        std::function<void()> task;
        bool hasTask = false;
        {
            std::lock_guard<std::mutex> loker(mtx_queue);
            if (!tasks_queue.empty()) {
                task = tasks_queue.front();
                hasTask = true;
                tasks_queue.pop();
            } else {
                std::this_thread::yield();
            }
        }
        if (hasTask) {
            task();
            hasTask = false;
            std::lock_guard<std::mutex> lk(mtx_cout);
            ++ finished_tasks_count;
            if (finished_tasks_count == tot_tasks) done = true;
            //std::cout << "I have already excute tasks: " << finished_tasks_count <<" " << tot_tasks << std::endl;
        }
    }
}


/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): 
ITaskSystem(num_threads), n(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
