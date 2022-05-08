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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

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

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    return;
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
ITaskSystem(num_threads), call_counter(-1), remain_taskbulks_num(0) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    done = false;
    
    for (int i = 0; i < num_threads; i ++) {
        threads.push_back(std::thread(&TaskSystemParallelThreadPoolSleeping::worker_thread, this));
    }

}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    //std::cout << "dtor" << std::endl;
    //sync();
    done = true;
    for (auto && t: threads) {
        if (t.joinable()) t.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    ++ call_counter;
    remain_taskbulks_num ++;
    {
        std::scoped_lock locker {mtx_all_tasks, mtx_remain_tasks, mtx_dep_out};
        all_tasks.push_back({runnable, num_total_tasks});
        remain_tasks.push_back(num_total_tasks);
        dep_out.push_back({});
    }
    int dep_in_degree_count = 0;
    // std::cout << "ready to check deps: " << remain_tasks.size() <<" & " <<dep_out.size() << std::endl;
    {
        std::scoped_lock locks {mtx_remain_tasks, mtx_dep_in_degree, mtx_dep_out};
        for (auto x: deps) {
            if (remain_tasks[x] != 0) {
                dep_in_degree_count ++;
                dep_out[x].push_back(call_counter);
            }
        }
        dep_in_degree.push_back(dep_in_degree_count);
    }
    // std::cout << "ready to add to queue" << std::endl;
    if (dep_in_degree_count == 0) {
        add_to_queue(call_counter);
    }
    return call_counter;
}

void TaskSystemParallelThreadPoolSleeping::worker_thread() {
    while (!done) {
        Task task;
        bool hasTask = false;
        {
            std::lock_guard<std::mutex> locker(mtx_ready_task_queue);
            if (!ready_task_queue.empty()) {
                hasTask = true;
                task = ready_task_queue.front();
                ready_task_queue.pop();
            } else {
                std::this_thread::yield();
            }
        }

        if (hasTask) {
            task.func();
            int remain_tasks_num = -1;
            {
                std::lock_guard<std::mutex> lk(mtx_remain_tasks);
                remain_tasks_num = -- remain_tasks[task.taskId];
            }
            if (remain_tasks_num == 0) {
                remain_taskbulks_num --;
                // std::cout << task.taskId << " has been done" << remain_taskbulks_num << std::endl;
                if (remain_taskbulks_num == 0) {
                    cond.notify_one();
                }
                std::vector<TaskID> ready_to_add_to_queue;
                {
                    std::scoped_lock loker {mtx_dep_in_degree, mtx_dep_out};
                    for (auto x: dep_out[task.taskId]) {
                        if (dep_in_degree[x] > 0) {
                            dep_in_degree[x] --;
                            if (dep_in_degree[x] == 0) {
                                ready_to_add_to_queue.push_back(x);
                            }
                        }
                    }
                }
                for (auto x: ready_to_add_to_queue) add_to_queue(x);
            }
        }
    }
} 

void TaskSystemParallelThreadPoolSleeping::add_to_queue(TaskID task_id) {
    
    std::scoped_lock locker {mtx_all_tasks, mtx_ready_task_queue};
    std::unique_lock lk(mtx_dep_in_degree);
    assert(dep_in_degree[task_id] == 0);
    lk.unlock();

    BulkTask bulktask = all_tasks[task_id];
    for (int i = 0; i < bulktask.num_total_tasks; i ++) {
        std::function<void()> task_func = std::bind(&IRunnable::runTask, bulktask.runnable, i, bulktask.num_total_tasks);
        ready_task_queue.push({task_func, task_id});
    }
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //
    // std::cout << "called sync" << std::endl;
    std::unique_lock locker(mtx_ready_task_queue);
    cond.wait(locker, [&](){return remain_taskbulks_num == 0;});
    return;
}
