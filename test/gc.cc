#include <cstdlib>
#include <cerrno>
#include <cmath>
#include <string>
#include <queue>
#include <vector>
#include <sys/stat.h>

#include "sacio.h"
#include "support/ThreadPool.h"
#include "support/Logging.h"
#include "support/TimingEvent.h"
#include "support/Type.h"
#include "support/Exception.h"
#include "support/StdErrCategory.h"


/*** application ***/

float kTimeBefore = 0.5;
float kTimeAfter = 3.5;

struct Data {
  float* data;
  size_t size;
};

Data readTemp(std::string temp_path) {
  //read template
  SacInput temp_sac(temp_path);
  float delta = temp_sac.header().delta;
  float event_time = temp_sac.header().t1;
  float start_time = event_time - kTimeBefore;
  float end_time = event_time + kTimeAfter;
  float init_time = temp_sac.header().b;
  size_t start_bytes = rint((start_time - init_time) / delta) * sizeof(float);
  size_t temp_size = rint((end_time - start_time) / delta);
  size_t temp_bytes = temp_size * sizeof(float);
  float* temp_data = new float[temp_size];
  temp_sac.read(reinterpret_cast<char*>(temp_data), start_bytes, temp_bytes);

  Data ret;
  ret.data = temp_data;
  ret.size = temp_size;
  return ret;
}

Data readCont(std::string cont_path) {
  //read continuous
  SacInput cont_sac(cont_path);
  float delta = cont_sac.header().delta;
  float init_time = cont_sac.header().b;
  float final_time = cont_sac.header().e;
  size_t cont_size = cont_sac.header().npts;
  size_t cont_bytes = cont_size * sizeof(float);
  float* cont_data = new float[cont_size];
  cont_sac.read(reinterpret_cast<char*>(cont_data), 0, cont_bytes);

  Data ret;
  ret.data = cont_data;
  ret.size = cont_size;
  return ret;
}

void getTempMeanVar(float* data, unsigned size, float& mean, float& var) {
  mean = 0;
  for (int i = 0; i < size; ++i) {
    mean += data[i];
  }
  mean = mean / size;
  var = 0;
  for (int i = 0; i < size; ++i) {
    float res = data[i] - mean;
    var += res * res;
  }
}

void getContMeanVar(float* data, unsigned size, unsigned window_size,
                    float* mean, float* var) {
  for (int i = 0; i < size - window_size; ++i) {
    float m = 0;
    for (int j = 0; j < window_size; ++j) {
      m += data[i + j];
    }
    m = m / window_size;
    mean[i] = m;
    float v = 0;
    for (int j = 0; j < window_size; ++j) {
      float res = data[i + j] - m;
      v += res * res;
    }
    var[i] = v;
  }
}

void getCorr(float* corr, float* temp, float* cont, 
             unsigned cont_size, unsigned temp_size, 
             float temp_mean, float temp_var,
             float* cont_mean, float* cont_var) {
  for (int i = 0; i < cont_size - temp_size; ++i) {
    float ex = temp_mean;
    float ey = cont_mean[i];
    float sx = temp_var;
    float sy = cont_var[i];
    float sxy = 0;
    if (sx < 1.0e-5f || sy < 1.0e-5f) {
      corr[i] = 0;
    }
    else {
      for (int j = 0; j < temp_size; ++j) {
        float resx = temp[j] - ex;
        float resy = cont[i + j] - ey;
        sxy += resx * resy;
      }
      corr[i] = sxy / sqrt(sx * sy);
    }
  }
}

Data gc(Data temp, Data cont) {
  float* temp_data = temp.data;
  float* cont_data = cont.data;
  size_t temp_size = temp.size;
  size_t cont_size = cont.size;
  //mean and var
  float temp_mean, temp_var;
  getTempMeanVar(temp_data, temp_size, temp_mean, temp_var);
  float* cont_mean = new float[cont_size];
  float* cont_var = new float[cont_size];
  getContMeanVar(cont_data, cont_size, temp_size, cont_mean, cont_var);

  //correlation
  float* corr_data = new float[cont_size];
  getCorr(corr_data, temp_data, cont_data, cont_size, temp_size,
          temp_mean, temp_var, cont_mean, cont_var);

  delete [] cont_mean;
  delete [] cont_var;
  
  Data ret;
  ret.data = corr_data;
  ret.size = cont_size;
  return ret;
}

Data gc(std::string temp_path, std::string cont_path) {
  //get data
  Data temp = readTemp(temp_path);
  Data cont = readCont(cont_path);

  //kernel
  Data ret = gc(temp, cont);

  //clean up
  delete [] temp.data;
  delete [] cont.data;

  return ret;
}

/*** threads ***/

/*!\class WorkQueue
 * \brief A thread safe wrapper for std::queue
 */
template<typename T>
class WorkQueue {
 public:
  /* type */
  typedef T ValueType;
  /* testEmpty() */
  bool testEmpty() const { 
    return m_queue.empty(); 
  }
  /* empty() */
  bool empty() const { 
    support::Lock lock(m_mtx); 
    return m_queue.empty(); 
  }
  /* size() */
  size_t size() const { 
    support::Lock lock(m_mtx); 
    return m_queue.size(); 
  }
  /* enqueue() */
  void enqueue(const ValueType& elem) {
    support::Lock lock(m_mtx);
    m_queue.push(elem);
  }
  /* dequeue() */
  ValueType dequeue() { 
    support::Lock lock(m_mtx);
    ValueType ret = m_queue.front();
    m_queue.pop();
    return ret; 
  }
 private:
  std::queue<T> m_queue;
  support::Mutex m_mtx;
};

struct Work {
  int temp_idx;
  int cont_idx;
};

typedef std::vector<std::string> PathVector;
typedef std::vector<Data> DataVector;

template<typename VectorType>
struct ThreadArgs {
  support::Logger* logger;
  support::TimingEvent* timer;
  WorkQueue<Work>* work_queue;
  VectorType* temp_vector;
  VectorType* cont_vector;
};

template<typename VectorType>
void doWork(support::ThreadPool* pool,
            int tid,
            void* args) {
  ThreadArgs<VectorType>* thread_args = (ThreadArgs<VectorType>*) args;
  if (thread_args->work_queue->testEmpty()) {
    pool->exit();
    return;
  }
  Work work = thread_args->work_queue->dequeue();
  thread_args->logger->info("get work >> temp: " + 
                            support::Type2String<int>(work.temp_idx) + 
                            '\t' + "cont: " + 
                            support::Type2String<int>(work.cont_idx) + '\n');
  thread_args->timer->restart();
  Data corr = gc((*(thread_args->temp_vector))[work.temp_idx], 
                 (*(thread_args->cont_vector))[work.cont_idx]);
  thread_args->timer->pause();
  delete [] corr.data;
  thread_args->logger->info("finished\n");
}

/*** main ***/

enum WorkType {
  WorkOnPath = 0,
  WorkOnPtr,
};

std::vector<std::string> readList(std::string list_file) {
  std::vector<std::string> ret;
  std::ifstream if_list;
  std::string line;
  errno = 0;
  if_list.open(list_file.c_str());
  if (errno != 0) {
    throw support::Exception(errno, 
                             support::getErrorCategory<support::StdErrCategory>(),
                             "readList");
  }
  while(if_list.good()) {
    getline(if_list, line);
    if (line != "") ret.push_back(line);
  }
}

int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cerr << "Usage: gc <num_threads> <work_type>\n";
  }

  int num_threads = atoi(argv[1]);
  WorkType work_type = (WorkType)atoi(argv[2]);

  //init logging and timing
  support::LogSys::init();
  support::TimingSys::init();

  //init work_queue and vectors
  WorkQueue<Work> work_queue;
  PathVector temp_file_vector = readList("temp_list");
  PathVector cont_file_vector = readList("cont_list");

  //create user args
  DataVector temp_ptr_vector;
  DataVector cont_ptr_vector;

  ///read data if necessary
  if (work_type == WorkOnPtr) {
    support::TimingSys::newEvent("read data");
    support::TimingSys::startEvent("read data");
    for (int i = 0; i < temp_file_vector.size(); ++i) {
      Data temp = readTemp(temp_file_vector[i]);
      temp_ptr_vector.push_back(temp);
    }
    for (int i = 0; i < cont_file_vector.size(); ++i) {
      Data cont = readCont(cont_file_vector[i]);
      cont_ptr_vector.push_back(cont);
    }
    support::TimingSys::endEvent("read data");
  }
  ///append works to work queue
  for (int i = 0; i < temp_file_vector.size(); ++i) {
    for (int j = 0; j < cont_file_vector.size(); ++j) {
      Work work;
      work.temp_idx = i;
      work.cont_idx = j;
      work_queue.enqueue(work);
    }
  }
  ///create
  std::vector<void*> user_args;
  mkdir("gc_out", 0777);
  for (int i = 0; i < num_threads; ++i) {
    support::Logger& logger = 
        support::LogSys::newLogger("threadLog" + support::Type2String<int>(i));
    const support::HandlerRef& hdlr = 
        support::FileHandler::create("gc_out/" + support::Type2String<int>(i));
    logger.addHandler(hdlr);
    support::TimingSys::newEvent("threadEvent" + support::Type2String<int>(i));
    support::TimingEvent& timer = 
        support::TimingSys::get("threadEvent" + support::Type2String<int>(i));
    ThreadArgs<PathVector>* path_args = new ThreadArgs<PathVector>;
    ThreadArgs<DataVector>* ptr_args = new ThreadArgs<DataVector>;
    path_args->logger = &logger; ptr_args->logger = &logger;
    path_args->timer = &timer; ptr_args->timer = &timer;
    path_args->work_queue = &work_queue; ptr_args->work_queue = &work_queue;
    path_args->temp_vector = &temp_file_vector; ptr_args->temp_vector = &temp_ptr_vector;
    path_args->cont_vector = &cont_file_vector; ptr_args->cont_vector = &cont_ptr_vector;
    if (work_type == WorkOnPath) {
      user_args.push_back((void*)path_args);
    }
    else if (work_type == WorkOnPtr) {
      user_args.push_back((void*)ptr_args);
    }
  }

  //create thread pool
  support::TimingSys::newEvent("calculate total time");
  support::TimingSys::startEvent("calculate total time");
  support::ThreadsAttr attr;
  support::ThreadPool pool(num_threads, attr);
  if (work_type == WorkOnPath) {
    void (*doWorkFunc)(support::ThreadPool*, int, void*) = &doWork<PathVector>;
    pool.set(doWorkFunc, user_args);
  }
  else if (work_type == WorkOnPtr) {
    void (*doWorkFunc)(support::ThreadPool*, int, void*) = &doWork<DataVector>;
    pool.set(doWorkFunc, user_args);
  }
  pool.execute();
  pool.join();
  support::TimingSys::endEvent("calculate total time");

  //output timing
  support::Logger& logger = support::LogSys::newLogger("perf");
  support::HandlerRef hdlr = support::FileHandler::create("gc_out/perf");
  logger.addHandler(hdlr);
  for (support::TimingSys::iterator it = support::TimingSys::begin();
       it != support::TimingSys::end(); ++it) {
    support::LogSys::getLogger("perf").info(
        (*it).name() + "\n" + 
        "total: " + support::Time2String((*it).run_dur(), "%Ti ms\n") + 
        "ave: " + support::Time2String((*it).ave_dur(), "%Ti ms\n") + 
        "num: " + support::Type2String<int>((*it).num_pauses()) + 
        "\n\n");
  }
}

