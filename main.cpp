#include <list>
#include <iostream>
#include <fstream>
#include <functional>
#include <stdlib.h>
#include <ctime>
#include <unistd.h>
#include <thread>
#include <vector>
#include <map>
#include <mutex>

using Bulk = std::list<std::string>;

class Worker
{
public:
    Worker(std::function<void(Bulk)> workFunction) : m_running(false), m_workFunction(workFunction)
    {
        start();
    }

    Worker(Worker &&other)
    {
        m_workFunction = std::move(other.m_workFunction);
        m_queue = std::move(other.m_queue);
        start();
    }

    ~Worker()
    {
        stop();
    }

    void push_back(Bulk commands)
    {
        {
            std::lock_guard<std::mutex> lk(m_mutex);
            m_queue.push_back(commands);
        }
        m_condition.notify_all();
    }

    void start()
    {
        {
            std::lock_guard<std::mutex> lk(m_mutex);
            if (m_running == true) return;
            m_running = true;
        }

        m_thread = std::thread([this]
        {
            for (;;)
            {
                decltype(m_queue) local_queue;
                {
                    std::unique_lock<std::mutex> lk(m_mutex);
                    m_condition.wait(lk, [&] { return !m_queue.empty() + !m_running; });

                    if (!m_running)
                    {
                        for (auto& data : m_queue)
                            m_workFunction(data);

                        m_queue.clear();
                        return;
                    }

                    local_queue = std::move(m_queue);
                }

                for (auto& data : local_queue)
                    m_workFunction(data);
            }
        });
        m_thread_id = m_thread.get_id();
    }

    void stop()
    {
        {
            std::lock_guard<std::mutex> lk(m_mutex);
            if (m_running == false) return;
            m_running = false;
        }

        m_condition.notify_all();
        m_thread.join();
    }

    std::thread::id getThreadId ()
    {
        return m_thread_id;
    }
private:
    std::function<void(Bulk)> m_workFunction;
    std::condition_variable m_condition;
    std::list<Bulk> m_queue;
    std::mutex m_mutex;
    std::thread m_thread;
    std::thread::id m_thread_id; //save thread id after thread stopped
    bool m_running = false;
};

class Parser
{
    enum class ParsingState
    {
        TopLevel = 0,
        InBlock = 1
    };

public:
    Parser (int bulkSize)
        : m_bulkSize(bulkSize)
        , m_lineCount(0)
        , m_commandCount(0)
        , m_blockCount(0)
    { }

    void exec()
    {
        ParsingState state = ParsingState::TopLevel;
        Bulk commands;
        int depthCounter = 0;

        for(std::string line; std::getline(std::cin, line);)
        {
            m_lineCount++;
            switch (state)
            {
            case ParsingState::TopLevel:
            {
                if (line != "{")
                {
                    commands.push_back(line);
                    if (commands.size() == m_bulkSize)
                        publish(commands);
                    break;
                }
                else
                {
                    depthCounter++;
                    publish(commands);
                    state = ParsingState::InBlock;
                    break;
                }
            }
            case ParsingState::InBlock:
            {
                if (line != "}")
                {
                    if (line == "{")
                        depthCounter++;
                    else
                        commands.push_back(line);
                }
                else
                {
                    depthCounter--;
                    if (depthCounter == 0)
                    {
                        publish(commands);
                        state = ParsingState::TopLevel;
                    }
                }
                break;
            }
            default:
                break;
            }
        }

        if (state == ParsingState::TopLevel)
            publish(commands);
    }

    void subscribe(const std::function<void(Bulk)>& callback)
    {
        m_subscribers.push_back(callback);
    }

    void publish(Bulk &commands)
    {
        if (commands.size() == 0)
            return;

        m_commandCount += commands.size();
        m_blockCount++;

        for (const auto& subscriber : m_subscribers)
        {
            subscriber(commands);
        }
        commands.clear();
    }

    void printStats()
    {
        std::cout << std::this_thread::get_id() << " Lines " << m_lineCount << std::endl;
        std::cout << std::this_thread::get_id() << " Blocks " << m_blockCount << std::endl;
        std::cout << std::this_thread::get_id() << " Commands " << m_commandCount << std::endl;
    }

private:
    std::list<std::function<void(Bulk)> > m_subscribers;
    int m_bulkSize;

    int m_lineCount;
    int m_commandCount;
    int m_blockCount;
};

class IBulker
{
public:
    virtual void push_back(Bulk commands) = 0;
    virtual void stop() = 0;

    void printStats()
    {
        std::cout << "Blocks" << std::endl;
        for (std::map<Worker *,int>::iterator it = m_blockCount.begin(); it!=m_blockCount.end(); ++it)
            std::cout << "  " << it->first->getThreadId() << " => " << it->second << std::endl;

        std::cout << "Commands" << std::endl;
        for (std::map<Worker *,int>::iterator it = m_commandCount.begin(); it!=m_commandCount.end(); ++it)
            std::cout << "  " << it->first->getThreadId() << " => " << it->second << std::endl;
    }

protected:

    void initStats(Worker *worker)
    {
        if (m_blockCount.find(worker) == m_blockCount.end())
            m_blockCount.insert( std::pair<Worker *, int>(worker, 0) );

        if (m_commandCount.find(worker) == m_commandCount.end())
            m_commandCount.insert( std::pair<Worker *, int>(worker, 0) );
    }

    void calcStats(Worker *worker, Bulk commands)
    {
        initStats(worker);

        m_blockCount[worker]++;
        m_commandCount[worker] += commands.size();
    }

    std::map<Worker *, int> m_blockCount;
    std::map<Worker *, int> m_commandCount;
};

class ScreenWritter : public IBulker
{
public:

    ScreenWritter()
        : m_worker(std::bind(&ScreenWritter::write, this, std::placeholders::_1)) { }

    void push_back(Bulk commands)
    {
        m_worker.push_back(commands);
        calcStats(&m_worker, commands);
    }

    void stop()
    {
        m_worker.stop();
    }

    void write(Bulk commands)
    {
        std::cout << std::this_thread::get_id() << " " << "bulk:";
        for (auto command : commands)
            std::cout << command << " ";
        std::cout << std::endl;
    }

private:
    Worker m_worker;
};

class FileWriter : public IBulker
{
public:
    FileWriter(int wrkCount)
        : m_roundRobin(0)
    {
        for (int i = 0; i < wrkCount; ++i)
        {
            m_workers.push_back(std::move(Worker(std::bind(&FileWriter::write, this, std::placeholders::_1))));
        }
    }

    void push_back(Bulk commands)
    {
        m_workers.at(m_roundRobin).push_back(commands);
        calcStats(&m_workers.at(m_roundRobin), commands);
        m_roundRobin++;
        if (m_roundRobin == m_workers.size())
            m_roundRobin = 0;
    }

    void stop()
    {
        for (int i = 0; i < m_workers.size(); ++i)
        {
            initStats(&m_workers.at(i)); //init empty stats for all workers without executed commands
            m_workers.at(i).stop();
        }
    }

    void write(Bulk commands)
    {
        static int conflictResolverCounter = 0;
        static std::mutex conflictMutex;

        std::ofstream logFile;

        std::time_t result = std::time(nullptr);
        char buff[FILENAME_MAX];
        getcwd(buff, FILENAME_MAX );
        std::string current_working_dir(buff);
        {
            std::lock_guard<std::mutex> lk(conflictMutex);
            logFile.open (std::string(current_working_dir + "/bulk" + std::to_string(result) + "_" + std::to_string(conflictResolverCounter) + ".log"));
            std::cout << std::this_thread::get_id() << " " << std::string(current_working_dir + "/bulk" + std::to_string(result) + "_" + std::to_string(conflictResolverCounter) + ".log") << std::endl;
            conflictResolverCounter++;
        }
        logFile << "bulk:";
        for (auto command : commands)
            logFile << command << " ";
        logFile << std::endl;

        logFile.close();
    }

private:
    std::vector<Worker> m_workers;
    int m_roundRobin;
};

//$ bulkmt < bulk1.txt
int main(int argc, const char *argv[])
{
    int bulkCount = 5;
    if (argc == 2)
    {
        char *p;
        bulkCount = std::strtol(argv[1], &p, 10);
    }

    Parser parser(bulkCount);

    ScreenWritter screenWritter;
    parser.subscribe(std::bind(&ScreenWritter::push_back, &screenWritter, std::placeholders::_1));

    FileWriter fileWriter(2); //set file writter thread count here
    parser.subscribe(std::bind(&FileWriter::push_back, &fileWriter, std::placeholders::_1));

    parser.exec();

    screenWritter.stop();
    fileWriter.stop();

    std::cout << std::endl << "MAIN" << std::endl;
    parser.printStats();

    std::cout << std::endl << "LOG" << std::endl;
    screenWritter.printStats();

    std::cout << std::endl << "FILE" << std::endl;
    fileWriter.printStats();

    return 0;
}
