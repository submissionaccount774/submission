#include "../xdbc/xclient.h"
#include <string>

class Tester {

public:

    Tester(std::string name, xdbc::RuntimeEnv &env);

    void runAnalytics();

    void runStorage(const std::string &filename);

    void close();

private:
    xdbc::RuntimeEnv *env;
    xdbc::XClient xclient;
    std::string name;

    int analyticsThread(int thr, int &mins, int &maxs, long &sums, long &cnts, long &totalctns);

    int storageThread(int thr, const std::string &filename);


};