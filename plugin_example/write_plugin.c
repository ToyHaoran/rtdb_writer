#include <stdio.h>
#include "write_plugin.h"
#include <Session.h>
#include <string>

using namespace std;

string baseRoot = "root.sg";
Session *session;

int64_t getCurrentTimeMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}
template <typename T>
char* toCharPtr(const T& value) {
    std::stringstream ss;
    ss << value;
    std::string str = ss.str();
    char* cstr = (char*)malloc((str.size() + 1) * sizeof(char));
    std::strcpy(cstr, str.c_str());
    return cstr;
}

// 登陆数据库
void login(char *param) {
    if (param != NULL) {
        printf("rtdb login: param: %s\n", param);
    } else {
        printf("rtdb login: param: NULL\n");
    }
    LOG_LEVEL = LEVEL_DEBUG;

    session = new Session("192.168.150.100", 6667, "root", "root");
    session->open(false);
}

// 登出数据库
void logout() {
    free(session);
    printf("登出数据库\n");
}

// 1写实时模拟量
void write_rt_analog(int64_t unit_id, int64_t time, Analog *analog_array_ptr, int64_t count) {


    printf("write rt analog: unit_id: %lld, time: %lld, count: %lld\n", unit_id, time, count);
    int sum = 0;
    for (int i=0; i<10000000; i++) {
        sum++;
    }
}

// 2写实时数字量
void write_rt_digital(int64_t unit_id, int64_t time, Digital *digital_array_ptr, int64_t count) {
    printf("write rt digital: unit_id: %lld, time: %lld, count: %lld\n", unit_id, time, count);
}

// 1.1 写实时模拟量
void write_rt_analog_list(int64_t unit_id, int64_t *time, Analog **analog_array_array_ptr, int64_t *array_count, int64_t count) {
    printf("write rt analog: unit_id: %lld, section count: %lld\n", unit_id, count);
}

// 2.1 写实时数字量
void write_rt_digital_list(int64_t unit_id, int64_t *time, Digital **digital_array_array_ptr, int64_t *array_count, int64_t count) {
    printf("write rt digital: unit_id: %lld, section count: %lld\n", unit_id, count);
}

// 3 写历史模拟量
void write_his_analog(int64_t unit_id, int64_t time, Analog *analog_array_ptr, int64_t count) {
    printf("write his analog: unit_id: %lld, time: %lld, count: %lld\n", unit_id, time, count);
    int sum = 0;
    for (int i=0; i<10000000; i++) {
        sum++;
    }
}

// 4 写历史数字量
void write_his_digital(int64_t unit_id, int64_t time, Digital *digital_array_ptr, int64_t count) {
    printf("write his digital: unit_id: %lld, time: %lld, count: %lld\n", unit_id, time, count);
}

// 5 写静态模拟量
void write_static_analog(int64_t unit_id, StaticAnalog *static_analog_array_ptr, int64_t count) {
    StaticAnalog *arr = static_analog_array_ptr;

    vector<string> deviceIds;
    vector<int64_t> timestamps;
    vector<vector<string>> measurementsList;
    vector<vector<TSDataType::TSDataType>> typesList;
    vector<vector<char *>> valuesList;

    for (int i = 0; i < sizeof(arr); ++i)
    {
        StaticAnalog sa = arr[i];
        deviceIds.push_back(baseRoot +"unit" + std::to_string(unit_id) + "dev_static_analog");
        timestamps.push_back(getCurrentTimeMillis());
        measurementsList.push_back({"P_NUM", "TAGT", "FACK", "L4AR", "L3AR", "L2AR", "L1AR", "H4AR", "H3AR", "H2AR", "H1AR", "CHN", "PN", "DESC", "UNIT", "MU", "MD"});
        typesList.push_back({TSDataType::INT32, TSDataType::INT32, TSDataType::INT32, TSDataType::BOOLEAN, TSDataType::BOOLEAN, TSDataType::BOOLEAN, TSDataType::BOOLEAN, TSDataType::BOOLEAN, TSDataType::BOOLEAN, TSDataType::BOOLEAN, TSDataType::BOOLEAN, TSDataType::TEXT, TSDataType::TEXT, TSDataType::TEXT, TSDataType::TEXT, TSDataType::FLOAT, TSDataType::FLOAT});
        valuesList.push_back({toCharPtr(sa.p_num), toCharPtr(sa.tagt), toCharPtr(sa.fack), toCharPtr(sa.l4ar), toCharPtr(sa.l3ar), toCharPtr(sa.l2ar), toCharPtr(sa.l1ar), toCharPtr(sa.h4ar), toCharPtr(sa.h3ar),toCharPtr(sa.h2ar), toCharPtr(sa.h1ar), toCharPtr(sa.chn), toCharPtr(sa.pn), toCharPtr(sa.desc), toCharPtr(sa.unit), toCharPtr(sa.mu), toCharPtr(sa.md)});
    }
    session->insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);
}

// 6 写静态数字量
void write_static_digital(int64_t unit_id, StaticDigital *static_digital_array_ptr, int64_t count) {
    printf("write static digital: unit_id: %lld, count: %lld\n", unit_id, count);
}


