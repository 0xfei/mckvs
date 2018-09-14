#include <iostream>

#include "mem/mem_table.h"
#include "mem/base_block.h"

using namespace std;

using SV = MCKVS::StringValue<std::string>;

int main() {
    MCKVS::MemTable table;


    auto value = std::make_shared<SV>("hhxlovety");
    const std::string key = "/tt/vv";

    table.Set(key, value);

    std::vector<MCKVS::BlockPtr> vs;
    table.Get(key, vs);

    std::string v;
    static_pointer_cast<SV>(vs[0])->GetValue(v);
    cout << v << endl;

    table.Del(key);

    cout << "Endl" << endl;
    return 0;
}
