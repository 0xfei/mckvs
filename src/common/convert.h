//
// Created by hhx on 2018/9/14.
//

#ifndef TBKVS_CONVERT_H
#define TBKVS_CONVERT_H

#include <vector>
#include <string>
#include <regex>

namespace MCKVS
{

namespace UTIL
{

static const std::regex re{"/"};

std::vector<std::string> split(const std::string& s) {
    return {
        std::sregex_token_iterator(s.begin(), s.end(), re, -1),
        std::sregex_token_iterator()
    };
}

}
}



#endif //TBKVS_CONVERT_H
