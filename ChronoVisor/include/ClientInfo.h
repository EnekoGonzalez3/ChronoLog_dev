//
// Created by kfeng on 1/2/23.
//

#ifndef CHRONOLOG_CLIENTINFO_H
#define CHRONOLOG_CLIENTINFO_H


class ClientInfo {
public:
    ClientInfo() = default;

    std::string addr_;
    uint16_t port_{};
};


#endif //CHRONOLOG_CLIENTINFO_H
