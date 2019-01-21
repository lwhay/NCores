//
// Created by Michael on 2018/12/2.
//

#pragma once

template<typename type>
class CoresRandomAccesessor {
    virtual void set(int index, type value) = 0;

    virtual type get(int index) = 0;
};
