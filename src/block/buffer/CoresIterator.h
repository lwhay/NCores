//
// Created by Michael on 2018/12/2.
//

#pragma once

template<typename type>
class CoresIterator {
    virtual void open() = 0;

    virtual bool hashNext() = 0;

    virtual type next() = 0;
};