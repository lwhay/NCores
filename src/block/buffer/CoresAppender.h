//
// Created by Michael on 2019-02-18.
//
#pragma once

template<typename type>
class CoresAppender {
    virtual void append(type value) = 0;
};