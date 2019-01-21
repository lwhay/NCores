#pragma once

#include <iostream>
#include "CJsonObject.h"

namespace parser {
    class EntityDescriptor {
        CJsonObject descriptor;

        std::string _des;

    private:
        std::string Transform(const std::string &parent);

    public:
        EntityDescriptor(const std::string &des);

        void Transform();

        std::string Export() const;
    };
}
