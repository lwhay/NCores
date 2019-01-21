#include <EntityDescriptor.h>

namespace parser {

    EntityDescriptor::EntityDescriptor(const std::string &des) {
        _des = des;
        descriptor(des);
    }

    std::string EntityDescriptor::Transform(const std::string &parent) {
        std::string str;
        int i = descriptor.GetArraySize();
        bool pb = descriptor.Parse(parent);
        std::string desstring = descriptor.ToFormattedString();
        std::string name;
        descriptor.Get("name", name);
        CJsonObject type;
        descriptor.Get("type", type);
        std::string arraytype;
        type.Get("name", arraytype);
        std::cout << name << "\t\"" << arraytype << "\"\t" << descriptor.GetArraySize() << desstring << pb << std::endl;
        return str;
    }

    void EntityDescriptor::Transform() {
        Transform(_des);
    }

    std::string EntityDescriptor::Export() const {
        return descriptor.ToString();
    }
}
