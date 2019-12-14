//
// Created by Vince Tu on 2019/5/7.
//

#ifndef UNTITLED_SCHEMA_H
#define UNTITLED_SCHEMA_H

typedef std::map<Name, NodePtr> SymbolMap;

static bool validate(const NodePtr &node, SymbolMap &symbolMap) {
    if (!node->isValid()) {
//        throw Exception(format("Schema is invalid, due to bad node of type %1%")
//                        % node->type());
    }

    if (node->hasName()) {
        const Name &nm = node->name();
        SymbolMap::iterator it = symbolMap.find(nm);
        bool found = it != symbolMap.end() && nm == it->first;

        if (node->type() == CORES_SYMBOLIC) {
            if (!found) {
                //throw Exception(std::format("Symbolic name \"%1%\" is unknown") %
                //node->name());
            }

            shared_ptr<NodeSymbolic> symNode =
                    static_pointer_cast<NodeSymbolic>(node);

            // if the symbolic link is already resolved, we return true,
            // otherwise returning false will force it to be resolved
            return symNode->isSet();
        }

        if (found) {
            return false;
        }
        symbolMap.insert(it, make_pair(nm, node));
    }

    node->lock();
    size_t leaves = node->leaves();
    for (size_t i = 0; i < leaves; ++i) {
        const NodePtr &leaf(node->leafAt(i));

        if (!validate(leaf, symbolMap)) {

            // if validate returns false it means a node with this name already
            // existed in the map, instead of keeping this node twice in the
            // map (which could potentially create circular shared pointer
            // links that could not be easily freed), replace this node with a
            // symbolic link to the original one.

            node->setLeafToSymbolic(i, symbolMap.find(leaf->name())->second);
        }
    }

    return true;
}

static void validate(const NodePtr &p) {
    SymbolMap m;
    validate(p, m);
}

class Schema;

class ValidSchema {
public:
    explicit ValidSchema(const NodePtr &root) : root_(root) {
        validate(root_);
    }

    ///explicit ValidSchema(const Schema &schema);
    ValidSchema();

    void setSchema(const Schema &schema);

    const NodePtr &root() const {
        return root_;
    }

    void toJson(std::ostream &os) const;

    void toFlatList(std::ostream &os) const;

protected:
    NodePtr root_;
};


#endif //UNTITLED_SCHEMA_H
