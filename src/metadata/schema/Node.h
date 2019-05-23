//
// Created by Vince Tu on 2019/5/6.
//

#ifndef UNTITLED_NODE_H
#define UNTITLED_NODE_H

class GenericDatum;

using namespace std;

enum Type {

    AVRO_STRING,    /*!< String */
    AVRO_BYTES,     /*!< Sequence of variable length bytes data */
    AVRO_INT,       /*!< 32-bit integer */
    AVRO_LONG,      /*!< 64-bit integer */
    AVRO_FLOAT,     /*!< Floating point number */
    AVRO_DOUBLE,    /*!< Double precision floating point number */
    AVRO_BOOL,      /*!< Boolean value */
    AVRO_NULL,      /*!< Null */

    AVRO_RECORD,    /*!< Record, a sequence of fields */
    AVRO_ENUM,      /*!< Enumeration */
    AVRO_ARRAY,     /*!< Homogeneous array of some specific type */
    AVRO_MAP,       /*!< Homogeneous map from string to some specific type */
    AVRO_UNION,     /*!< Union of one or more types */
    AVRO_FIXED,     /*!< Fixed number of bytes */

    AVRO_NUM_TYPES, /*!< Marker */

    // The following is a pseudo-type used in implementation

    AVRO_SYMBOLIC = AVRO_NUM_TYPES, /*!< User internally to avoid circular references. */
    AVRO_UNKNOWN = -1 /*!< Used internally. */

};

class Name {
    std::string ns_;
    std::string simpleName_;
public:
    Name() {}

    Name(const std::string &name) { fullname(name); }

    Name(const std::string &simpleName, const std::string &ns) : ns_(ns), simpleName_(simpleName) { check(); }

    const std::string fullname() const { return (ns_.empty()) ? simpleName_ : ns_ + "." + simpleName_; }

    const std::string &ns() const { return ns_; }

    const std::string &simpleName() const { return simpleName_; }

    void ns(const std::string &n) { ns_ = n; }

    void simpleName(const std::string &n) { simpleName_ = n; }

    void fullname(const std::string &name) {
        std::string::size_type n = name.find_last_of('.');
        if (n == std::string::npos) {
            simpleName_ = name;
            ns_.clear();
        } else {
            ns_ = name.substr(0, n);
            simpleName_ = name.substr(n + 1);
        }
        check();
    }

    bool operator<(const Name &n) const {
        return (ns_ < n.ns_) ? true :
               (n.ns_ < ns_) ? false :
               (simpleName_ < n.simpleName_);
    }

    void check() const {
//        if (! ns_.empty() && (ns_[0] == '.' || ns_[ns_.size() - 1] == '.' || std::find_if(ns_.begin(), ns_.end(), invalidChar1) != ns_.end())) {
//            throw Exception("Invalid namespace: " + ns_);
//        }
//        if (simpleName_.empty() || std::find_if(simpleName_.begin(), simpleName_.end(), invalidChar2) != simpleName_.end()) {
//            throw Exception("Invalid name: " + simpleName_);
//        }
    }

    bool operator==(const Name &n) const { return ns_ == n.ns_ && simpleName_ == n.simpleName_; }

    bool operator!=(const Name &n) const { return !((*this) == n); }

    void clear() {
        ns_.clear();
        simpleName_.clear();
    }

    operator std::string() const {
        return fullname();
    }
};

class Node {
public:

    Node(Type type) :
            type_(type),
            locked_(false) {}

    ~Node() {}

    Type type() const {
        return type_;
    }

    void lock() {
        locked_ = true;
    }

    bool locked() const {
        return locked_;
    }

    virtual bool hasName() const = 0;

    void setName(const Name &name) {
        checkLock();
        checkName(name);
        doSetName(name);
    }

    virtual const Name &name() const = 0;

    void addLeaf(const NodePtr &newLeaf) {
        checkLock();
        doAddLeaf(newLeaf);
    }

    virtual size_t leaves() const = 0;

    virtual const NodePtr &leafAt(int index) const = 0;
//virtual const GenericDatum& defaultValueAt(int index) {
//    throw Exception(boost::format("No default value at: %1%") % index);
//}

    void addName(const std::string &name) {
        checkLock();
        checkName(name);
        doAddName(name);
    }

    virtual size_t names() const = 0;

    virtual const std::string &nameAt(int index) const = 0;

    virtual bool nameIndex(const std::string &name, size_t &index) const = 0;

    void setFixedSize(int size) {
        checkLock();
        doSetFixedSize(size);
    }

    virtual int fixedSize() const = 0;

    virtual bool isValid() const = 0;

//virtual SchemaResolution resolve(const Node &reader) const = 0;

    virtual void printJson(std::ostream &os, int depth) const = 0;

    virtual void printBasicInfo(std::ostream &os) const = 0;

    virtual void setLeafToSymbolic(int index, const NodePtr &node) = 0;

protected:

    void checkLock() const {
        if (locked()) {
            //throw Exception("Cannot modify locked schema");
        }
    }

    virtual void checkName(const Name &name) const {
        name.check();
    }

    virtual void doSetName(const Name &name) = 0;

    virtual void doAddLeaf(const NodePtr &newLeaf) = 0;

    virtual void doAddName(const std::string &name) = 0;

    virtual void doSetFixedSize(int size) = 0;

private:

    const Type type_;
    bool locked_;
};

typedef std::map <Name, NodePtr> SymbolTable;


template<typename Attribute>
struct NoAttribute {
    static const bool hasAttribute = false;

    size_t size() const {
        return 0;
    }

    void add(const Attribute &attr) {
        // There must be an add function for the generic NodeImpl, but the
        // Node APIs ensure that it is never called, the throw here is
        // just in case
        //throw Exception("This type does not have attribute");
    }

    const Attribute &get(size_t index = 0) const {
        // There must be an get function for the generic NodeImpl, but the
        // Node APIs ensure that it is never called, the throw here is
        // just in case
        //throw Exception("This type does not have attribute");
        // even though this code is unreachable the compiler requires it
        static const Attribute empty = Attribute();
        return empty;
    }

    Attribute &get(size_t index = 0) {
        // There must be an get function for the generic NodeImpl, but the
        // Node APIs ensure that it is never called, the throw here is
        // just in case
        //throw Exception("This type does not have attribute");
    }

};

template<typename Attribute>
struct SingleAttribute {
    static const bool hasAttribute = true;

    SingleAttribute() : attr_() {}

    SingleAttribute(const Attribute &a) : attr_(a) {}

    // copy constructing from another single attribute is allowed
    SingleAttribute(const SingleAttribute<Attribute> &rhs) :
            attr_(rhs.attr_) {}

    // copy constructing from a no attribute is allowed
    SingleAttribute(const NoAttribute<Attribute> &rhs) :
            attr_() {}

    size_t size() const {
        return 1;
    }

    void add(const Attribute &attr) {
        attr_ = attr;
    }

    const Attribute &get(size_t index = 0) const {
        if (index != 0) {
            //throw Exception("SingleAttribute has only 1 value");
        }
        return attr_;
    }

    Attribute &get(size_t index = 0) {
        if (index != 0) {
            //throw Exception("SingleAttribute has only 1 value");
        }
        return attr_;
    }

private:
    template<typename T> friend
    struct MultiAttribute;
    Attribute attr_;
};

template<typename Attribute>
struct MultiAttribute {
    static const bool hasAttribute = true;

    MultiAttribute() {}

    // copy constructing from another single attribute is allowed, it
    // pushes the attribute
    MultiAttribute(const SingleAttribute<Attribute> &rhs) {
        // since map is the only type that does this we know it's
        // final size will be two, so reserve
        attrs_.reserve(2);
        attrs_.push_back(rhs.attr_);
    }

    MultiAttribute(const MultiAttribute<Attribute> &rhs) :
            attrs_(rhs.attrs_) {}

    MultiAttribute(const NoAttribute<Attribute> &rhs) {}

    size_t size() const {
        return attrs_.size();
    }

    void add(const Attribute &attr) {
        attrs_.push_back(attr);
    }

    const Attribute &get(size_t index = 0) const {
        return attrs_.at(index);
    }

    Attribute &get(size_t index) {
        return attrs_.at(index);
    }

private:

    std::vector <Attribute> attrs_;
};

template<typename T>
SingleAttribute<T> asSingleAttribute(const T &t) {
    SingleAttribute<T> n;
    n.add(t);
    return n;
}


template<typename T>
struct NameIndexConcept {

    bool lookup(const std::string &name, size_t &index) const {
        //throw Exception("Name index does not exist");
        return 0;
    }

    bool add(const ::std::string &name, size_t index) {
        //throw Exception("Name index does not exist");
        return false;
    }
};

template<>
struct NameIndexConcept<MultiAttribute<std::string> > {
    typedef std::map <std::string, size_t> IndexMap;

    bool lookup(const std::string &name, size_t &index) const {
        IndexMap::const_iterator iter = map_.find(name);
        if (iter == map_.end()) {
            return false;
        }
        index = iter->second;
        return true;
    }

    bool add(const ::std::string &name, size_t index) {
        bool added = false;
        IndexMap::iterator lb = map_.lower_bound(name);
        if (lb == map_.end() || map_.key_comp()(name, lb->first)) {
            map_.insert(lb, IndexMap::value_type(name, index));
            added = true;
        }
        return added;
    }

private:

    IndexMap map_;
};

template
        <
                class NameConcept,
                class LeavesConcept,
                class LeafNamesConcept,
                class SizeConcept
        >
class NodeImpl : public Node {

protected:

    NodeImpl(Type type) :
            Node(type),
            nameAttribute_(),
            leafAttributes_(),
            leafNameAttributes_(),
            sizeAttribute_() {}

    NodeImpl(Type type,
             const NameConcept &name,
             const LeavesConcept &leaves,
             const LeafNamesConcept &leafNames,
             const SizeConcept &size) :
            Node(type),
            nameAttribute_(name),
            leafAttributes_(leaves),
            leafNameAttributes_(leafNames),
            sizeAttribute_(size) {}

    ~NodeImpl() {}

    void swap(NodeImpl &impl) {
        std::swap(nameAttribute_, impl.nameAttribute_);
        std::swap(leafAttributes_, impl.leafAttributes_);
        std::swap(leafNameAttributes_, impl.leafNameAttributes_);
        std::swap(sizeAttribute_, impl.sizeAttribute_);
        std::swap(nameIndex_, impl.nameIndex_);
    }

    bool hasName() const {
        return NameConcept::hasAttribute;
    }

    void doSetName(const Name &name) {
        nameAttribute_.add(name);
    }

    const Name &name() const {
        return nameAttribute_.get();
    }

    void doAddLeaf(const NodePtr &newLeaf) {
        leafAttributes_.add(newLeaf);
    }

    size_t leaves() const {
        return leafAttributes_.size();
    }

    const NodePtr &leafAt(int index) const {
        return leafAttributes_.get(index);
    }

    void doAddName(const std::string &name) {
        if (!nameIndex_.add(name, leafNameAttributes_.size())) {
            //throw Exception(boost::format("Cannot add duplicate name: %1%") % name);
        }
        leafNameAttributes_.add(name);
    }

    size_t names() const {
        return leafNameAttributes_.size();
    }

    const std::string &nameAt(int index) const {
        return leafNameAttributes_.get(index);
    }

    bool nameIndex(const std::string &name, size_t &index) const {
        return nameIndex_.lookup(name, index);
    }

    void doSetFixedSize(int size) {
        sizeAttribute_.add(size);
    }

    int fixedSize() const {
        return sizeAttribute_.get();
    }

    virtual bool isValid() const = 0;

    void printBasicInfo(std::ostream &os) const;

    void setLeafToSymbolic(int index, const NodePtr &node);

//    SchemaResolution furtherResolution(const Node &reader) const {
//        SchemaResolution match = RESOLVE_NO_MATCH;
//
//        if (reader.type() == AVRO_SYMBOLIC) {
//
//            // resolve the symbolic type, and check again
//            const NodePtr &node = reader.leafAt(0);
//            match = resolve(*node);
//        }
//        else if(reader.type() == AVRO_UNION) {
//
//            // in this case, need to see if there is an exact match for the
//            // writer's type, or if not, the first one that can be promoted to a
//            // match
//
//            for(size_t i= 0; i < reader.leaves(); ++i)  {
//
//                const NodePtr &node = reader.leafAt(i);
//                SchemaResolution thisMatch = resolve(*node);
//
//                // if matched then the search is done
//                if(thisMatch == RESOLVE_MATCH) {
//                    match = thisMatch;
//                    break;
//                }
//
//                // thisMatch is either no match, or promotable, this will set match to
//                // promotable if it hasn't been set already
//                if (match == RESOLVE_NO_MATCH) {
//                    match = thisMatch;
//                }
//            }
//        }
//
//        return match;
//    }

    NameConcept nameAttribute_;
    LeavesConcept leafAttributes_;
    LeafNamesConcept leafNameAttributes_;
    SizeConcept sizeAttribute_;
    NameIndexConcept<LeafNamesConcept> nameIndex_;
};

typedef NoAttribute<Name> NoName;
typedef SingleAttribute<Name> HasName;

typedef NoAttribute<NodePtr> NoLeaves;
typedef SingleAttribute<NodePtr> SingleLeaf;
typedef MultiAttribute<NodePtr> MultiLeaves;

typedef NoAttribute<std::string> NoLeafNames;
typedef MultiAttribute<std::string> LeafNames;

typedef NoAttribute<int> NoSize;
typedef SingleAttribute<int> HasSize;

typedef NodeImpl<NoName, NoLeaves, NoLeafNames, NoSize> NodeImplPrimitive;
typedef NodeImpl<HasName, NoLeaves, NoLeafNames, NoSize> NodeImplSymbolic;

typedef NodeImpl<HasName, MultiLeaves, LeafNames, NoSize> NodeImplRecord;
typedef NodeImpl<HasName, NoLeaves, LeafNames, NoSize> NodeImplEnum;
typedef NodeImpl<NoName, SingleLeaf, NoLeafNames, NoSize> NodeImplArray;
typedef NodeImpl<NoName, MultiLeaves, NoLeafNames, NoSize> NodeImplMap;
typedef NodeImpl<NoName, MultiLeaves, NoLeafNames, NoSize> NodeImplUnion;
typedef NodeImpl<HasName, NoLeaves, NoLeafNames, HasSize> NodeImplFixed;

class NodePrimitive : public NodeImplPrimitive {
public:

    ~NodePrimitive() {}

    explicit NodePrimitive(Type type) :
            NodeImplPrimitive(type) {}

//SchemaResolution resolve(const Node &reader)  const;

    void printJson(std::ostream &os, int depth) const {}

    bool isValid() const {
        return true;
    }
};

class NodeSymbolic : public NodeImplSymbolic {
    typedef std::weak_ptr <Node> NodeWeakPtr;

public:

    NodeSymbolic() :
            NodeImplSymbolic(AVRO_SYMBOLIC) {}

    explicit NodeSymbolic(const HasName &name) :
            NodeImplSymbolic(AVRO_SYMBOLIC, name, NoLeaves(), NoLeafNames(), NoSize()) {}

    NodeSymbolic(const HasName &name, const NodePtr n) :
            NodeImplSymbolic(AVRO_SYMBOLIC, name, NoLeaves(), NoLeafNames(), NoSize()), actualNode_(n) {}
//SchemaResolution resolve(const Node &reader)  const;

    void printJson(std::ostream &os, int depth) const {}

    bool isValid() const {
        return (nameAttribute_.size() == 1);
    }

    bool isSet() const {
        return (actualNode_.lock() != 0);
    }

    NodePtr getNode() const {
        NodePtr node = actualNode_.lock();
        if (!node) {
            //throw Exception(boost::format("Could not follow symbol %1%") % name());
        }
        return node;
    }

    void setNode(const NodePtr &node) {
        actualNode_ = node;
    }

protected:

    NodeWeakPtr actualNode_;

};

class NodeRecord : public NodeImplRecord {
    std::vector <GenericDatum> defaultValues;
public:
    NodeRecord() : NodeImplRecord(AVRO_RECORD) {}

    NodeRecord(const HasName &name, const MultiLeaves &fields,
               const LeafNames &fieldsNames,
               const std::vector <GenericDatum> &dv) :
            NodeImplRecord(AVRO_RECORD, name, fields, fieldsNames, NoSize()),
            defaultValues(dv) {
        for (size_t i = 0; i < leafNameAttributes_.size(); ++i) {
            if (!nameIndex_.add(leafNameAttributes_.get(i), i)) {
//            throw Exception(boost::format(
//                    "Cannot add duplicate name: %1%") %
//                            leafNameAttributes_.get(i));
            }
        }
    }

    void swap(NodeRecord &r) {
        NodeImplRecord::swap(r);
        defaultValues.swap(r.defaultValues);
    }

//SchemaResolution resolve(const Node &reader)  const;

    void printJson(std::ostream &os, int depth) const {}

    bool isValid() const {
        return ((nameAttribute_.size() == 1) &&
                (leafAttributes_.size() == leafNameAttributes_.size()));
    }

    const GenericDatum &defaultValueAt(int index) {
        return defaultValues[index];
    }
};

class NodeFixed : public NodeImplFixed {
public:

    NodeFixed() :
            NodeImplFixed(AVRO_FIXED) {}

    NodeFixed(const HasName &name, const HasSize &size) :
            NodeImplFixed(AVRO_FIXED, name, NoLeaves(), NoLeafNames(), size) {}

//SchemaResolution resolve(const Node &reader)  const;

    void printJson(std::ostream &os, int depth) const {};

    bool isValid() const {
        return (
                (nameAttribute_.size() == 1) &&
                (sizeAttribute_.size() == 1)
        );
    }
};

template<class A, class B, class C, class D>
inline void
NodeImpl<A, B, C, D>::printBasicInfo(std::ostream &os) const {
    os << type();
    if (hasName()) {
        //os << ' ' << nameAttribute_.get();
    }

    if (D::hasAttribute) {
        os << " " << sizeAttribute_.get();
    }
    os << '\n';
    int count = leaves();
    count = count ? count : names();
    for (int i = 0; i < count; ++i) {
        if (C::hasAttribute) {
            os << "name " << nameAt(i) << '\n';
        }
        if (type() != AVRO_SYMBOLIC && leafAttributes_.hasAttribute) {
            leafAt(i)->printBasicInfo(os);
        }
    }
//    if(isCompound(type())) {
//        os << "end " << type() << '\n';
//    }
}

template<class A, class B, class C, class D>
inline void
NodeImpl<A, B, C, D>::setLeafToSymbolic(int index, const NodePtr &node) {
    if (!B::hasAttribute) {
        //throw Exception("Cannot change leaf node for nonexistent leaf");
    }

    NodePtr &replaceNode = const_cast<NodePtr &>(leafAttributes_.get(index));
    if (replaceNode->name() != node->name()) {
        //throw Exception("Symbolic name does not match the name of the schema it references");
    }

    NodePtr symbol(new NodeSymbolic);
    NodeSymbolic *ptr = static_cast<NodeSymbolic *> (symbol.get());

    ptr->setName(node->name());
    ptr->setNode(node);
    replaceNode.swap(symbol);
}


#endif //UNTITLED_NODE_H
