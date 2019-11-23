//
// Created by Vince Tu on 2019/11/12.
//

#ifndef CORES_PARSER_H
#define CORES_PARSER_H

using namespace std;

class Node;

class GenericFixed;

class GenericRecord;

class Entity;

typedef std::vector<Entity> Array;
typedef std::map<std::string, Entity> Object;

typedef std::shared_ptr<Node> NodePtr;

enum FilterType {
    ftSelect,
    ftGt,
    ftLt,
    ftGe,
    ftLe,
    ftCj,
    ftDj,
    ftInvalid
};

struct fNode {
    string strdata;
    long ldata;
    double ddata;
    FilterType ft;
    fNode *L;
    fNode *R;

    fNode() {}

    fNode(const string &d, FilterType _ft = ftInvalid) : strdata(d), L(), R(), ft(_ft) {}

    fNode(const long &d, FilterType _ft = ftInvalid) : ldata(d), L(), R(), ft(_ft) {}

    fNode(const double &d, FilterType _ft = ftInvalid) : ddata(d), L(), R(), ft(_ft) {}

    fNode(const string &d, fNode *l, fNode *r, FilterType _ft) : strdata(d), L(l), R(r), ft(_ft) {}

    fNode(const long &d, fNode *l, fNode *r, FilterType _ft) : L(l), R(r), ldata(d), ft(_ft) {}

    fNode(const double &d, fNode *l, fNode *r, FilterType _ft) : L(l), R(r), ddata(d), ft(_ft) {}

    fNode(fNode *l, fNode *r, FilterType _ft) : L(l), R(r), ft(_ft) {}


    ~fNode() {}

    void setFt(FilterType _ft) {
        ft = _ft;
    }

    bool filter(int64_t value) {
        switch (ft) {
            case ftCj:
                return L->filter(value) & R->filter(value);
            case ftDj:
                return L->filter(value) | R->filter(value);
            case ftGt:
                return value > ldata;
            case ftGe:
                return value >= ldata;
            case ftLt:
                return value < ldata;
            case ftLe:
                return value <= ldata;
            default:
                cout << "don't support this filter" << endl;
                return false;
        }
    }

    bool filter(double value) {
        switch (ft) {
            case ftCj:
                return L->filter(value) & R->filter(value);
            case ftDj:
                return L->filter(value) | R->filter(value);
            case ftGt:
                return value > ddata;
            case ftGe:
                return value >= ddata;
            case ftLt:
                return value < ddata;
            case ftLe:
                return value <= ddata;
            default:
                cout << "don't support this filter" << endl;
                return false;
        }
    }
};

struct ColMeta {
    string pname;
    int select;
    fNode *fn;


    ColMeta(const string &n = "", int i = 0, fNode *_fn = NULL) :
            pname(const_cast<string &>(n)), select(i), fn(_fn) {}

    ColMeta(const ColMeta &cm) {
        this->pname = cm.pname;
        this->select = cm.select;
        this->fn = cm.fn;
    }

    ColMeta &operator=(ColMeta &other) {
        pname = other.pname;
        select = other.select;
        fn = other.fn;
        return *this;
    }

};

typedef std::map<string, ColMeta> FetchTable;

static NodePtr makePrimitive(const std::string &t) {
    if (t == "null") {
        return NodePtr(new NodePrimitive(AVRO_NULL));
    } else if (t == "boolean") {
        return NodePtr(new NodePrimitive(AVRO_BOOL));
    } else if (t == "int") {
        return NodePtr(new NodePrimitive(AVRO_INT));
    } else if (t == "long") {
        return NodePtr(new NodePrimitive(AVRO_LONG));
    } else if (t == "float") {
        return NodePtr(new NodePrimitive(AVRO_FLOAT));
    } else if (t == "double") {
        return NodePtr(new NodePrimitive(AVRO_DOUBLE));
    } else if (t == "string") {
        return NodePtr(new NodePrimitive(AVRO_STRING));
    } else if (t == "bytes") {
        return NodePtr(new NodePrimitive(AVRO_BYTES));
    } else {
        return NodePtr();
    }
}

static bool isFullName(const string &s) {
    return s.find('.') != string::npos;

    return s.find('.') == string::npos;

}

static Name getName(const string &name, const string &ns) {
    return (isFullName(name)) ? Name(name) : Name(name, ns);
}

static void makeFetch(const std::string &t, FetchTable &st, const string &ns) {
    NodePtr result = makePrimitive(t);
    if (result) {
    }

    FetchTable::const_iterator it = st.find(t);
    if (it != st.end()) {
        cout << "exception" << endl;
    }


    //throw Exception(boost::format("Unknown type: %1%") % n.fullname());
}

static NodePtr makeNode(const std::string &t, SymbolTable &st, const string &ns) {
    NodePtr result = makePrimitive(t);
    if (result) {
        return result;
    }
    Name n = getName(t, ns);

    SymbolTable::const_iterator it = st.find(n);
    if (it != st.end()) {

        return NodePtr(new NodeSymbolic(asSingleAttribute(n), it->second));
    }
    //throw Exception(boost::format("Unknown type: %1%") % n.fullname());
}

const Object::const_iterator findField(const Entity &e,
                                       const Object &m, const string &fieldName) {
    Object::const_iterator it = m.find(fieldName);
    if (it == m.end()) {
        cout << "exception" << endl;
//        return NULL;
    } else {
        return it;
    }
}

const string &getStringField(const Entity &e, const Object &m,
                             const string &fieldName) {
    Object::const_iterator it = findField(e, m, fieldName);
    //ensureType<string>(it->second, fieldName);
    return it->second.stringValue();
}

static Name getName(const Entity &e, const Object &m, const string &ns) {
    const string &name = getStringField(e, m, "name");

    if (isFullName(name)) {
        return Name(name);
    } else {
        Object::const_iterator it = m.find("namespace");
        if (it != m.end()) {
//            if (it->second.type() != type_traits<string>::type()) {
//                throw Exception(boost::format(
//                        "Json field \"%1%\" is not a %2%: %3%") %
//                                "namespace" % json::type_traits<string>::name() %
//                                it->second.toString());
//            }
            Name result = Name(name, it->second.stringValue());
            return result;
        }
        return Name(name, ns);
    }
}

const Array &getArrayField(const Entity &e, const Object &m,
                           const string &fieldName) {
    Object::const_iterator it = findField(e, m, fieldName);
    return it->second.arrayValue();
}

struct Field {
    const string &name;
    const NodePtr schema;
    const GenericDatum defaultValue;
    const bool required;

    Field(const string &n, const NodePtr &v, GenericDatum dv = GenericDatum(), bool r = true) :
            name(n), schema(v), defaultValue(dv), required(r) {}
};

class NodeUnion : public NodeImplUnion {
public:

    NodeUnion() :
            NodeImplUnion(AVRO_UNION) {}

    explicit NodeUnion(const MultiLeaves &types) :
            NodeImplUnion(AVRO_UNION, NoName(), types, NoLeafNames(), NoSize()) {}

//SchemaResolution resolve(const Node &reader)  const;

    void printJson(std::ostream &os, int depth) const {}

    bool isValid() const {
        std::set<std::string> seen;
        if (leafAttributes_.size() >= 1) {
            for (size_t i = 0; i < leafAttributes_.size(); ++i) {
                std::string name;
                const NodePtr &n = leafAttributes_.get(i);
                switch (n->type()) {
                    case AVRO_STRING:
                        name = "string";
                        break;
                    case AVRO_BYTES:
                        name = "bytes";
                        break;
                    case AVRO_INT:
                        name = "int";
                        break;
                    case AVRO_LONG:
                        name = "long";
                        break;
                    case AVRO_FLOAT:
                        name = "float";
                        break;
                    case AVRO_DOUBLE:
                        name = "double";
                        break;
                    case AVRO_BOOL:
                        name = "bool";
                        break;
                    case AVRO_NULL:
                        name = "null";
                        break;
                    case AVRO_ARRAY:
                        name = "array";
                        break;
                    case AVRO_MAP:
                        name = "map";
                        break;
                    case AVRO_RECORD:
                    case AVRO_ENUM:
                    case AVRO_UNION:
                    case AVRO_FIXED:
                    case AVRO_SYMBOLIC:
                        name = n->name().fullname();
                        break;
                    default:
                        return false;
                }
                if (seen.find(name) != seen.end()) {
                    return false;
                }
                seen.insert(name);
            }
            return true;
        }
        return false;
    }

    void setValid(int i) {}

    void invalidate() {}

    bool getValid(int i) const { return true; }
};


static void makeFetch(const Entity &e, FetchTable &st, const string &ns);

static void makeFetch(const Entity &e, const Array &m, FetchTable &st, const string &ns) {
    for (auto it = m.begin(); it != m.end(); ++it) {
        makeFetch(*it, st, ns);
    }
}

static NodePtr makeNode(const Entity &e, SymbolTable &st, const string &ns);

static NodePtr makeNode(const Entity &e, const Array &m, SymbolTable &st, const string &ns) {
    MultiAttribute<NodePtr> mm;
    for (auto it = m.begin(); it != m.end(); ++it) {
        mm.add(makeNode(*it, st, ns));
    }
    return NodePtr(new NodeUnion(mm));
}

static void assertType(const Entity &e, EntityType et) {
    if (e.type() != et) {
//        throw Exception(boost::format("Unexpected type for default value: "
//                                      "Expected %1%, but found %2%") % et % e.type());
    }
}

static vector<uint8_t> toBin(const std::string &s) {
    vector<uint8_t> result;
    result.resize(s.size());
    std::copy(s.c_str(), s.c_str() + s.size(), &result[0]);
    return result;
}

static GenericDatum makeGenericDatum(NodePtr n, const Entity &e, const SymbolTable &st) {
    Type t = n->type();
    if (t == AVRO_SYMBOLIC) {
        n = st.find(n->name())->second;
        t = n->type();
    }
    switch (t) {
        case AVRO_STRING:
            assertType(e, etString);
            return GenericDatum(e.stringValue());
        case AVRO_BYTES:
            assertType(e, etString);
            return GenericDatum(toBin(e.stringValue()));
        case AVRO_INT:
            assertType(e, etLong);
            return GenericDatum(static_cast<int32_t>(e.longValue()));
        case AVRO_LONG:
            assertType(e, etLong);
            return GenericDatum(e.longValue());
        case AVRO_FLOAT:
            assertType(e, etDouble);
            return GenericDatum(static_cast<float>(e.doubleValue()));
        case AVRO_DOUBLE:
            assertType(e, etDouble);
            return GenericDatum(e.doubleValue());
        case AVRO_BOOL:
            assertType(e, etBool);
            return GenericDatum(e.boolValue());
        case AVRO_NULL:
            assertType(e, etNull);
            return GenericDatum();
        case AVRO_RECORD: {
            assertType(e, etObject);
            GenericRecord result(n);
            const map<string, Entity> &v = e.objectValue();
            for (size_t i = 0; i < n->leaves(); ++i) {
                map<string, Entity>::const_iterator it = v.find(n->nameAt(i));
                if (it == v.end()) {
//                    throw Exception(boost::format(
//                            "No value found in default for %1%") % n->nameAt(i));
                }
                result.setFieldAt(i,
                                  makeGenericDatum(n->leafAt(i), it->second, st));
            }
            return GenericDatum(n, result);
        }
            /*case AVRO_ENUM:
                assertType(e, etString);
                return GenericDatum(n, GenericEnum(n, e.stringValue()));
            case AVRO_ARRAY:
            {
                assertType(e, json::etArray);
                GenericArray result(n);
                const vector<Entity>& elements = e.arrayValue();
                for (vector<Entity>::const_iterator it = elements.begin();
                     it != elements.end(); ++it) {
                    result.value().push_back(makeGenericDatum(n->leafAt(0), *it, st));
                }
                return GenericDatum(n, result);
            }
            case AVRO_MAP:
            {
                assertType(e, json::etObject);
                GenericMap result(n);
                const map<string, Entity>& v = e.objectValue();
                for (map<string, Entity>::const_iterator it = v.begin();
                     it != v.end(); ++it) {
                    result.value().push_back(make_pair(it->first,
                                                       makeGenericDatum(n->leafAt(1), it->second, st)));
                }
                return GenericDatum(n, result);
            }
            case AVRO_UNION:
            {
                GenericUnion result(n);
                string name;
                Entity e2;
                if (e.type() == etNull) {
                    name = "null";
                    e2 = e;
                } else {
                    assertType(e, etObject);
                    const map<string, Entity>& v = e.objectValue();
                    if (v.size() != 1) {
    //                    throw Exception(boost::format("Default value for "
    //                                                  "union has more than one field: %1%") % e.toString());
                    }
                    map<string, Entity>::const_iterator it = v.begin();
                    name = it->first;
                    e2 = it->second;
                }
                for (size_t i = 0; i < n->leaves(); ++i) {
                    const NodePtr& b = n->leafAt(i);
                    if (nameof(b) == name) {
                        result.selectBranch(i);
                        result.datum() = makeGenericDatum(b, e2, st);
                        return GenericDatum(n, result);
                    }
                }
    //            throw Exception(boost::format("Invalid default value %1%") %
    //                            e.toString());
            }*/
        case AVRO_FIXED:
            assertType(e, etString);
            return GenericDatum(n, GenericFixed(n, toBin(e.stringValue())));
//        default:
//            throw Exception(boost::format("Unknown type: %1%") % t);
    }
    return GenericDatum();
}

static fNode *getBody(const Object &m) {
    Object::const_iterator it = m.find("composite");
    if (it != m.end()) {
        fNode *fn;
        const string &com = it->second.stringValue();
        if (com.compare("conjunctive") == 0) {
            it = m.find("condition");
            Array arr = it->second.arrayValue();
            fn = new fNode(getBody(arr[0].objectValue()), getBody(arr[1].objectValue()), ftCj);
        } else if (com.compare("disjuncitve") == 0) {
            it = m.find("condition");
            Array arr = it->second.arrayValue();
            fn = new fNode(getBody(arr[0].objectValue()), getBody(arr[1].objectValue()), ftDj);
        }
        return fn;
//        else if(com.compare("negative")==0){
//            it = m.find("condition");
//            Array arr=it->second.arrayValue();
//            fn=new fNode(getBody(arr[0].objectValue()),ft);
//        }
    } else if ((it = m.find("operator")) != m.end()) {
        const string &ope = it->second.stringValue();
        it = m.find("operand");
        const Object &opr = it->second.objectValue();
        Object::const_iterator ito = opr.find("type");
        fNode *fn;
        if (ito->second.stringValue().compare("double")) {
            ito = opr.find("value");
            double tmp = ito->second.doubleValue();
            fn = new fNode(tmp);
        } else if (ito->second.stringValue().compare("float")) {
            ito = opr.find("value");
            double tmp = ito->second.doubleValue();
            fn = new fNode(tmp);
        } else if (ito->second.stringValue().compare("long")) {
            ito = opr.find("value");
            long tmp = ito->second.longValue();
            fn = new fNode(tmp);
        } else if (ito->second.stringValue().compare("int")) {
            ito = opr.find("value");
            long tmp = ito->second.longValue();
            fn = new fNode(tmp);
        } else if (ito->second.stringValue().compare("string")) {
            ito = opr.find("value");
            string tmp = ito->second.stringValue();
            fn = new fNode(tmp);
        }
        if (ope.compare("lt") == 0) {
            fn->setFt(ftLt);
        } else if (ope.compare("le") == 0) {
            fn->setFt(ftLe);
        } else if (ope.compare("gt") == 0) {
            fn->setFt(ftGt);
        } else if (ope.compare("ge") == 0) {
            fn->setFt(ftGe);
        }
        return fn;
    }
}

static void selectField(const Entity &e, FetchTable &st, const string &ns) {
    const Object &m = e.objectValue();
    const string &n = getStringField(e, m, "name");
    Object::const_iterator it = m.find("type");
    if (it != m.end()) {
        makeFetch(it->second, st, ns);
    }
    it = m.find("clause");
    if (it != m.end()) {
        if (it->second.stringValue().compare("fetch") == 0) {
            ColMeta tmp(ns, 1);
            st[n] = tmp;
        } else if (it->second.stringValue().compare("filter") == 0) {
            it = m.find("body");
            const Object &b = it->second.objectValue();
            fNode *fn = getBody(b);
            ColMeta cm(ns, 0, fn);
            st[n] = cm;
        }
    }
}

static Field makeField(const Entity &e, SymbolTable &st, const string &ns) {
    const Object &m = e.objectValue();
    const string &n = getStringField(e, m, "name");
    Object::const_iterator it = findField(e, m, "type");
    map<string, Entity>::const_iterator it2 = m.find("default");
    NodePtr node = makeNode(it->second, st, ns);
    GenericDatum d = (it2 == m.end()) ? GenericDatum() :
                     makeGenericDatum(node, it2->second, st);
    it = m.find("option");
    bool r = true;
    if (it != m.end() && it->second.stringValue().compare("optional") == 0)
        r = false;
    return Field(n, node, d, r);
}

static void selectRecordNode(const Entity &e,
                             const string &name, const Object &m, FetchTable &st) {
    const Array &v = getArrayField(e, m, "fields");
    for (Array::const_iterator it = v.begin(); it != v.end(); ++it) {
        selectField(*it, st, name);
    }
}

static NodePtr makeRecordNode(const Entity &e,
                              const Name &name, const Object &m, SymbolTable &st, const string &ns) {
    const Array &v = getArrayField(e, m, "fields");
    MultiAttribute<string> fieldNames;
    MultiAttribute<NodePtr> fieldValues;
    vector<GenericDatum> defaultValues;
    vector<bool> requiredValues;

    for (Array::const_iterator it = v.begin(); it != v.end(); ++it) {
        Field f = makeField(*it, st, ns);
        fieldNames.add(f.name);
        fieldValues.add(f.schema);
        defaultValues.push_back(f.defaultValue);
        requiredValues.push_back(f.required);
    }
    return NodePtr(new NodeRecord(asSingleAttribute(name),
                                  fieldValues, fieldNames, defaultValues, requiredValues));
}

const int64_t getLongField(const Entity &e, const Object &m,
                           const string &fieldName) {
    Object::const_iterator it = findField(e, m, fieldName);
    //ensureType<int64_t>(it->second, fieldName);
    return it->second.longValue();
}

static NodePtr makeFixedNode(const Entity &e,
                             const Name &name, const Object &m) {
    int v = static_cast<int>(getLongField(e, m, "size"));
    if (v <= 0) {
//        throw Exception(boost::format("Size for fixed is not positive: ") %
//                        e.toString());
    }
    return NodePtr(new NodeFixed(asSingleAttribute(name),
                                 asSingleAttribute(v)));
}


class NodeArray : public NodeImplArray {
public:

    NodeArray() :
            NodeImplArray(AVRO_ARRAY) {}

    explicit NodeArray(const SingleLeaf &items) :
            NodeImplArray(AVRO_ARRAY, NoName(), items, NoLeafNames(), NoSize()) {}

//SchemaResolution resolve(const Node &reader)  const;

    void printJson(std::ostream &os, int depth) const {}

    bool isValid() const {
        return (leafAttributes_.size() == 1);
    }

    void setValid(int i) {}

    void invalidate() {}

    bool getValid(int i) const { return true; }
};


static void selectArrayNode(const Entity &e, const Object &m, FetchTable &st, const string &ns) {
    Object::const_iterator it = findField(e, m, "items");
    makeFetch(it->second, st, ns);
}

static NodePtr makeArrayNode(const Entity &e, const Object &m, SymbolTable &st, const string &ns) {
    Object::const_iterator it = findField(e, m, "items");
    return NodePtr(new NodeArray(asSingleAttribute(makeNode(it->second, st, ns))));
}


static void makeFetch(const Entity &e, const Object &m,
                      FetchTable &st, const string &ns) {
    const string &type = getStringField(e, m, "type");
    if (type == "record") {
        const string &name = getStringField(e, m, "name");
        ColMeta tmp = ColMeta("");
        if (ns.size() != 0) {
            ColMeta tmpc = ColMeta(ns);
            tmp = tmpc;
        }
        st[name] = tmp;
        selectRecordNode(e, name, m, st);
    } else if (type == "array") {
        selectArrayNode(e, m, st, ns);
    } else {
        cout << "makefetch wrong type" + type << endl;
    }
    /*else if (type == "map") {
        return makeMapNode(e, m, st, ns);
    }*/
    //throw Exception(boost::format("Unknown type definition: %1%")
    //% e.toString());
}


static NodePtr makeNode(const Entity &e, const Object &m,
                        SymbolTable &st, const string &ns) {
    const string &type = getStringField(e, m, "type");
    if (NodePtr result = makePrimitive(type)) {
        return result;
    } else if (type == "record" || type == "error" ||
               type == "enum" || type == "fixed") {
        Name nm = getName(e, m, ns);
        NodePtr result;
        if (type == "record" || type == "error") {
            result = NodePtr(new NodeRecord());
            st[nm] = result;
            NodePtr r = makeRecordNode(e, nm, m, st, nm.ns());
            (std::dynamic_pointer_cast<NodeRecord>(r))->swap(
                    *std::dynamic_pointer_cast<NodeRecord>(result));
        } else {
            result = makeFixedNode(e, nm, m);
            st[nm] = result;
        }
        return result;
    } else if (type == "array") {
        return makeArrayNode(e, m, st, ns);
    } /*else if (type == "map") {
        return makeMapNode(e, m, st, ns);
    }*/
    //throw Exception(boost::format("Unknown type definition: %1%")
    //% e.toString());
}


static void makeFetch(const Entity &e, FetchTable &ft, const string &ns) {
    switch (e.type()) {
        case etString:;
        case etObject:
            makeFetch(e, e.objectValue(), ft, ns);
            break;
        case etArray:
            makeFetch(e, e.arrayValue(), ft, ns);
            break;
            //default:
            //throw Exception(boost::format("Invalid Avro type: %1%") % e.toString());
    }
}

static NodePtr makeNode(const Entity &e, SymbolTable &st, const string &ns) {
    switch (e.type()) {
        case etString:
            return makeNode(e.stringValue(), st, ns);
        case etObject:
            return makeNode(e, e.objectValue(), st, ns);
        case etArray:
            return makeNode(e, e.arrayValue(), st, ns);
            //default:
            //throw Exception(boost::format("Invalid Avro type: %1%") % e.toString());
    }
}


class SchemaReader {
private:
    string schemafile;
    bool filter;

public:
    SchemaReader(string sf, bool fl = true) : schemafile(sf), filter(fl) {
    }

    ValidSchema *read() {
        if (filter) {
            fstream schema_f(schemafile, schema_f.binary | schema_f.in);
            if (!schema_f.is_open()) {
                cout << schemafile << "cannot open" << endl;
            }
            ostringstream buf;
            char ch;
            while (buf && schema_f.get(ch))
                buf.put(ch);
            string s_schema = buf.str();
            char *schema = const_cast<char *>(s_schema.c_str());
            JsonParser *test = new JsonParser();
            test->init(schema);
            Entity e_test = readEntity(*test);
            SymbolTable st;
            NodePtr n = makeNode(e_test, st, "");
            ValidSchema *vschema = new ValidSchema(n);
            return vschema;
        } else {
            fstream fetch_f("../res/schema/custom/query.qsc", fetch_f.binary | fetch_f.in);
            if (!fetch_f.is_open()) {
                cout << "../res/schema/custom/query.qsc" << "cannot open" << endl;
            }
            ostringstream buf_f;
            char ch;
            while (buf_f && fetch_f.get(ch))
                buf_f.put(ch);
            string s_fetch = buf_f.str();
            char *fetch = const_cast<char *>(s_fetch.c_str());
            JsonParser *f_test = new JsonParser();
            f_test->init(fetch);
            Entity e_fetch = readEntity(*f_test);
            FetchTable ft;
            makeFetch(e_fetch, ft, "");
            return NULL;
        }
    }
};


#endif //CORES_PARSER_H
