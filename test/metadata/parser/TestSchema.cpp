#include <locale>
#include <stack>
#include <string>
#include <sstream>
#include <cstring>
#include <map>
#include <vector>
#include <any>
#include <memory>
#include <iostream>
#include <set>
#include <fstream>
#include "JsonParser.h"
#include "Node.h"
#include "Generic.h"
#include "Schema.h"
#include "BlockCache.h"
#include "utils.h"
#include "BatchFileWriter.h"

#define DEBUG 1

class Node;

class GenericFixed;

class GenericRecord;

class Entity;

typedef std::vector<Entity> Array;
typedef std::map<std::string, Entity> Object;

typedef std::shared_ptr<Node> NodePtr;

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
//        throw Exception(std::format("Missing Json field \"%1%\": %2%") %
//                        fieldName % e.toString());
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

    Field(const string &n, const NodePtr &v, GenericDatum dv) :
            name(n), schema(v), defaultValue(dv) {}
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
};

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

static Field makeField(const Entity &e, SymbolTable &st, const string &ns) {
    const Object &m = e.objectValue();
    const string &n = getStringField(e, m, "name");
    Object::const_iterator it = findField(e, m, "type");
    map<string, Entity>::const_iterator it2 = m.find("default");
    NodePtr node = makeNode(it->second, st, ns);
    GenericDatum d = (it2 == m.end()) ? GenericDatum() :
                     makeGenericDatum(node, it2->second, st);
    return Field(n, node, d);
}

static NodePtr makeRecordNode(const Entity &e,
                              const Name &name, const Object &m, SymbolTable &st, const string &ns) {
    const Array &v = getArrayField(e, m, "fields");
    MultiAttribute<string> fieldNames;
    MultiAttribute<NodePtr> fieldValues;
    vector<GenericDatum> defaultValues;

    for (Array::const_iterator it = v.begin(); it != v.end(); ++it) {
        Field f = makeField(*it, st, ns);
        fieldNames.add(f.name);
        fieldValues.add(f.schema);
        defaultValues.push_back(f.defaultValue);
    }
    return NodePtr(new NodeRecord(asSingleAttribute(name),
                                  fieldValues, fieldNames, defaultValues));
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
};


static NodePtr makeArrayNode(const Entity &e, const Object &m, SymbolTable &st, const string &ns) {
    Object::const_iterator it = findField(e, m, "items");
    return NodePtr(new NodeArray(asSingleAttribute(makeNode(it->second, st, ns))));
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

void filesMerge(string file1, string file2, string path) {
    ifstream file_1;
    file_1.open(file1 + "/fileout.dat", ios_base::in | ios_base::binary);
    if (!file_1.is_open()) {
        std::cout << "failed to open " << file1 + "/fileout.dat" << '\n';
    }
    unique_ptr<HeadReader> headreader1(new HeadReader());
    headreader1->readHeader(file_1);
    file_1.close();

    ifstream file_2;
    file_2.open(file2 + "/fileout.dat", ios_base::in | ios_base::binary);
    if (!file_2.is_open()) {
        std::cout << "failed to open " << file2 + "/fileout.dat" << '\n';
    }
    unique_ptr<HeadReader> headreader2(new HeadReader());
    headreader2->readHeader(file_2);
    file_2.close();

    fstream fo;
    int column_num = headreader1->getColumnCount() + headreader2->getColumnCount();
    int blocksize = headreader1->getBlockSize();
    fo.open(path + "/fileout.dat", ios_base::out | ios_base::binary);
    if (!fo.is_open()) {
        std::cout << "failed to open " << path + "/fileout.dat" << '\n';
    }
    long *foffsets = new long[column_num]();
    int rowcount = headreader1->getRowCount();
    fo.write((char *) &blocksize, sizeof(blocksize));
    fo.write((char *) &rowcount, sizeof(rowcount));
    fo.write((char *) &column_num, sizeof(column_num));
    //fo << schema ;
    //metadata
    long *result = new long[column_num]();
    for (int j = 0; j < headreader1->getColumnCount(); ++j) {
        int blockCount = headreader1->getColumn(j).getblockCount();
        fo.write((char *) &blockCount, sizeof(int));
        foffsets[j] = fo.tellg();
        fo.write((char *) &foffsets[j], sizeof(long));
        for (int i = 0; i < headreader1->getColumn(j).getblockCount(); ++i) {
            int rowCount = headreader1->getColumn(j).getBlock(i).getRowcount();
            int offset = headreader1->getColumn(j).getBlock(i).getOffset();
            fo.write((char *) &(rowCount), sizeof(int));
            fo.write((char *) &(offset), sizeof(int));
        }
    }
    for (int j = 0; j < headreader2->getColumnCount(); ++j) {
        int blockCount = headreader2->getColumn(j).getblockCount();
        fo.write((char *) &blockCount, sizeof(int));
        foffsets[j + headreader1->getColumnCount()] = fo.tellg();
        fo.write((char *) &foffsets[j + headreader1->getColumnCount()], sizeof(long));
        for (int i = 0; i < headreader2->getColumn(j).getblockCount(); ++i) {
            int rowCount = headreader2->getColumn(j).getBlock(i).getRowcount();
            int offset = headreader2->getColumn(j).getBlock(i).getOffset();
            fo.write((char *) &(rowCount), sizeof(int));
            fo.write((char *) &(offset), sizeof(int));
        }
    }
    result[0] = fo.tellg();
    fo.close();
    FILE *fp = fopen((path + "/fileout.dat").data(), "ab+");
    char buffer[blocksize];
    long *tmplong = (long *) buffer;
    FILE *f1 = fopen((file1 + "/fileout.dat").data(), "rb");
    fseek(f1, headreader1->getColumn(0).getOffset(), SEEK_SET);
    while (!feof(f1)) {
        int tmp = ftell(fp);
        fread(buffer, sizeof(char), blocksize, f1);
        fwrite(buffer, sizeof(char), blocksize, fp);
    }
    fclose(f1);
    result[headreader1->getColumnCount()] = ftell(fp);
    FILE *f2 = fopen((file2 + "/fileout.dat").data(), "rb");
    fseek(f2, headreader2->getColumn(0).getOffset(), SEEK_SET);
    while (!feof(f2)) {
        fread(buffer, sizeof(char), blocksize, f2);
        fwrite(buffer, sizeof(char), blocksize, fp);
    }
    fclose(f2);
    fflush(fp);
    fclose(fp);

    for (int j = 1; j < headreader1->getColumnCount(); ++j) {
        result[j] = result[j - 1] + headreader1->getColumn(j).getOffset() - headreader1->getColumn(j - 1).getOffset();
    }
    for (int j = 1; j < headreader2->getColumnCount(); ++j) {
        result[j + headreader1->getColumnCount()] =
                result[j - 1 + headreader1->getColumnCount()] + headreader2->getColumn(j).getOffset() -
                headreader2->getColumn(j - 1).getOffset();
    }
    fp = fopen((path + "/fileout.dat").data(), "rb+");
    for (int m = 0; m < column_num; ++m) {
        fseek(fp, foffsets[m], SEEK_SET);
        fwrite((char *) &result[m], sizeof(result[m]), 1, fp);
    }
    fflush(fp);
    fclose(fp);
}

void OLWriter(char *lineitemPath = "../res/tpch/lineitem.tbl", char *ordersPath = "../res/tpch/orders.tbl",
              char *lineitemResult = "./lineitem", char *ordersResult = "./orders", char *result = ".",
              int blocksize = 1024) {
    fstream schema_f("../res/schema/nest.avsc", schema_f.binary | schema_f.in);
    ostringstream buf;
    char ch;
    while (buf && schema_f.get(ch))
        buf.put(ch);
    string s_schema = buf.str();
    char *schema = const_cast<char *>(s_schema.c_str());
    JsonParser test;
    test.init(schema);
    Entity e_test = readEntity(test);
    SymbolTable st;
    NodePtr n = makeNode(e_test, st, "");
    ValidSchema *vschema = new ValidSchema(n);
    GenericDatum c;
    c = GenericDatum(vschema->root());
    GenericRecord *r[2] = {NULL};
    GenericRecord *rt;
    BatchFileWriter order(c, ordersResult, blocksize);
    for (int i = 0; i < 1; i++) {
        r[i] = new GenericRecord(c.value<GenericRecord>());
    }
    GenericDatum t = r[0]->fieldAt(9);
    c = GenericDatum(r[0]->fieldAt(9).value<GenericArray>().schema()->leafAt(0));
    BatchFileWriter lineitmes(c, lineitemResult, blocksize);
    r[1] = new GenericRecord(c.value<GenericRecord>());
    fstream li(lineitemPath, ios::in);
    fstream od(ordersPath, ios::in);
    if (!li.is_open() || !od.is_open()) {
        std::cout << "failed to open " << lineitemPath << "or" << ordersPath << '\n';
    }
    string lline;
    string oline;
    getline(od, oline);
    order.readLine(oline);
    vector<GenericDatum> tmp_arr;
    for (; std::getline(li, lline);) {
//cout<<"lr"<<endl;
        lineitmes.readLine(lline);
        while (order.getInd(0) < lineitmes.getInd(0)) {
            order.setArr(9, tmp_arr);
            tmp_arr.clear();
//cout<<"ow"<<endl;
            order.writeRecord();
            getline(od, oline);
//cout<<"or"<<endl;
            order.readLine(oline);
        }
//            while(order.getInd(1)==lineitmes.getInd(1)) {
//                lineitmes.writeRecord();
//                tmp_arr.push_back(GenericDatum(lineitmes.getRecord()));
//            }
//cout<<"lw"<<endl;
        lineitmes.writeRecord();
//cout<<"lg"<<endl;
        GenericRecord tmpRe = lineitmes.getRecord();
//cout<<"pb"<<endl;
        tmp_arr.push_back(GenericDatum(&tmpRe));
    }
//cout<<"wr"<<endl;
    order.writeRest();
    lineitmes.writeRest();
//cout<<"mf"<<endl;
    order.mergeFiles();
    lineitmes.mergeFiles();

    filesMerge(ordersResult, lineitemResult, result);
}

void COfilesMerge(string file1, string file2, string file3, string path) {
    ifstream file_1;
    file_1.open(file1 + "/fileout.dat", ios_base::in | ios_base::binary);
    unique_ptr<HeadReader> headreader1(new HeadReader());
    headreader1->readHeader(file_1);
    file_1.close();

    ifstream file_2;
    file_2.open(file2 + "/fileout.dat", ios_base::in | ios_base::binary);
    unique_ptr<HeadReader> headreader2(new HeadReader());
    headreader2->readHeader(file_2);
    file_2.close();

    ifstream file_3;
    file_3.open(file3 + "/fileout.dat", ios_base::in | ios_base::binary);
    unique_ptr<HeadReader> headreader3(new HeadReader());
    headreader3->readHeader(file_3);
    file_3.close();

    fstream fo;
    int column_num = headreader1->getColumnCount() + headreader2->getColumnCount() + headreader3->getColumnCount();
    int blocksize = headreader1->getBlockSize();
    fo.open(path + "/fileout.dat", ios_base::out | ios_base::binary);
    long *foffsets = new long[column_num]();
    int rowcount = headreader1->getRowCount();
    fo.write((char *) &blocksize, sizeof(blocksize));
    fo.write((char *) &rowcount, sizeof(rowcount));
    fo.write((char *) &column_num, sizeof(column_num));
    //fo << schema ;
    //metadata
    long *result = new long[column_num]();
    for (int j = 0; j < headreader1->getColumnCount(); ++j) {
        int blockCount = headreader1->getColumn(j).getblockCount();
        fo.write((char *) &blockCount, sizeof(int));
        foffsets[j] = fo.tellg();
        fo.write((char *) &foffsets[j], sizeof(long));
        for (int i = 0; i < headreader1->getColumn(j).getblockCount(); ++i) {
            int rowCount = headreader1->getColumn(j).getBlock(i).getRowcount();
            int offset = headreader1->getColumn(j).getBlock(i).getOffset();
            fo.write((char *) &(rowCount), sizeof(int));
            fo.write((char *) &(offset), sizeof(int));
        }
    }
    for (int j = 0; j < headreader2->getColumnCount(); ++j) {
        int blockCount = headreader2->getColumn(j).getblockCount();
        fo.write((char *) &blockCount, sizeof(int));
        foffsets[j + headreader1->getColumnCount()] = fo.tellg();
        fo.write((char *) &foffsets[j + headreader1->getColumnCount()], sizeof(long));
        for (int i = 0; i < headreader2->getColumn(j).getblockCount(); ++i) {
            int rowCount = headreader2->getColumn(j).getBlock(i).getRowcount();
            int offset = headreader2->getColumn(j).getBlock(i).getOffset();
            fo.write((char *) &(rowCount), sizeof(int));
            fo.write((char *) &(offset), sizeof(int));
        }
    }
    for (int j = 0; j < headreader3->getColumnCount(); ++j) {
        int blockCount = headreader3->getColumn(j).getblockCount();
        fo.write((char *) &blockCount, sizeof(int));
        foffsets[j + headreader1->getColumnCount() + headreader2->getColumnCount()] = fo.tellg();
        fo.write((char *) &foffsets[j + headreader1->getColumnCount() + headreader2->getColumnCount()], sizeof(long));
        for (int i = 0; i < headreader3->getColumn(j).getblockCount(); ++i) {
            int rowCount = headreader3->getColumn(j).getBlock(i).getRowcount();
            int offset = headreader3->getColumn(j).getBlock(i).getOffset();
            fo.write((char *) &(rowCount), sizeof(int));
            fo.write((char *) &(offset), sizeof(int));
        }
    }
    result[0] = fo.tellg();
    fo.close();
    FILE *fp = fopen((path + "/fileout.dat").data(), "ab+");
    char buffer[blocksize];
    long *tmplong = (long *) buffer;
    FILE *f1 = fopen((file1 + "/fileout.dat").data(), "rb");
    fseek(f1, headreader1->getColumn(0).getOffset(), SEEK_SET);
    while (!feof(f1)) {
        int tmp = ftell(fp);
        fread(buffer, sizeof(char), blocksize, f1);
        fwrite(buffer, sizeof(char), blocksize, fp);
    }
    fclose(f1);
    result[headreader1->getColumnCount()] = ftell(fp);
    FILE *f2 = fopen((file2 + "/fileout.dat").data(), "rb");
    fseek(f2, headreader2->getColumn(0).getOffset(), SEEK_SET);
    while (!feof(f2)) {
        fread(buffer, sizeof(char), blocksize, f2);
        fwrite(buffer, sizeof(char), blocksize, fp);
    }
    fclose(f2);

    result[headreader1->getColumnCount() + headreader2->getColumnCount()] = ftell(fp);
    FILE *f3 = fopen((file3 + "/fileout.dat").data(), "rb");
    fseek(f3, headreader3->getColumn(0).getOffset(), SEEK_SET);
    while (!feof(f3)) {
        fread(buffer, sizeof(char), blocksize, f3);
        fwrite(buffer, sizeof(char), blocksize, fp);
    }
    fclose(f3);
    fflush(fp);
    fclose(fp);

    for (int j = 1; j < headreader1->getColumnCount(); ++j) {
        result[j] = result[j - 1] + headreader1->getColumn(j).getOffset() - headreader1->getColumn(j - 1).getOffset();
    }
    for (int j = 1; j < headreader2->getColumnCount(); ++j) {
        result[j + headreader1->getColumnCount()] =
                result[j - 1 + headreader1->getColumnCount()] + headreader2->getColumn(j).getOffset() -
                headreader2->getColumn(j - 1).getOffset();
    }
    for (int j = 1; j < headreader3->getColumnCount(); ++j) {
        result[j + headreader1->getColumnCount() + headreader2->getColumnCount()] =
                result[j - 1 + headreader1->getColumnCount() + headreader2->getColumnCount()] +
                headreader3->getColumn(j).getOffset() - headreader3->getColumn(j - 1).getOffset();
    }
    fp = fopen((path + "/fileout.dat").data(), "rb+");
    for (int m = 0; m < column_num; ++m) {
        fseek(fp, foffsets[m], SEEK_SET);
        fwrite((char *) &result[m], sizeof(result[m]), 1, fp);
    }
    fflush(fp);
    fclose(fp);
}

void COLWriter() {
    fstream schema_f("../res/schema/custom/nest.avsc", schema_f.binary | schema_f.in | schema_f.out);
    ostringstream buf;
    char ch;
    while (buf && schema_f.get(ch))
        buf.put(ch);
    string s_schema = buf.str();
    char *schema = const_cast<char *>(s_schema.c_str());
    JsonParser test;
    test.init(schema);
    Entity e_test = readEntity(test);
    SymbolTable st;
    NodePtr n = makeNode(e_test, st, "");
    ValidSchema *vschema = new ValidSchema(n);
    GenericDatum c;
    c = GenericDatum(vschema->root());
    GenericRecord *r[3] = {NULL};
    GenericRecord *rt;
    BatchFileWriter customer(c, "./customer", 1024);
    r[0] = new GenericRecord(c.value<GenericRecord>());
    GenericDatum t = r[0]->fieldAt(8);
    c = GenericDatum(r[0]->fieldAt(8).value<GenericArray>().schema()->leafAt(0));
    r[1] = new GenericRecord(c.value<GenericRecord>());
    BatchFileWriter orders(c, "./orders", 1024);
    fileReader fr(c, 1024);
    c = GenericDatum(r[1]->fieldAt(9).value<GenericArray>().schema()->leafAt(0));
    BatchFileWriter lineitem(c, "./lineitem", 1024);
    vector<GenericDatum> blank;
    vector<vector<GenericDatum>> tmpa;
    vector<int> r1;
    for (int i = 0; i < 10; i++) {
        r1.push_back(i);
    }
    vector<int> r2;
    for (int i = 10; i < 26; i++) {
        r2.push_back(i);
    }

    while (fr.next(r1, r2)) {
        GenericRecord tmp = fr.getRecord();
        int custkey = tmp.fieldAt(1).value<long>();
        if (custkey > tmpa.size()) {
            tmpa.resize(custkey, blank);
        }
        tmpa[custkey - 1].push_back(GenericDatum(tmp));
    }
    fstream ct("../res/tpch/customer.tbl", ios::in);
    string cline;
    for (; std::getline(ct, cline);) {
        customer.readLine(cline);
        {
            customer.setArr(8, tmpa[customer.getInd(0) - 1]);
            customer.writeRecord();
            for (auto iter:tmpa[customer.getInd(0) - 1]) {
                orders.setRecord(iter.value<GenericRecord>());
                orders.writeRecord();
                for (auto it:iter.value<GenericRecord>().fieldAt(9).value<GenericArray>().value()) {
                    int tmp = iter.value<GenericRecord>().fieldAt(9).value<GenericArray>().value().size();
                    tmp = it.value<GenericRecord>().fieldAt(1).value<long>();
                    lineitem.setRecord(it.value<GenericRecord>());
                    lineitem.writeRecord();
                }
            }
        }
    }
    customer.writeRest();
    customer.mergeFiles();
    orders.writeRest();
    orders.mergeFiles();
    lineitem.writeRest();
    lineitem.mergeFiles();

    COfilesMerge("./customer", "./orders", "./lineitem", "./result1");
}

void NestedReader(string datafile, string schemafile) {
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
    GenericDatum c;
    c = GenericDatum(vschema->root());
    GenericRecord *r[2] = {NULL, NULL};
    r[0] = new GenericRecord(c.value<GenericRecord>());
    GenericDatum t = r[0]->fieldAt(9);
    c = GenericDatum(r[0]->fieldAt(9).value<GenericArray>().schema()->leafAt(0));
    r[1] = new GenericRecord(c.value<GenericRecord>());
    ifstream file_in;
    file_in.open(datafile, ios_base::in | ios_base::binary);
    unique_ptr<HeadReader> headreader(new HeadReader());
    headreader->readHeader(file_in);
    file_in.close();
    FILE **fpp = new FILE *[26];
    int *rind = new int[26]();
    int *bind = new int[26]();
    int *rcounts = new int[26]();
    int blocksize = headreader->getBlockSize();
    Block *blockreaders[26];
    for (int i1 = 0; i1 < 26; ++i1) {
        fpp[i1] = fopen(datafile.data(), "rb");
        fseek(fpp[i1], headreader->getColumns()[i1].getOffset(), SEEK_SET);
        rcounts[i1] = headreader->getColumns()[i1].getBlock(bind[i1]).getRowcount();
        blockreaders[i1] = new Block(fpp[i1], 0L, 0, blocksize);
        blockreaders[i1]->loadFromFile();
    }
    vector<int> r1;
    for (int i = 0; i < 10; i++) {
        r1.push_back(i);
    }
    vector<int> r2;
    for (int i = 10; i < 26; i++) {
        r2.push_back(i);
    }
    vector<GenericDatum> &records = r[0]->fieldAt(9).value<GenericArray>().value();
    for (int k1 = 0; k1 < headreader->getRowCount(); ++k1) {
        for (int i :r1) {
            switch (r[0]->fieldAt(i).type()) {
                case AVRO_LONG: {
                    if (rind[i] == rcounts[i]) {
                        blockreaders[i]->loadFromFile();
                        rind[i] = 0;
                        rcounts[i] = headreader->getColumn(i).getBlock(bind[i]).getRowcount();
                        bind[i]++;
                    }
                    int64_t tmp = blockreaders[i]->next<long>();
                    r[0]->fieldAt(i) = tmp;
                    //cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case AVRO_INT: {
                    if (rind[i] == rcounts[i]) {
                        blockreaders[i]->loadFromFile();
                        rind[i] = 0;
                        rcounts[i] = headreader->getColumn(i).getBlock(bind[i]).getRowcount();
                        bind[i]++;
                    }
                    int tmp = blockreaders[i]->next<int>();
                    r[0]->fieldAt(i) = tmp;
                    //cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case AVRO_STRING: {
                    if (rind[i] == rcounts[i]) {
                        blockreaders[i]->loadFromFile();
                        rind[i] = 0;
                        rcounts[i] = headreader->getColumn(i).getBlock(bind[i]).getRowcount();
                        bind[i]++;
                    }
                    char *tmp = blockreaders[i]->next<char *>();
                    r[0]->fieldAt(i) = tmp;
                    //cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case AVRO_FLOAT: {
                    if (rind[i] == rcounts[i]) {
                        blockreaders[i]->loadFromFile();
                        rind[i] = 0;
                        rcounts[i] = headreader->getColumn(i).getBlock(bind[i]).getRowcount();
                        bind[i]++;
                    }
                    float tmp = blockreaders[i]->next<float>();
                    r[0]->fieldAt(i) = tmp;
                    //cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case AVRO_BYTES: {
                    if (rind[i] == rcounts[i]) {
                        blockreaders[i]->loadFromFile();
                        rind[i] = 0;
                        rcounts[i] = headreader->getColumn(i).getBlock(bind[i]).getRowcount();
                        bind[i]++;
                    }
                    char tmp = blockreaders[i]->next<char>();
                    r[0]->fieldAt(i) = tmp;
                    //cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case AVRO_ARRAY: {
                    if (rind[i] == rcounts[i]) {
                        blockreaders[i]->loadFromFile();
                        rind[i] = 0;
                        rcounts[i] = headreader->getColumn(i).getBlock(bind[i]).getRowcount();
                        bind[i]++;
                    }
                    int arrsize = blockreaders[i]->next<int>();
                    rind[i]++;
                    //cout << arrsize << endl;
                    //vector<GenericDatum> records;
                    for (int j = 0; j < arrsize; ++j) {
                        for (int k:r2) {
                            switch (r[1]->fieldAt(k - 10).type()) {
                                case AVRO_LONG: {
                                    if (rind[k] == rcounts[k]) {
                                        blockreaders[k]->loadFromFile();
                                        rind[k] = 0;
                                        rcounts[k] = headreader->getColumn(k).getBlock(bind[k]).getRowcount();
                                        bind[k]++;
                                    }
                                    int64_t tmp = blockreaders[k]->next<long>();
                                    r[1]->fieldAt(k - 10) = tmp;
                                    //cout << tmp << " ";
                                    rind[k]++;
                                    break;
                                }
                                case AVRO_INT: {
                                    if (rind[k] == rcounts[k]) {
                                        blockreaders[k]->loadFromFile();
                                        rind[k] = 0;
                                        rcounts[k] = headreader->getColumn(k).getBlock(bind[k]).getRowcount();
                                        bind[k]++;
                                    }
                                    int tmp = blockreaders[k]->next<int>();
                                    r[1]->fieldAt(k - 10) = tmp;
                                    //cout << tmp << " ";
                                    rind[k]++;
                                    break;
                                }
                                case AVRO_STRING: {
                                    if (rind[k] == rcounts[k]) {
                                        blockreaders[k]->loadFromFile();
                                        rind[k] = 0;
                                        rcounts[k] = headreader->getColumn(k).getBlock(bind[k]).getRowcount();
                                        bind[k]++;
                                    }
                                    char *tmp = blockreaders[k]->next<char *>();
                                    r[1]->fieldAt(k - 10) = tmp;
                                    //cout << tmp << " ";
                                    rind[k]++;
                                    break;
                                }
                                case AVRO_FLOAT: {
                                    if (rind[k] == rcounts[k]) {
                                        blockreaders[k]->loadFromFile();
                                        rind[k] = 0;
                                        rcounts[k] = headreader->getColumn(k).getBlock(bind[k]).getRowcount();
                                        bind[k]++;
                                    }
                                    float tmp = blockreaders[k]->next<float>();
                                    r[1]->fieldAt(k - 10) = tmp;
                                    //cout << tmp << " ";
                                    rind[k]++;
                                    break;
                                }
                                case AVRO_BYTES: {
                                    if (rind[k] == rcounts[k]) {
                                        blockreaders[k]->loadFromFile();
                                        rind[k] = 0;
                                        rcounts[k] = headreader->getColumn(k).getBlock(bind[k]).getRowcount();
                                        bind[k]++;
                                    }
                                    char tmp = blockreaders[k]->next<char>();
                                    r[1]->fieldAt(k - 10) = tmp;
                                    //       cout << tmp << " ";
                                    rind[k]++;
                                    break;
                                }
                            }
                        }
                        records.push_back(GenericDatum(r[1]));
                    }
                    records.clear();
                }
            }
        }
    }
    for (int i1 = 0; i1 < 26; ++i1) {
        fclose(fpp[i1]);
    }
}

void nextReader() {
    fstream schema_f("../res/schema/nest.avsc", schema_f.binary | schema_f.in | schema_f.out);
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
    GenericDatum c;
    c = GenericDatum(vschema->root());
    fileReader fr(c, 1024);
    fr.nextprint({0, 1, 9}, {10});
    GenericRecord tmp = fr.getRecord();
    cout << tmp.fieldAt(0).value<long>() << " ";
    cout << tmp.fieldAt(1).value<long>() << endl;
    vector<GenericDatum> tmparr = tmp.fieldAt(9).value<GenericArray>().value();
    for (auto inter:tmparr) {
        cout << inter.value<GenericRecord>().fieldAt(0).value<long>() << " ";
    }
    cout << endl;
    vector<GenericDatum> blank;
    vector<vector<GenericDatum>> tmpa;
    int custkey = tmp.fieldAt(1).value<long>();
    if (custkey > tmpa.size()) {
        tmpa.resize(custkey, blank);
    }
    tmpa[custkey - 1].push_back(GenericDatum(tmp));
    cout << tmpa[custkey - 1][0].value<GenericRecord>().fieldAt(0).value<long>() << endl;
}

void LReader(string datafile, string schemafile, vector<int> rv) {
    Tracer tracer;
    tracer.startTime();
    fstream schema_f(schemafile, schema_f.binary | schema_f.in | schema_f.out);
    ostringstream buf;
    char ch;
    while (buf && schema_f.get(ch))
        buf.put(ch);
    string s_schema = buf.str();
    char *schema = const_cast<char *>(s_schema.c_str());
    JsonParser *test = new JsonParser();
    cout << "sfile open: " << tracer.getRunTime() << endl;
    test->init(schema);
    cout << "sinit open: " << tracer.getRunTime() << endl;
    Entity e_test = readEntity(*test);
    SymbolTable st;
    NodePtr n = makeNode(e_test, st, "");
    ValidSchema *vschema = new ValidSchema(n);
    GenericDatum c;
    c = GenericDatum(vschema->root());
    cout << "record init: " << tracer.getRunTime() << endl;
    GenericRecord *r[2] = {NULL, NULL};
    r[0] = new GenericRecord(c.value<GenericRecord>());
    GenericDatum t = r[0]->fieldAt(9);
    c = GenericDatum(r[0]->fieldAt(9).value<GenericArray>().schema()->leafAt(0));
    r[1] = new GenericRecord(c.value<GenericRecord>());
    cout << "record set: " << tracer.getRunTime() << endl;
    ifstream file_in;
    file_in.open(datafile, ios_base::in | ios_base::binary);
    if (!file_in.is_open()) {
        cout << "cannot open" << datafile << endl;
    }
    unique_ptr<HeadReader> headreader(new HeadReader());
    headreader->readHeader(file_in);
    file_in.close();
    cout << "header read: " << tracer.getRunTime() << endl;
    FILE **fpp = new FILE *[16];

    for (int i1 = 0; i1 < 16; ++i1) {
        fpp[i1] = fopen(datafile.data(), "rb");
        fseek(fpp[i1], headreader->getColumn(i1 + 10).getOffset(), SEEK_SET);
    }
    int *rind = new int[16]();
    int *bind = new int[16]();
    int *rcounts = new int[16]();
    vector<int> **offarrs = new vector<int> *[16];
    for (int l1 = 0; l1 < 16; ++l1) {
        rcounts[l1] = headreader->getColumn(l1 + 10).getBlock(bind[l1]).getRowcount();
    }
    cout << "header set: " << tracer.getRunTime() << endl;
    int blocksize = headreader->getBlockSize();
    Block *blockreaders[16];
    for (int j = 0; j < 16; ++j) {
        blockreaders[j] = new Block(fpp[j], 0L, 0, blocksize);
        blockreaders[j]->loadFromFile();
    }
    long max = headreader->getColumn(10 + rv[0]).getblockCount();
    cout << "header rewind: " << tracer.getRunTime() << endl;

    int offsize;
    if (blocksize < 1 << 8) offsize = 1;
    else if (blocksize < 1 << 16) offsize = 2;
    else if (blocksize < 1 << 24) offsize = 3;
    else if (blocksize < 1 << 32) offsize = 4;
//    orderkey=blockreaders[i]->get<long>(rind[i]);
    int last = bind[rv[0]];
    for (; bind[rv[0]] < max;) {
        for (int i :rv) {
            switch (r[1]->fieldAt(i).type()) {
                case AVRO_LONG: {
                    if (rind[i] == rcounts[i]) {
                        blockreaders[i]->loadFromFile();
                        rind[i] = 0;
                        rcounts[i] = headreader->getColumn(i + 10).getBlock(bind[i]).getRowcount();
                        bind[i]++;
                    }
                    int64_t tmp = blockreaders[i]->next<long>();
                    r[1]->fieldAt(i) = tmp;
//                    cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case AVRO_INT: {
                    if (rind[i] == rcounts[i]) {
                        blockreaders[i]->loadFromFile();
                        rind[i] = 0;
                        rcounts[i] = headreader->getColumn(i + 10).getBlock(bind[i]).getRowcount();
                        bind[i]++;
                    }
                    int tmp = blockreaders[i]->next<int>();
                    r[1]->fieldAt(i) = tmp;
//                    cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case AVRO_STRING: {
                    if (rind[i] == rcounts[i]) {
                        blockreaders[i]->loadFromFile();
                        rind[i] = 0;
                        rcounts[i] = headreader->getColumn(i + 10).getBlock(bind[i]).getRowcount();
                        bind[i]++;
                    }
                    if (rind[i] == 0) {
                        offarrs[i] = blockreaders[i]->initString(offsize);
                    }
                    char *tmp = blockreaders[i]->getoffstring((*offarrs[i])[rind[i]]);
//                    char *tmp = blockreaders[i]->next<char *>();
                    r[1]->fieldAt(i) = tmp;
#ifdef DEBUG
                    cout << tmp << " ";
#endif
                    rind[i]++;
                    break;
                }
                case AVRO_FLOAT: {
                    if (rind[i] == rcounts[i]) {
                        blockreaders[i]->loadFromFile();
                        rind[i] = 0;
                        rcounts[i] = headreader->getColumn(i + 10).getBlock(bind[i]).getRowcount();
                        bind[i]++;
                    }
                    float tmp = blockreaders[i]->next<float>();
                    r[1]->fieldAt(i) = tmp;
//                    cout << tmp << " ";
                    rind[i]++;
                    break;
                }
                case AVRO_BYTES: {
                    if (rind[i] == rcounts[i]) {
                        blockreaders[i]->loadFromFile();
                        rind[i] = 0;
                        rcounts[i] = headreader->getColumn(i + 10).getBlock(bind[i]).getRowcount();
                        bind[i]++;
                    }
                    char tmp = blockreaders[i]->next<char>();
                    r[1]->fieldAt(i) = tmp;
//                    cout << tmp << " ";
                    rind[i]++;
                    break;
                }
            }
#ifdef DEBUG
            if (i == 15) {
                cout << endl;
            }
#endif
        }
#ifdef DEBUG
        if (bind[0] != last && bind[0] % 1000 == 0) {
            cout << "\t" << bind[0] << ":" << tracer.getRunTime() << endl;
            last = bind[0];
        }
#endif
    }
    for (int i1 = 0; i1 < 16; ++i1) {
        fclose(fpp[i1]);
    }
}

void fileTest() {
    fstream schema_f("../res/schema/nest.avsc", schema_f.binary | schema_f.in | schema_f.out);
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
    GenericDatum c;
    c = GenericDatum(vschema->root());
    GenericRecord *r[2] = {NULL, NULL};
    r[0] = new GenericRecord(c.value<GenericRecord>());
    GenericDatum t = r[0]->fieldAt(9);
    c = GenericDatum(r[0]->fieldAt(9).value<GenericArray>().schema()->leafAt(0));
    r[1] = new GenericRecord(c.value<GenericRecord>());
    ifstream file_in;
    file_in.open("./fileout.dat", ios_base::in | ios_base::binary);
    unique_ptr<HeadReader> headreader(new HeadReader());
    headreader->readHeader(file_in);
    file_in.close();
    FILE *fpr = fopen("./fileout.dat", "rb");
    FILE *_fpr = fopen("./fileout.dat", "rb");
    int count = 0;
    int i = 25;
    long off = headreader->getColumns()[i].getOffset();
    fseek(fpr, off, SEEK_SET);
    fseek(_fpr, headreader->getColumns()[0].getOffset(), SEEK_SET);
    char buffer[1024];
    any tmp = "sss";

    Block *stringBlock = new Block(fpr, 0L, 0, 1024);
    Block *keyBlock = new Block(_fpr, 0L, 0, 1024);
    long max = headreader->getColumn(i).getblockCount();
//    orderkey=blockreaders[i]->get<long>(rind[i]);
//        for (; bind[0] < max;) 
    for (int k = 0; k < max; k++) {
        stringBlock->loadFromFile();
//        keyBlock->loadFromFile();
        int rcount = headreader->getColumn(i).getBlock(k).getRowcount();
        for (int i = 0; i < rcount; i++) {
            tmp = stringBlock->next<char *>();
            //   long tmp = keyBlock->next<long>();
            count++;
//            cout << key << " " << tmp << endl;
        }
//        stringBlock->loadFromFile();
        // cout << endl;
    }
    delete stringBlock;
}

int main(int argc, char **argv) {
    if (argc == 3) {
        OLWriter(argv[1], argv[2]);
    } else {
//    testFILEWRITER();
//    testFileReader();
//    OLWriter();
        system("echo 3 > /proc/sys/vm/drop_caches");
        Tracer tracer;
        tracer.getRunTime();
        NestedReader("./fileout.dat", "../res/schema/nest.avsc");
//    nextReader();
        cout << "nest: " << tracer.getRunTime() << endl;
        system("echo 3 > /proc/sys/vm/drop_caches");
        tracer.startTime();
        vector<int> tmp;
//        for (int i = 0; i < 16; i++) {
//            tmp.push_back(i);
//        }

        tmp.push_back(15);
        LReader("./fileout.dat", "../res/schema/nest.avsc", tmp);
        cout << "flat: " << tracer.getRunTime() << endl;
//    fileTest();
//    filesMerge("./orders","./lineitem",".");
//    COLWriter();
    }
    return 0;
}
