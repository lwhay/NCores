//
// Created by Michael on 2019-05-23.
//

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
    } /*else if (type == "array") {
        return makeArrayNode(e, m, st, ns);
    } else if (type == "map") {
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

void testrecord() {
    any test;
}

void SplitString(const std::string &s, std::vector<std::string> &v, const std::string &c) {
    std::string::size_type pos1, pos2;
    pos2 = s.find(c);
    pos1 = 0;
    while (std::string::npos != pos2) {
        v.push_back(s.substr(pos1, pos2 - pos1));
        pos1 = pos2 + c.size();
        pos2 = s.find(c, pos1);
    }
    if (pos1 != s.length())
        v.push_back(s.substr(pos1));
}

void ReadRecord(GenericRecord &record, const std::string &s) {
    vector<string> v;
    SplitString(s, v, "|");
    for (vector<string>::size_type i = 0; i != v.size(); ++i) {
        cout << v[i] << " ";
        switch (record.fieldAt(i).type()) {
            case AVRO_INT: {
                int tmp = stoi(v[i]);
                record.fieldAt(i) = tmp;
                break;
            }
            case AVRO_DOUBLE: {
                double tmp = stod(v[i]);
                record.fieldAt(i) = tmp;
                break;
            }
            case AVRO_STRING: {
                record.fieldAt(i) = v[i];
                break;
            }
        }
    }
}


class BlockReader {
    int rowCount;
    int lengthOffset;
    int size;

public:
    BlockReader() : lengthOffset(0) {}

    BlockReader(istream is) {
        is.read((char *) &rowCount, sizeof(rowCount));
        is.read((char *) &size, sizeof(size));
    }

    void writeTo(ostream os) {
        os.write((char *) &rowCount, sizeof(rowCount));
        os.write((char *) size, sizeof(size));
    }

    int getRowcount() {
        return rowCount;
    }

    void read(istream &is) {
        is.read((char *) &this->rowCount, sizeof(this->rowCount));
        is.read((char *) &this->size, sizeof(this->size));
    }
};

class ColumnReader {
    long offset;
    string metaData;
    int blockCount;
    vector<BlockReader> blocks;

public:
    ColumnReader() {}

    ColumnReader(const ColumnReader &columnreader) {
        offset = columnreader.offset;
        metaData = columnreader.metaData;
        blockCount = columnreader.blockCount;
        blocks = columnreader.blocks;
    }

    ColumnReader(int count, long _offset) : blockCount(count), offset(_offset) {
        //metaData=meta;
    }

    long getOffset() {
        return offset;
    }

    void setBlocks(vector<BlockReader> _blocks) {
        this->blocks.assign(_blocks.begin(), _blocks.begin() + _blocks.size());
    }

    vector<BlockReader> getBlocks() {
        return blocks;
    }

    int getblockCount() {
        return blockCount;
    }

    void pushBlock(BlockReader block) {
        blocks.push_back(block);
    }
};

class HeadReader {
    int rowCount;
    int blockSize;
    int columnCount;
    string metaData;
    vector<ColumnReader> columns;

public:
    HeadReader() {}

    int getRowCount() {
        return rowCount;
    }

    int getColumnCount() {
        return columnCount;
    }

    vector<ColumnReader> getColumns() {
        return columns;
    }

    string getMetaData() {
        return metaData;
    }

    void setColumns(vector<ColumnReader> _columns) {
        this->columns.assign(_columns.begin(), _columns.begin() + _columns.size());
    }

    void readHeader(istream &is) {
        is.read((char *) &this->blockSize, sizeof(this->blockSize));
        is.read((char *) &this->rowCount, sizeof(this->rowCount));
        is.read((char *) &this->columnCount, sizeof(this->columnCount));
        getline(is, metaData);
        unique_ptr<vector<ColumnReader>> columns(new vector<ColumnReader>());
        for (int i = 0; i < columnCount; ++i) {
            int blockCount;
            is.read((char *) &blockCount, sizeof(blockCount));
            long offset;
            is.read((char *) &offset, sizeof(offset));
            unique_ptr<vector<BlockReader>> blocks(new vector<BlockReader>());
            for (int j = 0; j < blockCount; ++j) {
                unique_ptr<BlockReader> block(new BlockReader);
                block->read(is);
                blocks->push_back(*block);
            }
            int tmp = blocks->size();
            unique_ptr<ColumnReader> column(new ColumnReader(blockCount, offset));
            column->setBlocks(*blocks);
            columns->push_back(*column);
        }
        this->setColumns(*columns);
    }
};


void TestFile() {
    FILE **files = (FILE **) (malloc(2 * sizeof(FILE *)));
    files[0] = fopen("./test.dat", "wb+");
    char *buffer = new char[1024];
    *(int *) &buffer[3] = 1024;
    fwrite(buffer, sizeof(char), 1024, files[0]);
    fclose(files[0]);
}

int main() {
    char *schema = "{\"type\": \"record\",\"name\": \"Test\",\"fields\": [{\"name\": \"f\",\"type\": \"int\"},{\"name\": \"f2\", \"type\": \"double\"},{\"name\": \"f3\", \"type\": \"string\"}]}";
    JsonParser *test = new JsonParser();
    test->init(schema);
    Entity e_test = readEntity(*test);
    SymbolTable st;
    NodePtr n = makeNode(e_test, st, "");
    ValidSchema *vschema = new ValidSchema(n);
    GenericDatum c;
    c = GenericDatum(vschema->root());
    GenericRecord *r[1] = {NULL};
    for (int i = 0; i < 1; i++) {
        r[i] = new GenericRecord(c.value<GenericRecord>());
    }
    string testrecord = "1|1.28|test|";
    ReadRecord(*r[0], testrecord);
    char *filename = (char *) malloc(strlen("./file") + 6);
    char *headname = (char *) malloc(strlen("./file") + 10);
    TestFile();
    int culnum = r[0]->fieldCount();
    FILE **files = (FILE **) (malloc(culnum * sizeof(FILE *)));
    FILE **heads = (FILE **) (malloc(culnum * sizeof(FILE *)));
    char **buffers = (char **) (malloc(culnum * sizeof(char *)));
    int **headsinfo = (int **) (malloc(culnum * sizeof(int *)));
    int *index = new int[culnum];
    int *hindex = new int[culnum];
    int *offsets = new int[culnum];
    int *blockcounts = new int[culnum];

    for (int k = 0; k < culnum; ++k) {
        sprintf(filename, "%s%d%s", "./file", k, ".dat");
        sprintf(headname, "%s%d%s", "./file", k, ".head");
        heads[k] = fopen(headname, "wb+");
        files[k] = fopen(filename, "wb+");
        buffers[k] = new char[1024];
        headsinfo[k] = new int[1024];
    }
    int strl = 0;
    for (int k = 0; k < culnum; ++k) {
        switch (r[0]->fieldAt(k).type()) {
            case AVRO_INT: {
                int tmp = r[0]->fieldAt(k).value<int>();
                cout << index[k] << endl;
                *(int *) (&buffers[k][index[k]]) = tmp;
                index[k] += sizeof(int);
                if (index[k] >= 1024) {
                    fwrite(buffers[k], sizeof(char), 1024, files[k]);
                    blockcounts[k]++;
                    memset(buffers[k], 0, 1024 * sizeof(char));
                    headsinfo[k][hindex[k]++] = 1024 / sizeof(int);
                    offsets[k] += 1024 / sizeof(int);
                    headsinfo[k][hindex[k]++] = offsets[k];
                    if (hindex[k] >= 1024) {
                        fwrite(headsinfo[k], sizeof(int), 1024, heads[k]);
                        memset(headsinfo[k], 0, 1024 * sizeof(int));
                        hindex[k];
                    }
                }
                break;
            }
            case AVRO_DOUBLE: {
                double tmp = r[0]->fieldAt(k).value<double>();
                *(double *) (&buffers[k][index[k]]) = tmp;
                index[k] += sizeof(double);
                if (index[k] >= 1024) {
                    fwrite(buffers[k], sizeof(char), 1024, files[k]);
                    blockcounts[k]++;
                    memset(buffers[k], 0, 1024 * sizeof(char));
                    headsinfo[k][hindex[k]++] = 1024 / sizeof(double);
                    offsets[k] += 1024 / sizeof(double);
                    headsinfo[k][hindex[k]++] = offsets[k];
                    if (hindex[k] >= 1024) {
                        fwrite(headsinfo[k], sizeof(int), 1024, heads[k]);
                        memset(headsinfo[k], 0, 1024 * sizeof(int));
                        hindex[k];
                    }
                }
                break;
            }
            case AVRO_STRING: {
                string tmp = r[0]->fieldAt(k).value<string>();
                if ((index[k] + tmp.length()) >= 1024) {
                    fwrite(buffers[k], sizeof(char), 1024, files[k]);
                    blockcounts[k]++;
                    memset(buffers[k], 0, 1024 * sizeof(char));
                    headsinfo[k][hindex[k]++] = strl;
                    offsets[k] += strl;
                    headsinfo[k][hindex[k]++] = offsets[k];
                    index[k] = 0;
                    if (hindex[k] >= 1024) {
                        fwrite(headsinfo[k], sizeof(int), 1024, heads[k]);
                        memset(headsinfo[k], 0, 1024 * sizeof(int));
                        hindex[k];
                    }
                }
                std::strcpy(&buffers[k][index[k]], tmp.c_str());
                index[k] += tmp.length();
                strl++;
                break;
            }
        }
    }
    for (int k = 0; k < culnum; ++k) {
        fwrite(buffers[k], sizeof(char), 1024, files[k]);
        blockcounts[k]++;
        fwrite(headsinfo[k], sizeof(int), 1024, heads[k]);
    }
    fstream fo;
    fo.open("./fileout.dat", ios_base::out | ios_base::binary);
    long *foffsets = new long[culnum];
    int blocksize = 1024;
    int rowcount = 0;
    fo.write((char *) &blocksize, sizeof(blocksize));
    fo.write((char *) &rowcount, sizeof(rowcount));
    fo.write((char *) &culnum, sizeof(culnum));
    fo << schema << endl;
    int typeint;
    float typefloat;
    long *result = new long[culnum];
    std::vector<int> bc(2);
    for (int k = 0; k < culnum; ++k) {
        sprintf(filename, "%s%d%s", "./file", k, ".dat");
        sprintf(headname, "%s%d%s", "./file", k, ".head");
        heads[k] = fopen(headname, "wb+");
        files[k] = fopen(filename, "wb+");
    }
    for (int j = 0; j < culnum; ++j) {
        fo.write((char *) &blockcounts[j], sizeof(int));
        foffsets[j] = fo.tellg();
        fo.write((char *) &foffsets[j], sizeof(long));
        for (int i = 0; i < blockcounts[j]; ++i) {
            fread(&bc[0], sizeof bc[0], bc.size(), heads[j]);
            fo.write((char *) &bc[0], sizeof(int));
            fo.write((char *) &bc[1], sizeof(int));
        }
    }
    fo.close();
    FILE *fp = fopen("./fileout.dat", "ab+");
    char *buffer = new char[1024];
    for (int j = 0; j < culnum; ++j) {
        result[j] = ftell(fp);
        for (int i = 0; i < blockcounts[j]; ++i) {
            fread(buffer, sizeof(char), 1024, files[j]);
            fwrite(buffer, sizeof(char), 1024, fp);
        }
    }
    fflush(fp);
    fclose(fp);
    fp = fopen("./fileout.dat", "rb+");
    for (int m = 0; m < culnum; ++m) {
        fseek(fp, foffsets[0], SEEK_SET);
        fwrite((char *) &result[0], sizeof(result[0]), 1, fp);
    }
    ifstream file_in;
    file_in.open("fileou.dat", ios_base::in | ios_base::binary);
    unique_ptr<HeadReader> headreader(new HeadReader());
    headreader->readHeader(file_in);
    file_in.close();
    FILE *fpr = fopen("fileou.dat", "rb");
    int rowcounts = headreader->getRowCount();

    cout << endl;
    cout << r[0]->fieldAt(0).value<int>() << endl;
    cout << r[0]->fieldAt(1).value<double>() << endl;
    cout << r[0]->fieldAt(2).value<string>() << endl;
    for (int i = 0; i < 1; i++) {
        delete r[i];
    }
    for (int l = 0; l < culnum; ++l) {
        fflush(files[l]);
        fflush(heads[l]);
        fclose(files[l]);
        fclose(heads[l]);
        delete buffers[l];
        delete headsinfo[l];
    }
    cout << "1" << endl;
//    std::free(files) ;
//    cout<<"1"<<endl;
//    std::free(heads) ;
//    cout<<"1"<<endl;
//    std::free(buffers) ;
//    cout<<"1"<<endl;
//    std::free(headsinfo) ;
//    cout<<"1"<<endl;
//    std::free(filename) ;
//    cout<<"1"<<endl;
//    std::free(headname) ;
//    cout<<"1"<<endl;
    return 0;
}
