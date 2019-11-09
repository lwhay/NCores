//
// Created by Vince Tu on 2019/5/7.
//

#ifndef UNTITLED_GENERIC_H
#define UNTITLED_GENERIC_H

using namespace std;

class GenericRecord;

inline NodePtr resolveSymbol(const NodePtr &node) {
    if (node->type() != AVRO_SYMBOLIC) {
        //throw Exception("Only symbolic nodes may be resolved");
    }
    std::shared_ptr<NodeSymbolic> symNode = std::static_pointer_cast<NodeSymbolic>(node);
    return symNode->getNode();
}

class GenericDatum {
    Type type_;

    GenericDatum(Type t) : type_(t) {}

    template<typename T>
    GenericDatum(Type t, const T &v) : type_(t), value_(v) {}

    void init(const NodePtr &schema);

public:
    std::any value_;

    /**
     * The avro data type this datum holds.
     */
    Type type() const {
        return type_;
    }

    /**
     * Returns the value held by this datum.
     * T The type for the value. This must correspond to the
     * avro type returned by type().
     */
    template<typename T>
    const T &value() const {
        return *std::any_cast<T>(&value_);
    }

    /**
     * Returns the reference to the value held by this datum, which
     * can be used to change the contents. Please note that only
     * value can be changed, the data type of the value held cannot
     * be changed.
     *
     * T The type for the value. This must correspond to the
     * avro type returned by type().
     */
    template<typename T>
    T &value() {
        return *std::any_cast<T>(&value_);
    }

    /**
     * Returns true if and only if this datum is a union.
     */
    bool isUnion() const { return type_ == AVRO_UNION; }

    /**
     * Returns the index of the current branch, if this is a union.
     * \sa isUnion().
     */
    size_t unionBranch() const;

    /**
     * Selects a new branch in the union if this is a union.
     * \sa isUnion().
     */
    void selectBranch(size_t branch);

    /// Makes a new AVRO_NULL datum.
    GenericDatum() : type_(AVRO_NULL) {}

    /// Makes a new AVRO_BOOL datum whose value is of type bool.
    GenericDatum(bool v) : type_(AVRO_BOOL), value_(v) {}

    /// Makes a new AVRO_INT datum whose value is of type int32_t.
    GenericDatum(int32_t v) : type_(AVRO_INT), value_(v) {}

    /// Makes a new AVRO_LONG datum whose value is of type int64_t.
    GenericDatum(int64_t v) : type_(AVRO_LONG), value_(v) {}

#ifdef __APPLE__

    GenericDatum(long v) : type_(AVRO_LONG), value_(v) {}

#endif

    /// Makes a new AVRO_FLOAT datum whose value is of type float.
    GenericDatum(float v) : type_(AVRO_FLOAT), value_(v) {}

    /// Makes a new AVRO_DOUBLE datum whose value is of type double.
    GenericDatum(double v) : type_(AVRO_DOUBLE), value_(v) {}

    /// Makes a new AVRO_STRING datum whose value is of type std::string.
    GenericDatum(const std::string &v) : type_(AVRO_STRING), value_(v) {}

    GenericDatum(const char *v) : type_(AVRO_STRING), value_(v) {}

    /// Makes a new AVRO_BYTES datum whose value is of type
    /// std::vector<uint8_t>.
    GenericDatum(const std::vector<uint8_t> &v) :
            type_(AVRO_BYTES), value_(v) {}

    GenericDatum(char v) :
            type_(AVRO_BYTES), value_(v) {}


    GenericDatum(GenericRecord *v);

    GenericDatum(GenericRecord v);

    /**
     * Constructs a datum corresponding to the given avro type.
     * The value will the appropraite default corresponding to the
     * data type.
     * \param schema The schema that defines the avro type.
     */
    GenericDatum(const NodePtr &schema) : type_(schema->type()) {
        init(schema);
    }

    /**
     * Constructs a datum corresponding to the given avro type and set
     * the value.
     * \param schema The schema that defines the avro type.
     * \param v The value for this type.
     */
    template<typename T>
    GenericDatum(const NodePtr &schema, const T &v) :
            type_(schema->type()) {
        init(schema);
        value<T>() = v;
    }

    /**
     * Constructs a datum corresponding to the given avro type.
     * The value will the appropraite default corresponding to the
     * data type.
     * \param schema The schema that defines the avro type.
     */
    //GenericDatum(const ValidSchema& schema);
};

class GenericContainer {
    NodePtr schema_;

    static void assertType(const NodePtr &schema, Type type);

protected:
    /**
     * Constructs a container corresponding to the given schema.
     */
    GenericContainer(Type type, const NodePtr &s) : schema_(s) {
        //assertType(s, type);
    }

public:
    /// Returns the schema for this object
    const NodePtr &schema() const {
        return schema_;
    }
};

class GenericUnion : public GenericContainer {
    size_t curBranch_;
    GenericDatum datum_;

public:
/**
 * Constructs a generic union corresponding to the given schema \p schema,
 * and the given value. The schema should be of Avro type union
 * and the value should correspond to one of the branches of the union.
 */
    GenericUnion(const NodePtr &schema) :
            GenericContainer(AVRO_UNION, schema), curBranch_(schema->leaves()) {
    }

/**
 * Returns the index of the current branch.
 */
    size_t currentBranch() const { return curBranch_; }

/**
 * Selects a new branch. The type for the value is changed accordingly.
 * \param branch The index for the selected branch.
 */
    void selectBranch(size_t branch) {
        if (curBranch_ != branch) {
            datum_ = GenericDatum(schema()->leafAt(branch));
            curBranch_ = branch;
        }
    }

/**
 * Returns the datum corresponding to the currently selected branch
 * in this union.
 */
    GenericDatum &datum() {
        return datum_;
    }

/**
 * Returns the datum corresponding to the currently selected branch
 * in this union.
 */
    const GenericDatum &datum() const {
        return datum_;
    }
};

class GenericRecord : public GenericContainer {
    std::vector<GenericDatum> fields_;
public:
/**
 * Constructs a generic record corresponding to the given schema \p schema,
 * which should be of Avro type record.
 */
    GenericRecord(const NodePtr &schema) :
            GenericContainer(AVRO_RECORD, schema) {
        fields_.resize(schema->leaves());
        for (size_t i = 0; i < schema->leaves(); ++i) {
            fields_[i] = GenericDatum(schema->leafAt(i));
        }
    }

/**
 * Returns the number of fields in the current record.
 */
    size_t fieldCount() const {
        return fields_.size();
    }

/**
 * Returns index of the field with the given name \p name
 */
    size_t fieldIndex(const std::string &name) const {
        size_t index = 0;
        if (!schema()->nameIndex(name, index)) {
            cout << ("Invalid field name: " + name) << endl;
            return -1;
        }
        return index;
    }

/**
 * Returns true if a field with the given name \p name is located in this r
 * false otherwise
 */
    bool hasField(const std::string &name) const {
        size_t index = 0;
        return schema()->nameIndex(name, index);
    }

/**
 * Returns the field with the given name \p name.
 */
    const GenericDatum &field(const std::string &name) const {
        return fieldAt(fieldIndex(name));
    }

/**
 * Returns the reference to the field with the given name \p name,
 * which can be used to change the contents.
 */
    GenericDatum &field(const std::string &name) {
        if (fieldIndex(name) == -1) {
            cout << "this name doesn't exisit";
//            return NULL;
        }
        return fieldAt(fieldIndex(name));
    }

/**
 * Returns the field at the given position \p pos.
 */
    const GenericDatum &fieldAt(size_t pos) const {
        return fields_[pos];
    }

/**
 * Returns the reference to the field at the given position \p pos,
 * which can be used to change the contents.
 */
    GenericDatum &fieldAt(size_t pos) {
        return fields_[pos];
    }

/**
 * Replaces the field at the given position \p pos with \p v.
 */
    void setFieldAt(size_t pos, const GenericDatum &v) {
        // assertSameType(v, schema()->leafAt(pos));
        fields_[pos] = v;
    }
};

class GenericArray : public GenericContainer {
public:
/**
 * The contents type for the array.
 */
    typedef std::vector<GenericDatum> Value;

/**
 * Constructs a generic array corresponding to the given schema \p schema,
 * which should be of Avro type array.
 */
    GenericArray(const NodePtr &schema) : GenericContainer(AVRO_ARRAY, schema) {
    }

/**
 * Returns the contents of this array.
 */
    const Value &value() const {
        return value_;
    }

/**
 * Returns the reference to the contents of this array.
 */
    Value &value() {
        return value_;
    }

private:
    Value value_;
};

class GenericFixed : public GenericContainer {
    std::vector<uint8_t> value_;
public:
/**
 * Constructs a generic enum corresponding to the given schema \p schema,
 * which should be of Avro type fixed.
 */
    GenericFixed(const NodePtr &schema) : GenericContainer(AVRO_FIXED, schema) {
        value_.resize(schema->fixedSize());
    }

    GenericFixed(const NodePtr &schema, const std::vector<uint8_t> &v) :
            GenericContainer(AVRO_FIXED, schema), value_(v) {}

/**
 * Returns the contents of this fixed.
 */
    const std::vector<uint8_t> &value() const {
        return value_;
    }

/**
 * Returns the reference to the contents of this fixed.
 */
    std::vector<uint8_t> &value() {
        return value_;
    }
};

GenericDatum::GenericDatum(GenericRecord *v) {
    type_ = AVRO_RECORD;
    value_ = *v;
}

GenericDatum::GenericDatum(GenericRecord v) {
    type_ = AVRO_RECORD;
    value_ = v;
}

void GenericDatum::init(const NodePtr &schema) {
    NodePtr sc = schema;
    if (type_ == AVRO_SYMBOLIC) {
        sc = resolveSymbol(schema);
        type_ = sc->type();
    }
    switch (type_) {
        case AVRO_NULL:
            break;
        case AVRO_BOOL:
            value_ = bool();
            break;
        case AVRO_INT:
            value_ = int32_t();
            break;
        case AVRO_LONG:
            value_ = int64_t();
            break;
        case AVRO_FLOAT:
            value_ = float();
            break;
        case AVRO_DOUBLE:
            value_ = double();
            break;
        case AVRO_STRING:
            value_ = string();
            break;
        case AVRO_BYTES:
            value_ = vector<uint8_t>();
            break;
        case AVRO_FIXED:
            value_ = GenericFixed(sc);
            break;
        case AVRO_RECORD:
            value_ = GenericRecord(sc);
            break;
        case AVRO_ARRAY:
            value_ = GenericArray(sc);
            break;
//                case AVRO_MAP:
//                    value_ = GenericMap(sc);
//                    break;
//                case AVRO_UNION:
//                    value_ = GenericUnion(sc);
//                    break;
//                default:
//                    throw Exception(boost::format("Unknown schema type %1%") %
//                                    toString(type_));
    }
}


#endif //UNTITLED_GENERIC_H
