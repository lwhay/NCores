//
// Created by Vince Tu on 2019/5/6.
//

#ifndef UNTITLED_JSONPARSER_H
#define UNTITLED_JSONPARSER_H

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


typedef bool Bool;
typedef int64_t Long;
typedef double Double;
typedef std::string String;

using namespace std;

class Entity;

class Node;

struct StreamReader {
    char *_in;
    int _next;
    int _end;

    StreamReader() : _in(NULL), _next(0), _end(0) {}

    StreamReader(char *in) : _in(in), _next(0), _end(0) {}

    char read() {
        return _in[_next++];
    }

    void reset(char *str) {
        _in = str;
        _next = 0;
        _end = strlen(str);
    }

    void readBytes(char *b, size_t n) {
        while (n > 0) {
            size_t q = _end - _next;
            if (q > n) {
                q = n;
            }
            ::memcpy(b, _in + _next, q);
            _next += q;
            b += q;
            n -= q;
        }
    }

    bool hasMore() {
        if (_next == _end) {
            return false;
        } else if (_next != _end) {
            return true;
        } else {
            cout << "has more wrong" << endl;
            return true;
        }
    }
};


typedef std::vector<Entity> Array;
typedef std::map<std::string, Entity> Object;

typedef std::shared_ptr<Node> NodePtr;

enum EntityType {
    etNull,
    etBool,
    etLong,
    etDouble,
    etString,
    etArray,
    etObject
};

class Entity {
    EntityType type_;
    std::any value_;

    void ensureType(EntityType) const {};
public:
    Entity() : type_(etNull) {}

    Entity(Bool v) : type_(etBool), value_(v) {}

    Entity(Long v) : type_(etLong), value_(v) {}

    Entity(Double v) : type_(etDouble), value_(v) {}

    Entity(const shared_ptr<String> &v) : type_(etString), value_(v) {}

    Entity(const shared_ptr<Array> &v) : type_(etArray), value_(v) {}

    Entity(const shared_ptr<Object> &v) : type_(etObject), value_(v) {}

    EntityType type() const { return type_; }

    Bool boolValue() const {
        ensureType(etBool);
        return any_cast<Bool>(value_);
    }

    Long longValue() const {
        ensureType(etLong);
        return any_cast<Long>(value_);
    }

    Double doubleValue() const {
        ensureType(etDouble);
        return any_cast<Double>(value_);
    }

    const String &stringValue() const {
        ensureType(etString);
        return **any_cast<shared_ptr<String> >(&value_);
    }

    const Array &arrayValue() const {
        ensureType(etArray);
        return **any_cast<shared_ptr<Array> >(&value_);
    }

    const Object &objectValue() const {
        ensureType(etObject);
        return **any_cast<shared_ptr<Object> >(&value_);
    }

    std::string toString() const;
};

class JsonParser {
public:
    enum Token {
        tkNull,
        tkBool,
        tkLong,
        tkDouble,
        tkString,
        tkArrayStart,
        tkArrayEnd,
        tkInt,
        tkFloat,
        tkObjectStart,
        tkObjectEnd
    };

private:
    enum State {
        stValue,    // Expect a data type
        stArray0,   // Expect a data type or ']'
        stArrayN,   // Expect a ',' or ']'
        stObject0,  // Expect a string or a '}'
        stObjectN,  // Expect a ',' or '}'
        stKey       // Expect a ':'
    };
    std::stack<State> stateStack;
    State curState;
    bool hasNext;
    char nextChar;
    bool peeked;

    StreamReader in_;
    Token curToken;
    bool bv;
    int64_t lv;
    double dv;
    std::string sv;

    Token doAdvance() {
        char ch = next();
        if (ch == ']') {
            if (curState == stArray0 || stArrayN) {
                curState = stateStack.top();
                stateStack.pop();
                return tkArrayEnd;
            }
        } else if (ch == '}') {
            if (curState == stObject0 || stObjectN) {
                curState = stateStack.top();
                stateStack.pop();
                return tkObjectEnd;
            }
        } else if (ch == ',') {
            if (curState == stObjectN) {
                curState = stObject0;
            }
            ch = next();
        } else if (ch == ':') {
            curState = stObjectN;
            ch = next();
        }

        if (curState == stObject0) {
            curState = stKey;
        } else if (curState == stArray0) {
            curState = stArrayN;
        }

        switch (ch) {
            case '[':
                stateStack.push(curState);
                curState = stArray0;
                return tkArrayStart;
            case '{':
                stateStack.push(curState);
                curState = stObject0;
                return tkObjectStart;
            case '"':
                return tryString();
            case 't':
                bv = true;
                return tryLiteral("rue", 3, tkBool);
            case 'f':
                bv = false;
                return tryLiteral("alse", 4, tkBool);
            case 'n':
                return tryLiteral("ull", 3, tkNull);
            default:
                if (isdigit(ch) || ch == '-') {
                    return tryNumber(ch);
                }
        }
    }

    Token tryLiteral(const char exp[], size_t n, Token tk) {
        return tk;
    }

    Token tryNumber(char ch) {
        sv.clear();
        sv.push_back(ch);

        hasNext = false;
        int state = (ch == '-') ? 0 : (ch == '0') ? 1 : 2;
        for (;;) {
            switch (state) {
                case 0:
                    if (in_.hasMore()) {
                        ch = in_.read();
                        if (isdigit(ch)) {
                            state = (ch == '0') ? 1 : 2;
                            sv.push_back(ch);
                            continue;
                        }
                        hasNext = true;
                    }
                    break;
                case 1:
                    if (in_.hasMore()) {
                        ch = in_.read();
                        if (ch == '.') {
                            state = 3;
                            sv.push_back(ch);
                            continue;
                        }
                        hasNext = true;
                    }
                    break;
                case 2:
                    if (in_.hasMore()) {
                        ch = in_.read();
                        if (isdigit(ch)) {
                            sv.push_back(ch);
                            continue;
                        } else if (ch == '.') {
                            state = 3;
                            sv.push_back(ch);
                            continue;
                        }
                        hasNext = true;
                    }
                    break;
                case 3:
                case 6:
                    if (in_.hasMore()) {
                        ch = in_.read();
                        if (isdigit(ch)) {
                            sv.push_back(ch);
                            state++;
                            continue;
                        }
                        hasNext = true;
                    }
                    break;
                case 4:
                    if (in_.hasMore()) {
                        ch = in_.read();
                        if (isdigit(ch)) {
                            sv.push_back(ch);
                            continue;
                        } else if (ch == 'e' || ch == 'E') {
                            sv.push_back(ch);
                            state = 5;
                            continue;
                        }
                        hasNext = true;
                    }
                    break;
                case 5:
                    if (in_.hasMore()) {
                        ch = in_.read();
                        if (ch == '+' || ch == '-') {
                            sv.push_back(ch);
                            state = 6;
                            continue;
                        } else if (isdigit(ch)) {
                            sv.push_back(ch);
                            state = 7;
                            continue;
                        }
                        hasNext = true;
                    }
                    break;
                case 7:
                    if (in_.hasMore()) {
                        ch = in_.read();
                        if (isdigit(ch)) {
                            sv.push_back(ch);
                            continue;
                        }
                        hasNext = true;
                    }
                    break;
            }
            if (state == 1 || state == 2 || state == 4 || state == 7) {
                if (hasNext) {
                    nextChar = ch;
                }
                std::istringstream iss(sv);
                if (state == 1 || state == 2) {
                    iss >> lv;
                    return tkLong;
                } else {
                    iss >> dv;
                    return tkDouble;
                }
            } else {
                if (hasNext) {
                    cout << "unexpected" << endl;
                } else {
                    cout << "Unexpected EOF" << endl;
                }
            }
        }
    }

    Token tryString() {
        sv.clear();
        for (;;) {
            char ch = in_.read();
            if (ch == '"') {
                return tkString;
            } else if (ch == '\\') {
                ch = in_.read();
                switch (ch) {
                    case '"':
                    case '\\':
                    case '/':
                        sv.push_back(ch);
                        continue;
                    case 'b':
                        sv.push_back('\b');
                        continue;
                    case 'f':
                        sv.push_back('\f');
                        continue;
                    case 'n':
                        sv.push_back('\n');
                        continue;
                    case 'r':
                        sv.push_back('\r');
                        continue;
                    case 't':
                        sv.push_back('\t');
                        continue;
                    case 'u':
                    case 'U': {
                        unsigned int n = 0;
                        char e[4];
                        in_.readBytes((char *) e, 4);
                        for (int i = 0; i < 4; i++) {
                            n *= 16;
                            char c = e[i];
                            if (isdigit(c)) {
                                n += c - '0';
                            } else if (c >= 'a' && c <= 'f') {
                                n += c - 'a' + 10;
                            } else if (c >= 'A' && c <= 'F') {
                                n += c - 'A' + 10;
                            }
                        }
                        sv.push_back(n);
                    }
                        break;
                }
            } else {
                sv.push_back(ch);
            }
        }
    }

    char next() {
        char ch = hasNext ? nextChar : ' ';
        while (isspace(ch)) {
            ch = in_.read();
        }
        hasNext = false;
        return ch;
    }

public:
    JsonParser() : curState(stValue), hasNext(false), peeked(false) {}

    void init(char *is) {
        in_.reset(is);
    }

    Token advance() {
        if (!peeked) {
            curToken = doAdvance();
        } else {
            peeked = false;
        }
        return curToken;
    }

    Token peek() {
        if (!peeked) {
            curToken = doAdvance();
            peeked = true;
        }
        return curToken;
    }

    void pk() {
        peeked = true;
    }

    void expectToken(Token tk);

    bool boolValue() {
        return bv;
    }

    Token cur() {
        return curToken;
    }

    double doubleValue() {
        return dv;
    }

    int64_t longValue() {
        return lv;
    }

    std::string stringValue() {
        return sv;
    }

    static const char *const tokenNames[];

    static const char *toString(Token tk) {
        return tokenNames[tk];
    }
};

const char *const
        JsonParser::tokenNames[] = {
                "Null",
                "Bool",
                "Integer",
                "Double",
                "String",
                "Array start",
                "Array end",
                "Object start",
                "Object end",
        };

Entity readEntity(JsonParser &p) {
    switch (p.peek()) {
        case JsonParser::tkNull:
            p.advance();
            return Entity();
        case JsonParser::tkBool:
            p.advance();
            return Entity(p.boolValue());
        case JsonParser::tkLong:
            p.advance();
            return Entity(p.longValue());
        case JsonParser::tkDouble:
            p.advance();
            return Entity(p.doubleValue());
        case JsonParser::tkString: {
            p.advance();
            shared_ptr<String> s = make_shared<String>(p.stringValue());
            return Entity(s);
        }
        case JsonParser::tkArrayStart: {
            p.advance();
            std::shared_ptr<Array> v = std::make_shared<Array>();
            while (p.peek() != JsonParser::tkArrayEnd) {
                v->push_back(readEntity(p));
            }
            p.advance();
            return Entity(v);
        }
        case JsonParser::tkObjectStart: {
            p.advance();
            shared_ptr<Object> v = make_shared<Object>();
            while (p.peek() != JsonParser::tkObjectEnd) {
                p.advance();
                std::string k = p.stringValue();
                Entity n = readEntity(p);
                v->insert(std::make_pair(k, n));
            }
            p.advance();
            return Entity(v);
        }
        default:
            throw std::domain_error(JsonParser::toString(p.peek()));
    }

}


#endif //UNTITLED_JSONPARSER_H
