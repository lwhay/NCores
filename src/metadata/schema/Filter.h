//
// Created by Vince Tu on 28/12/2019.
//

#ifndef CORES_FILTER_H
#define CORES_FILTER_H

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

    bool filter(string value) {
        switch (ft) {
            case ftCj:
                return L->filter(value) & R->filter(value);
            case ftDj:
                return L->filter(value) | R->filter(value);
            case ftGt:
                return value > strdata;
            case ftGe:
                return value >= strdata;
            case ftLt:
                return value < strdata;
            case ftLe:
                return value <= strdata;
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


#endif //CORES_FILTER_H
