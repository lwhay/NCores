//
// Created by Vince Tu on 2019/12/17.
//

#include "Node.h"

#ifndef CORES_COLUMNARDATA_H
#define CORES_COLUMNARDATA_H

class ColWriterImpl {
public:
    ColWriterImpl(Type type, int bsize) : type_(type), bsize_(bsize) {}

    inline Type type() const {
        return type_;
    }

    inline int bsize() const {
        return bsize_;
    }

    virtual void write(const GenericDatum& data)  = 0;

    virtual void writeRest()  =0;

    virtual int getBlocknum() =0;
protected:
    const Type type_;
    const int bsize_;
};

//fixed required write
class FRColWriter : public ColWriterImpl {
public:
    FRColWriter(int bsize, Type t, string path) : ColWriterImpl(t, bsize), d_ind(0), h_ind(0), lrb(0), block_num(0),rib(0) {
        h_file = fopen((path + ".head").data(), "wb+");
        d_file = fopen((path + ".data").data(), "wb+");
        d_buffer = new char[bsize];
        h_info = new int[bsize];
        if (!h_file) {
            cout << ("File opening failed");
        }
    }

    void write(const GenericDatum& data) {
        switch (type_) {
            case CORES_INT: {
                int tmp = data.value<int32_t>();
                if (d_ind >= bsize_) {
                    d_ind = 0;
                    fwrite(d_buffer, sizeof(char), bsize_, d_file);
                    block_num++;
                    memset(d_buffer, 0, bsize_ * sizeof(char));
                    h_info[h_ind++] = rib;
                    lrb += rib;
                    h_info[h_ind++] = lrb;
                    rib=0;
                    if (h_ind >= bsize_) {
                        fwrite(h_info, sizeof(int), bsize_, h_file);
                        memset(h_info, 0, bsize_ * sizeof(int));
                        h_ind = 0;
                    }
                }
                *(int *) (&d_buffer[d_ind]) = tmp;
                d_ind += sizeof(int);
                rib++;
                break;
            }
            case CORES_LONG: {
                int64_t tmp = data.value<int64_t>();
                if (d_ind >= bsize_) {
                    d_ind = 0;
                    fwrite(d_buffer, sizeof(char), bsize_, d_file);
                    block_num++;
                    memset(d_buffer, 0, bsize_ * sizeof(char));
                    h_info[h_ind++] = rib;
                    lrb += rib;
                    h_info[h_ind++] = lrb;
                    rib=0;
                    if (h_ind >= bsize_) {
                        fwrite(h_info, sizeof(int), bsize_, h_file);
                        memset(h_info, 0, bsize_ * sizeof(int));
                        h_ind = 0;
                    }
                }
                *(int64_t *) (&(d_buffer[d_ind])) = tmp;
                d_ind += sizeof(int64_t);
                rib++;
                break;
            }
            case CORES_DOUBLE: {
                double tmp = data.value<double>();
                if (d_ind >= bsize_) {
                    d_ind = 0;
                    fwrite(d_buffer, sizeof(char), bsize_, d_file);
                    block_num++;
                    memset(d_buffer, 0, bsize_ * sizeof(char));
                    h_info[h_ind++] = rib;
                    lrb += rib;
                    h_info[h_ind++] = lrb;
                    rib=0;
                    if (h_ind >= bsize_) {
                        fwrite(h_info, sizeof(int), bsize_, h_file);
                        memset(h_info, 0, bsize_ * sizeof(int));
                        h_ind = 0;
                    }
                }
                *(double *) (&d_buffer[d_ind]) = tmp;
                d_ind += sizeof(double);
                rib++;
                break;
            }
            case CORES_FLOAT: {
                float tmp = data.value<float>();
                if (d_ind >= bsize_) {
                    d_ind = 0;
                    fwrite(d_buffer, sizeof(char), bsize_, d_file);
                    block_num++;
                    memset(d_buffer, 0, bsize_ * sizeof(char));
                    h_info[h_ind++] = rib;
                    lrb += rib;
                    h_info[h_ind++] = lrb;
                    rib=0;
                    if (h_ind >= bsize_) {
                        fwrite(h_info, sizeof(int), bsize_, h_file);
                        memset(h_info, 0, bsize_ * sizeof(int));
                        h_ind = 0;
                    }
                }
                *(float *) (&d_buffer[d_ind]) = tmp;
                d_ind += sizeof(float);
                rib++;
                break;
            }
            case CORES_BYTES: {
                char tmp = data.value<char>();
                if (d_ind >= bsize_) {
                    d_ind = 0;
                    fwrite(d_buffer, sizeof(char), bsize_, d_file);
                    block_num++;
                    memset(d_buffer, 0, bsize_ * sizeof(char));
                    h_info[h_ind++] = rib;
                    lrb += rib;
                    h_info[h_ind++] = lrb;
                    rib=0;
                    if (h_ind >= bsize_) {
                        fwrite(h_info, sizeof(char), bsize_, h_file);
                        memset(h_info, 0, bsize_ * sizeof(char));
                        h_ind = 0;
                    }
                }
                *(char *) (&d_buffer[d_ind]) = tmp;
                d_ind += sizeof(char);
                rib++;
                break;
            }
            case CORES_ARRAY: {
                GenericArray ga = data.value<GenericArray>();
                int tmp = ga.value().size();
                if (d_ind >= bsize_) {
                    d_ind = 0;
                    fwrite(d_buffer, sizeof(char), bsize_, d_file);
                    block_num++;
                    memset(d_buffer, 0, bsize_ * sizeof(char));
                    h_info[h_ind++] = rib;
                    lrb += rib;
                    h_info[h_ind++] = lrb;
                    rib=0;
                    if (h_ind >= bsize_) {
                        fwrite(h_info, sizeof(int), bsize_, h_file);
                        memset(h_info, 0, bsize_ * sizeof(int));
                        h_ind = 0;
                    }
                }
                *(int *) (&d_buffer[d_ind]) = tmp;
                d_ind += sizeof(int);
                rib++;
                break;
            }
            default:
                cout << "incorect type" << endl;
        }
    }

    void writeRest(){
        fwrite(d_buffer, sizeof(char), bsize_, d_file);
        block_num++;
        h_info[h_ind++] = rib;
        lrb += rib;
        h_info[h_ind++] = lrb;
        if (h_ind != 0) {
            fwrite(h_info, sizeof(int), bsize_, h_file);
        }

        fflush(d_file);
        fflush(h_file);
        fclose(d_file);
        fclose(h_file);
    }

    inline int getBlocknum() {
        return block_num;
    }
private:
    FILE *d_file;
    FILE *h_file;
    char *d_buffer;
    int *h_info;
    int d_ind;
    int h_ind;
    int lrb;//last row of current block
    int block_num;
    int rib;
};

//fixed optional write
class FOColWriter : public ColWriterImpl {
public:
    FOColWriter(int bsize, Type t, string path) : ColWriterImpl(t, bsize), d_ind(0), h_ind(0), lrb(0), block_num(0),
                                                  rib(0) {
        h_file = fopen((path + ".head").data(), "wb+");
        d_file = fopen((path + ".data").data(), "wb+");
        d_buffer = new char[bsize];
        h_info = new int[bsize];
        if (!h_file) {
            cout << ("File opening failed");
        }
    }

    void nullWrite() {
        int zero = 0;
        if (rib % 8 == 0) zero = 1;
        if (d_ind + zero + valid.size() >= bsize_) {
            fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
            fwrite(d_buffer, sizeof(char), bsize_ - valid.size(), d_file);
            valid.clear();
            d_ind = 0;
            block_num++;
            memset(d_buffer, 0, bsize_ * sizeof(char));
            h_info[h_ind++] = rib;
            lrb += rib;
            h_info[h_ind++] = lrb;
            rib = 0;
            if (h_ind >= bsize_) {
                fwrite(h_info, sizeof(int), bsize_, h_file);
                memset(h_info, 0, bsize_ * sizeof(int));
                h_ind = 0;
            }
        }
        if (zero) {
            valid.push_back(0x0);
        }
        rib++;
    }

    void write(const GenericDatum& data) {
        if (data.type() == CORES_NULL) {
            nullWrite();
        } else {
            switch (type_) {
                case CORES_INT: {
                    int tmp = data.value<int>();
                    int zero = 0;
                    if (rib % 8 == 0) zero = 1;
                    if (d_ind + zero + valid.size() >= bsize_) {
                        fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                        fwrite(d_buffer, sizeof(char), bsize_ - valid.size(), d_file);
                        valid.clear();
                        d_ind = 0;
                        block_num++;
                        memset(d_buffer, 0, bsize_ * sizeof(char));
                        h_info[h_ind++] = rib;
                        lrb += rib;
                        h_info[h_ind++] = lrb;
                        rib = 0;
                        if (h_ind >= bsize_) {
                            fwrite(h_info, sizeof(int), bsize_, h_file);
                            memset(h_info, 0, bsize_ * sizeof(int));
                            h_ind = 0;
                        }
                    }
                    if (zero) {
                        valid.push_back(0x0);
                    }
                    valid.back() |= 1L << (rib % 8);
                    *(int *) (&d_buffer[d_ind]) = tmp;
                    d_ind += sizeof(int);
                    rib++;
                    break;
                }
                case CORES_LONG: {
                    int64_t tmp = data.value<int64_t>();
                    int zero = 0;
                    if (rib % 8 == 0) zero = 1;
                    if (d_ind + zero + valid.size() >= bsize_) {
                        fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                        fwrite(d_buffer, sizeof(char), bsize_ - valid.size(), d_file);
                        valid.clear();
                        d_ind = 0;
                        block_num++;
                        memset(d_buffer, 0, bsize_ * sizeof(char));
                        h_info[h_ind++] = rib;
                        lrb += rib;
                        h_info[h_ind++] = lrb;
                        rib = 0;

                        if (h_ind >= bsize_) {
                            fwrite(h_info, sizeof(int), bsize_, h_file);
                            memset(h_info, 0, bsize_ * sizeof(int));
                            h_ind = 0;
                        }
                    }
                    if (rib % 8 == 0) zero = 1;
                    if (zero) {
                        valid.push_back(0x0);
                    }
                    valid.back() |= 1L << (rib % 8);

                    *(int64_t *) (&(d_buffer[d_ind])) = tmp;
                    d_ind += sizeof(int64_t);
                    rib++;
                    break;
                }
                case CORES_DOUBLE: {
                    double tmp = data.value<double>();
                    int zero = 0;
                    if (rib % 8 == 0) zero = 1;
                    if (d_ind + zero + valid.size() >= bsize_) {
                        fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                        fwrite(d_buffer, sizeof(char), bsize_ - valid.size(), d_file);
                        valid.clear();
                        d_ind = 0;
                        block_num++;
                        memset(d_buffer, 0, bsize_ * sizeof(char));
                        h_info[h_ind++] = rib;
                        lrb += rib;
                        h_info[h_ind++] = lrb;
                        rib = 0;
                        if (h_ind >= bsize_) {
                            fwrite(h_info, sizeof(int), bsize_, h_file);
                            memset(h_info, 0, bsize_ * sizeof(int));
                            h_ind = 0;
                        }
                    }
                    if (zero) {
                        valid.push_back(0x0);
                    }
                    valid.back() |= 1L << (rib % 8);

                    *(double *) (&d_buffer[d_ind]) = tmp;
                    d_ind += sizeof(double);
                    rib++;
                    break;
                }
                case CORES_FLOAT: {
                    float tmp = data.value<float>();
                    int zero = 0;
                    if (rib % 8 == 0) zero = 1;
                    if (d_ind + zero + valid.size() >= bsize_) {
                        fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                        fwrite(d_buffer, sizeof(char), bsize_ - valid.size(), d_file);
                        valid.clear();
                        d_ind = 0;
                        block_num++;
                        memset(d_buffer, 0, bsize_ * sizeof(char));
                        h_info[h_ind++] = rib;
                        lrb += rib;
                        h_info[h_ind++] = lrb;
                        rib = 0;
                        if (h_ind >= bsize_) {
                            fwrite(h_info, sizeof(int), bsize_, h_file);
                            memset(h_info, 0, bsize_ * sizeof(int));
                            h_ind = 0;
                        }
                    }
                    if (zero) {
                        valid.push_back(0x0);
                    }
                    valid.back() |= 1L << (rib % 8);

                    *(float *) (&d_buffer[d_ind]) = tmp;
                    d_ind += sizeof(float);
                    rib++;
                    break;
                }
                case CORES_BYTES: {
                    char tmp = data.value<char>();
                    int zero = 0;
                    if (rib % 8 == 0) zero = 1;
                    if (d_ind + zero + valid.size() >= bsize_) {
                        fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                        fwrite(d_buffer, sizeof(char), bsize_ - valid.size(), d_file);
                        valid.clear();
                        d_ind = 0;
                        block_num++;
                        memset(d_buffer, 0, bsize_ * sizeof(char));
                        h_info[h_ind++] = rib;
                        lrb += rib;
                        h_info[h_ind++] = lrb;
                        rib = 0;
                        if (h_ind >= bsize_) {
                            fwrite(h_info, sizeof(int), bsize_, h_file);
                            memset(h_info, 0, bsize_ * sizeof(int));
                            h_ind = 0;
                        }
                    }
                    if (zero) {
                        valid.push_back(0x0);
                    }
                    valid.back() |= 1L << (rib % 8);

                    *(char *) (&d_buffer[d_ind]) = tmp;
                    d_ind += sizeof(char);
                    rib++;
                    break;
                }
                case CORES_ARRAY: {
                    int tmp = data.value<GenericArray>().value().size();
                    int zero = 0;
                    if (rib % 8 == 0) zero = 1;
                    if (d_ind + zero + valid.size() >= bsize_) {
                        fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                        fwrite(d_buffer, sizeof(char), bsize_ - valid.size(), d_file);
                        valid.clear();
                        d_ind = 0;
                        block_num++;
                        memset(d_buffer, 0, bsize_ * sizeof(char));
                        h_info[h_ind++] = rib;
                        lrb += rib;
                        h_info[h_ind++] = lrb;
                        rib = 0;
                        if (h_ind >= bsize_) {
                            fwrite(h_info, sizeof(int), bsize_, h_file);
                            memset(h_info, 0, bsize_ * sizeof(int));
                            h_ind = 0;
                        }
                    }
                    if (zero) {
                        valid.push_back(0x0);
                    }
                    valid.back() |= 1L << (rib % 8);

                    *(int *) (&d_buffer[d_ind]) = tmp;
                    d_ind += sizeof(int);
                    rib++;
                    break;
                }
                default:
                    cout << "incorect type" << endl;
            }
        }
    }

    void writeRest(){

        fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
        fwrite(d_buffer, sizeof(char), bsize_ - valid.size(), d_file);

        block_num++;
        h_info[h_ind++] = rib;
        lrb += rib;
        h_info[h_ind++] = lrb;

        if (h_ind != 0) {
            fwrite(h_info, sizeof(int), bsize_, h_file);
        }
        
        fflush(d_file);
        fflush(h_file);
        fclose(d_file);
        fclose(h_file);
    }

    inline int getBlocknum() {
        return block_num;
    }
private:
    FILE *d_file;
    FILE *h_file;
    char *d_buffer;
    int *h_info;
    int d_ind;
    int h_ind;
    int lrb;//last row of current block
    int block_num;
    int rib;

    vector<unsigned char> valid;
};

// versatile required 
class VRColWriter:public ColWriterImpl{
public:
    VRColWriter(int bsize, Type t, string path) : ColWriterImpl(t, bsize), d_ind(0), h_ind(0), lrb(0), block_num(0),rib(0) {
        h_file = fopen((path + ".head").data(), "wb+");
        d_file = fopen((path + ".data").data(), "wb+");
        d_buffer = new char[bsize];
        h_info = new int[bsize];
        if (!h_file) {
            cout << ("File opening failed");
        }

        if (bsize < 1 << 8) offsize = 1;
        else if (bsize < 1 << 16) offsize = 2;
        else if (bsize < 1 << 24) offsize = 3;
        else if (bsize < 1 << 32) offsize = 4;
        else cout << "bsize_ is too large" << endl;
    }

    void write(const GenericDatum& data) {
        switch (type_) {
            case CORES_STRING: {
                string tmp = data.value<string>();
                if ((d_ind + tmp.length() + 1 + offsize * rib) > bsize_) {
                    int tmpoff = offsize * rib;
                    for (int j :off_ind) {
                        int tmp = j + tmpoff;
                        fwrite((char *) &tmp, offsize, sizeof(char), d_file);
                    }
                    fwrite(d_buffer, sizeof(char), bsize_ - tmpoff, d_file);
                    block_num++;
                    memset(d_buffer, 0, bsize_ * sizeof(char));
                    h_info[h_ind++] = rib;
                    lrb += rib;
                    h_info[h_ind++] = lrb;
                    d_ind = 0;
                    rib = 0;
                    off_ind.clear();
                    if (h_ind >= bsize_) {
                        fwrite(h_info, sizeof(int), bsize_, h_file);
                        memset(h_info, 0, bsize_ * sizeof(int));
                        h_ind = 0;
                    }
                }
                off_ind.push_back(d_ind);
                std::strcpy(&d_buffer[d_ind], tmp.c_str());
                d_ind += tmp.length() + 1;
                rib++;
                break;
            }
            default:
                cout << "incorect type" << endl;
        }
    }

    void writeRest(){
        int tmpoff = offsize * rib;
        for (int j :off_ind) {
            int tmp = j + tmpoff;
            fwrite((char *) &tmp, offsize, sizeof(char), d_file);
        }
        fwrite(d_buffer, sizeof(char),
               bsize_ - offsize * off_ind.size(), d_file);

        block_num++;
        h_info[h_ind++] = rib;
        lrb += rib;
        h_info[h_ind++] = lrb;

        if (h_ind != 0) {
            fwrite(h_info, sizeof(int), bsize_, h_file);
        }

        fflush(d_file);
        fflush(h_file);
        fclose(d_file);
        fclose(h_file);
    }

    inline int getBlocknum() {
        return block_num;
    }
private:
    FILE *d_file;
    FILE *h_file;
    char *d_buffer;
    int *h_info;
    int d_ind;
    int h_ind;
    int lrb;//last row of current block
    int block_num;
    int rib;
    
    int offsize;
    vector<int> off_ind;
    
};

// versatile optional 
class VOColWriter:public ColWriterImpl{
public:
    VOColWriter(int bsize, Type t, string path) : ColWriterImpl(t, bsize), d_ind(0), h_ind(0), lrb(0), block_num(0),rib(0) {
        h_file = fopen((path + ".head").data(), "wb+");
        d_file = fopen((path + ".data").data(), "wb+");
        d_buffer = new char[bsize];
        h_info = new int[bsize];
        if (!h_file) {
            cout << ("File opening failed");
        }

        if (bsize < 1 << 8) offsize = 1;
        else if (bsize < 1 << 16) offsize = 2;
        else if (bsize < 1 << 24) offsize = 3;
        else if (bsize < 1 << 32) offsize = 4;
        else cout << "bsize_ is too large" << endl;
    }

    void write(const GenericDatum& data) {
        switch (type_) {
            case CORES_STRING: {
                string tmp = data.value<string>();
                int zero = 0;
                if (rib % 8 == 0) zero = 1;
                if ((d_ind + tmp.length() + 1 + offsize * rib + valid.size()) > bsize_) {
                    fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
                    int _size=valid.size();
                    int tmpoff = offsize * rib;
                    for (int j :off_ind) {
                        int tmp = j + tmpoff;
                        fwrite((char *) &tmp, offsize, sizeof(char), d_file);
                    }
                    fwrite(d_buffer, sizeof(char),
                           bsize_ - valid.size() - offsize * off_ind.size(), d_file);
                    valid.clear();
                    valid.push_back(0x0);
                    block_num++;
                    memset(d_buffer, 0, bsize_ * sizeof(char));
                    h_info[h_ind++] = rib;
                    lrb += rib;
                    h_info[h_ind++] = lrb;
                    d_ind = 0;
                    rib = 0;
                    off_ind.clear();
                    if (h_ind >= bsize_) {
                        fwrite(h_info, sizeof(int), bsize_, h_file);
                        memset(h_info, 0, bsize_ * sizeof(int));
                        h_ind = 0;
                    }
                }else if (zero) {
                    valid.push_back(0x0);
                }
                valid.back() |= 1L << (rib % 8);

                int tmpi = tmp.length();
                off_ind.push_back(d_ind);
                std::strcpy(&d_buffer[d_ind], tmp.c_str());
                d_ind += tmp.length() + 1;
                rib++;
                break;
            }
            default:
                cout << "incorect type" << endl;
        }
    }

    void writeRest(){
        fwrite((char *) &valid[0], valid.size(), sizeof(char), d_file);
        int tmpoff = offsize * rib;
        for (int j :off_ind) {
            int tmp = j + tmpoff;
            fwrite((char *) &tmp, offsize, sizeof(char), d_file);
        }
        fwrite(d_buffer, sizeof(char),
               bsize_ - valid.size()- offsize * off_ind.size(), d_file);

        block_num++;
        h_info[h_ind++] = rib;
        lrb += rib;
        h_info[h_ind++] = lrb;

        if (h_ind != 0) {
            fwrite(h_info, sizeof(int), bsize_, h_file);
        }

        fflush(d_file);
        fflush(h_file);
        fclose(d_file);
        fclose(h_file);
    }

    inline int getBlocknum() {
        return block_num;
    }
private:
    FILE *d_file;
    FILE *h_file;
    char *d_buffer;
    int *h_info;
    int d_ind;
    int h_ind;
    int lrb;//last row of current block
    int block_num;
    int rib;

    int offsize;
    vector<int> off_ind;

    vector<unsigned char> valid;
};

#endif //CORES_COLUMNARDATA_H
