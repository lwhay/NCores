// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <bitset>

#include "src/utility.h"

namespace bitweaving {

std::string ToString(WordUnit word) {
    std::bitset<kNumBitsPerWord> bitset(word);
    return bitset.to_string();
}

std::istream &operator>>(std::istream &in, ColumnType &type) {
    int val;
    if (in >> val) {
        switch (val) {
            case kNaive:
            case kBitWeavingH:
            case kBitWeavingV:
                type = ColumnType(val);
                break;
        }
    }
    return in;
}

}  // namespace bitweaving

