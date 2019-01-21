//
// Created by Michael on 2018/12/2.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <sys/time.h>
#include <vector>

#define SIZE (1LL << 30)

#define UNIT 4

using namespace std;

/*  $./memcopy.exe 1
	0 0
	1 1
	2 2
	3 3
	1       1073741824
	44137553        2818543111      107374182400
*/

const uint32_t arr_size = (1080 * 720 * 3); //HD image in rgb24
const uint32_t iterations = 100000;
uint8_t arr1[arr_size];
uint8_t arr2[arr_size];
std::vector<uint8_t> v;

void tinytest() {
    {
        memcpy(arr1, arr2, sizeof(arr1));
        printf("memcpy()\n");
    }

    printf("%p, %p\n", (arr1 + arr_size), arr2);

    v.reserve(sizeof(arr1));
    {
        std::copy(arr1, arr1 + sizeof(arr1), v.begin());
        printf("std::copy()\n");
    }

    {
        time_t t = time(NULL);
        for (uint32_t i = 0; i < iterations; ++i)
            memcpy(arr1, arr2, sizeof(arr1));
        printf("memcpy()    elapsed %I64u s  gran %I64u\n", time(NULL) - t, sizeof(arr1));
    }

    {
        time_t t = time(NULL);
        for (uint32_t i = 0; i < iterations; ++i)
            std::copy(arr1, arr1 + sizeof(arr1), v.begin());
        printf("std::copy() elapsed %I64u s  gran %I64u\n", time(NULL) - t, sizeof(arr1));
    }
}

int main(int argc, char **argv) {
    /*tinytest();
    return 0;*/
    int *t = new int[4];
    int *s = new int[4];
    for (int i = 0; i < 4; i++) {
        s[i] = i;
        t[i] = -1;
    }
    for (int i = 0; i < 4; i++) {
        memcpy(t + i * 1, s + i * 1, 4);
    }
    for (int i = 0; i < 4; i++) {
        cout << s[i] << " " << t[i] << endl;
    }
    delete[] s;
    delete[] t;
    timeval begTime;
    gettimeofday(&begTime, NULL);
    int *src = new int[SIZE];
    int *des = new int[SIZE];
    long duration = 0;
    long inittime = 0;
    long copycount = 0;
    int gran = atoi(argv[1]) * (UNIT / 4);
    int count = SIZE / gran;
    cout << gran << "\t" << count << "\t" << SIZE << endl;
    timeval endTime;
    timeval b;
    timeval e;
    for (int r = 0; r < 100; r++) {
        gettimeofday(&b, NULL);
        for (int i = 0; i < SIZE; i++) {
            src[i] = r * 100 + i;
        }
        gettimeofday(&e, NULL);
        inittime += (e.tv_sec - b.tv_sec) * 1000000 + e.tv_usec - b.tv_usec;
        //cout << "copied" << endl;
        gettimeofday(&begTime, NULL);
        for (long long i = 0; i < count; i++) {
            //cout << des << " + " << i * gran << "<->" << src << " + " << i * gran << endl;
            memcpy(des + i * gran, src + i * gran, gran * 4);
            copycount++;
        }
        gettimeofday(&endTime, NULL);
        duration += (endTime.tv_sec - begTime.tv_sec) * 1000000 + endTime.tv_usec - begTime.tv_usec;
    }
    cout << inittime << "\t" << duration << "\t" << copycount << endl;
    delete[] src;
    delete[] des;
    return 0;
}
