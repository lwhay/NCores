//
// Created by Michael on 2018/12/2.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <sys/time.h>
#include <pthread.h>

#define TOTAL (1024 * 1024 * 1024)

#define UNIT 4

#define THR_NUM 8

using namespace std;

/*  $./memcopy.exe 1
 * 4-byte
    //1 thread
    0 0
    1 1
    2 2
    3 3
    1	1073741824
    32812946	389187082	107374182400
    //2 threads
    0 0
    1 1
    2 2
    3 3
    1	536870912
    0 0
    1 1
    2 2
    3 3
    1	536870912
    19062647	211296728	53687091200
    19015706	211437447	53687091200
    //4 threads
    0 0
    01 1
    0
    2 21
    1
    32  23
    3 3
    1	268435456
    1	268435456
    0 0
    01 0
    11 1
    2 2
    3 3
    2 2
    3 3
    1	268435456
    1	268435456
    12234573	134937301	26843545600
    12328524	135327723	26843545600
    12609881	137327645	26843545600
    13109359	138187540	26843545600
    //8 threads
    0 0
    1 1
    2 2
    3 3
    2	67108864
    0 0
    1 1
    2 2
    3 3
    2	67108864
    0 0
    1 1
    2 2
    3 3
    2	67108864
    0 0
    1 1
    02  02
    31  31
    2 2
    3 3
    2	67108864
    2	67108864
    0 0
    01  10
    21  2
    13
    32
    2
    3 3
    2	67108864
    0 0
    1 1
    2 2
    3 3
    2	67108864
    2	67108864
    17292271	55582469	6710886400
    16870943	56128809	6710886400
    17124888	56218603	6710886400
    17562648	55780853	6710886400
    19254633	54510735	6710886400
    19187484	54593509	6710886400
    17922131	56140103	6710886400
    18125884	55936350	6710886400
* 4m-bytee
    //1 thread
    0 0
    1 1
    2 2
    3 3
    1048576	1024
    33061465	36032335	102400
    //2 threads
    0 0
    1 1
    2 2
    3 3
    0 0
    1048576	512
    1 1
    2 2
    3 3
    1048576	512
    27091829	30423850	51200
    27359583	30234213	51200
    //4 threads
    0 0
    1 1
    2 2
    3 3
    1048576	256
    0 0
    1 1
    2 2
    3 3
    1048576	256
    0 0
    1 1
    2 2
    3 3
    1048576	256
    0 0
    1 1
    2 2
    3 3
    1048576	256
    24827241	31860284	25600
    24906041	32765837	25600
    25124685	33469073	25600
    25186746	33578881	25600
* 1G-byte
    //1 thread
    0 0
    1 1
    2 2
    3 3
    268435456	4
    33702546	37031864	400
    //2 threads
    0 0
    1 1
    2 2
    3 3
    268435456	2
    0 0
    1 1
    2 2
    3 3
    268435456	2
    23999812	33687679	200
    24233838	33641187	200
    //4 threads
    0 0
    1 1
    2 2
    3 3
    268435456	1
    0 0
    1 1
    2 2
    3 3
    268435456	1
    0 0
    1 1
    2 2
    3 3
    268435456	1
    0 0
    1 1
    2 2
    3 3
    268435456	1
    24702957	32515808	100
    24890248	32344124	100
    24750187	33187313	100
    24171675	33797091	100
 */

struct params {
    int total;
    int gran;
};

void *memcopy(void *p) {
    int size = reinterpret_cast<params *>(p)->total;
    int gran = reinterpret_cast<params *>(p)->gran;
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
    int *src = new int[size];
    int *des = new int[size];
    long long duration = 0;
    long long inittime = 0;
    long long copycount = 0;
    int count = size / gran;
    cout << gran << "\t" << count << endl;
    timeval endTime;
    timeval b;
    timeval e;
    for (int r = 0; r < 100; r++) {
        gettimeofday(&b, NULL);
        for (int i = 0; i < size; i++) {
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
    return (void *) 0;
}

int main(int argc, char **argv) {
    int gran = 1 << atoi(argv[1]) * (UNIT / 4);
    pthread_t workers[THR_NUM];
    struct params *p = new params;
    p->total = TOTAL / THR_NUM;
    p->gran = gran;
    for (int i = 0; i < THR_NUM; i++) {
        pthread_create(&workers[i], nullptr, &memcopy, p);
    }
    for (int i = 0; i < THR_NUM; i++) {
        pthread_join(workers[i], nullptr);
    }
    delete p;
    return 0;
}
