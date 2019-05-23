# NCores

## Micro-benchmark of read/wirte sequential files.

Table 1: Staged costs of CORES-C/M with variant selecitivies on TPCH-MQ06 (in seconds).

Threshold| 0.1| 0.01| 0.001| 0.0001 |0.00001| 0.1| 0.01| 0.001| 0.0001 |0.00001
--- | --- | --- | --- |--- |---| --- | --- | --- |--- |---
CORES| -C | -C | -C|-C  | -C | -M  |-M   |-M  |-M  |-M   
IO time |14.96 | 35.03| 37.39 |40.88 |41.55     |8.23 | 11.89 | 11.51 |12.28 |13.01 
filtering |35.26 | 16.56| 14.06| 10.81| 10.09   |56.05 | 55.90| 55.60 |53.99 |53.94 
generation| 5.88 |1.89| 1.48 |0.81 |0.23        |5.55 | 1.93  | 1.52 |0.78 |	0.26
total|56.11  | 53.48 | 52.93  |52.50 |51.87     |69.83 |	69.73| 68.62  |67.06  |	67.20 
