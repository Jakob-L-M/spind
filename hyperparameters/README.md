# Hyperparameter Optimization
To utilize the full potential of _SPIND_, we will search for the best performing set of hyperparameter. The execution relies on four user chosen parameters.`CHUNK_SIZE`, `MERGE_SIZE`, `SORT_SIZE` and `VALIDATION_SIZE`. Additionally, the `parallelism degree` can be set globally for the execution.

## Parameter Bounds
- `CHUNK_SIZE` is lower bounded by 10.000 to avoid the creation of massive amounts of files. The upper bound will be 100mil since we already showed that the creation of chunks generally improves performance.
- `MERGE_SIZE` is lower bounded by 2 for obvious reasons. The upper bound will be 1.000 to avoid exceeding the os systems capability of opening files.
- `SORT_SIZE` is upper bounded by your main memory. For my system I will limit it to 5mil. The lower bound is again 10.000 analog to `CHUNK_SIZE`.
- `VALIDATION_SIZE` is lower bounded by 1, representing iterative validation without parallelization and upper bounded by your main memory. Here the memory is exhausted faster, if the dataset has a lot of relations. To be safe, I set the upper bound to 1mil.
- `Parallelism degree` is lower bounded by 1 and upper bounded by the number of threads of the machines CPU. In my case that's 12.



| iter | target | CHUNK_SIZE | MERGE_SIZE | PARALLEL | SORT_SIZE | VALIDATION_SIZE |
|-|-|-|-|-|-|-|
| 1 | 267.6 | 4.176.049 | 720.9 | 1.001 | 1.519e+06 | 1.468e+05 |
| 2 | 155.3 | 932.463 | 187.9 | 4.801 | 1.99e+06 | 5.388e+05 |
| 3 | 173.4 | 4.197.753 | 685.8 | 3.249 | 4.392e+06 | 2.739e+04 |
| 4 | 144.4 | 6.707.970 | 418.5 | 7.146 | 7.105e+05 | 1.981e+05 |
| 5 | 156.6 | 8.009.438 | 968.3 | 4.448 | 3.465e+06 | 8.764e+05 |
| 6 | 280.4 | 8.947.121 | 86.87 | 1.43 | 8.575e+05 | 8.781e+05 |
| 7 | 114.1 | 992.485 | 422.3 | 11.54 | 2.67e+06 | 6.919e+05 |
| 8 | 131.8 | 3.162.001 | 687.1 | 10.18 | 1.013e+05 | 7.501e+05 |
| 9 | 163.3 | 9.888.722 | 748.7 | 4.085 | 3.949e+06 | 1.032e+05 |
| 10 | 146.9 | 4.484.456 | 908.8 | 4.23 | 1.446e+06 | 1.3e+05 |
| 11 | 178.0 | 203.476 | 679.5 | 3.328 | 1.335e+06 | 4.916e+05 |
| 12 | 174.1 | 543.092 | 575.0 | 2.614 | 2.951e+06 | 6.998e+05 |
| 13 | 114.2 | 1.032.321 | 415.2 | 8.638 | 2.077e+06 | 4.995e+04 |
| 14 | 136.7 | 5.363.605 | 664.5 | 6.664 | 4.724e+06 | 5.866e+05 |
| 15 | 173.4 | 9.035e+06 | 139.2 | 2.532 | 4.039e+06 | 3.977e+05 |
| 16 | 145.6 | 7.806e+06 | 948.8 | 3.575 | 4.665e+06 | 2.553e+05 |
| 17 | 125.0 | 3.006e+06 | 459.5 | 7.448 | 3.718e+06 | 4.742e+05 |
| 18 | 220.8 | 7.364e+06 | 691.1 | 2.055 | 3.376e+06 | 6.519e+05 |
| 19 | 114.6 | 3.891e+06 | 925.9 | 10.34 | 2.937e+06 | 2.194e+05 |
| 20 | 115.1 | 8.182e+05 | 850.3 | 9.814 | 3.443e+06 | 7.56e+05 |
| 21 | 130.6 | 2.346e+06 | 139.1 | 10.72 | 3.936e+04 | 7.489e+05 |
| 22 | 124.4 | 5.509e+06 | 215.6 | 11.81 | 2.138e+06 | 8.25e+05 |
| 23 | 151.4 | 6.617e+06 | 505.2 | 3.872 | 3.115e+06 | 4.068e+05 |
| 24 | 120.9 | 1.417e+06 | 268.3 | 10.36 | 2.251e+06 | 7.105e+05 |
| 25 | 209.9 | 4.694e+06 | 39.58 | 2.432 | 3.531e+06 | 4.685e+05 |
| 26 | 270.3 | 3.852e+06 | 533.2 | 1.338 | 6.769e+05 | 6.178e+04 |
| 27 | 129.8 | 2.69e+06 | 541.3 | 5.993 | 4.888e+06 | 3.14e+05 |
| 28 | 178.0 | 1.695e+06 | 750.6 | 3.044 | 2.455e+06 | 7.169e+05 |
| 29 | 140.6 | 6.661e+06 | 526.2 | 5.635 | 2.865e+06 | 3.236e+05 |
| 30 | 116.2 | 5.31e+06 | 577.5 | 10.7 | 4.188e+06 | 7.307e+05 |