# Compared rml_kgc tests - LOG


## Differences (high-level) between current tests and remote tests: 


**Differences Detected!**
```diff
1d0
< ./remote_tests_log.txt
226,233d224
< ./rml-core/RMLTC0008b-multidata-JSON/mapping.ttl
< ./rml-core/RMLTC0008b-multidata-JSON/output.nq
< ./rml-core/RMLTC0008b-multidata-JSON/README.md
< ./rml-core/RMLTC0008b-multidata-JSON/student.json
< ./rml-core/RMLTC0008b-multidata-one-source-JSON/mapping.ttl
< ./rml-core/RMLTC0008b-multidata-one-source-JSON/output.nq
< ./rml-core/RMLTC0008b-multidata-one-source-JSON/README.md
< ./rml-core/RMLTC0008b-multidata-one-source-JSON/student.json
1257d1247
< ./rml-lv/.gitkeep
```

**Local-only tests detected: move these to src/test/resources/rml_kgc/test-cases/spec-adaptations:**
```text
./remote_tests_log.txt
./rml-core/RMLTC0008b-multidata-JSON/mapping.ttl
./rml-core/RMLTC0008b-multidata-JSON/output.nq
./rml-core/RMLTC0008b-multidata-JSON/README.md
./rml-core/RMLTC0008b-multidata-JSON/student.json
./rml-core/RMLTC0008b-multidata-one-source-JSON/mapping.ttl
./rml-core/RMLTC0008b-multidata-one-source-JSON/output.nq
./rml-core/RMLTC0008b-multidata-one-source-JSON/README.md
./rml-core/RMLTC0008b-multidata-one-source-JSON/student.json
./rml-lv/.gitkeep
```
## Differences (low-level) between current tests and remote tests: 

**Differences Detected: rml-star - RMLSTARTC001b**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rml_kgc/spec/rml-star/RMLSTARTC001b/data2.csv remote_tests/resources/rml_kgc/spec/rml-star/RMLSTARTC001b/data2.csv
1d0
< **Input 1**
```

**Differences Detected: rml-star - RMLSTARTC002b**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rml_kgc/spec/rml-star/RMLSTARTC002b/data2.csv remote_tests/resources/rml_kgc/spec/rml-star/RMLSTARTC002b/data2.csv
1d0
< **Input 1**
```

**Differences Detected: rml-star - RMLSTARTC003b**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rml_kgc/spec/rml-star/RMLSTARTC003b/data2.csv remote_tests/resources/rml_kgc/spec/rml-star/RMLSTARTC003b/data2.csv
1d0
< **Input 1**
```

**Differences Detected: rml-star - RMLSTARTC004b**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rml_kgc/spec/rml-star/RMLSTARTC004b/data2.csv remote_tests/resources/rml_kgc/spec/rml-star/RMLSTARTC004b/data2.csv
1d0
< **Input 1**
```

**Differences Detected: rml-star - RMLSTARTC005b**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rml_kgc/spec/rml-star/RMLSTARTC005b/data2.csv remote_tests/resources/rml_kgc/spec/rml-star/RMLSTARTC005b/data2.csv
1d0
< **Input 1**
```

**Differences Detected: rml-star - RMLSTARTC006b**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rml_kgc/spec/rml-star/RMLSTARTC006b/data2.csv remote_tests/resources/rml_kgc/spec/rml-star/RMLSTARTC006b/data2.csv
1d0
< **Input 1**
```

**Differences Detected: rml-star - RMLSTARTC007b**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rml_kgc/spec/rml-star/RMLSTARTC007b/data2.csv remote_tests/resources/rml_kgc/spec/rml-star/RMLSTARTC007b/data2.csv
1d0
< **Input 1**
```

**Differences Detected: rml-star - RMLSTARTC008b**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rml_kgc/spec/rml-star/RMLSTARTC008b/data2.csv remote_tests/resources/rml_kgc/spec/rml-star/RMLSTARTC008b/data2.csv
1d0
< **Input 1**
```

_Removing remote rml_kgc tests..._


## Consulted rml_kgc repositories and commits: 

**Repository/commit of module rml-cc:**
https://github.com/kg-construct/rml-cc
 & 
2026-08-17 19:29:12: https://github.com/kg-construct/rml-cc/commit/d97fddf0f769f352d256a96b93544f3dfd192ffc

**Repository/commit of module rml-star:**
https://github.com/kg-construct/rml-star
 & 
2026-08-17 19:29:35: https://github.com/kg-construct/rml-star/commit/711f72efe10f7a38b6d058a837dafe3455ad93d3

**Repository/commit of module rml-io:**
https://github.com/kg-construct/rml-io
 & 
2026-08-17 19:28:44: https://github.com/kg-construct/rml-io/commit/980b90626d86394af91ed606f8493927d59d5e67

**Repository/commit of module rml-lv:**
https://github.com/kg-construct/rml-lv
 & 
2026-08-17 19:29:48: https://github.com/kg-construct/rml-lv/commit/e3aa626b0fed4c7c0068908533b7da4712d44bd3

**Repository/commit of module rml-fnml:**
https://github.com/kg-construct/rml-fnml
 & 
2026-08-17 19:29:25: https://github.com/kg-construct/rml-fnml/commit/dc9ac9acdafb01c3edfc119a6cdcd2706f768662

**Repository/commit of module rml-io-registry:**
https://github.com/kg-construct/rml-io-registry
 & 
2026-08-17 19:30:00: https://github.com/kg-construct/rml-io-registry/commit/3bb0c3ce7ada75d053d584faa58361ab74b6fdbd

**Repository/commit of module rml-core:**
https://github.com/kg-construct/rml-core
 & 
2026-08-17 19:28:08: https://github.com/kg-construct/rml-core/commit/82ab28d46803ba66a83c133f1db371a60116f84d

## Differences (high-level) between current rmlio tests and remote rmlio tests: 


**Differences Detected!**
```diff
988,989d987
< ./fno/class.csv
< ./fno/function_tests.ttl
1076d1073
< ./fno/student.csv
```

**Local-only tests detected: move these to src/test/resources/rmlio/test-cases/spec-adaptations:**
```text
./fno/class.csv
./fno/function_tests.ttl
./fno/student.csv
```
## Differences (low-level) between current rmlio tests and remote rmlio tests: 

**Differences Detected: core - RMLTC0002a-JSON**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rmlio/spec/core/RMLTC0002a-JSON/output.nq remote_tests/resources/rmlio/spec/core/RMLTC0002a-JSON/output.nq
2c2
< <http://example.com/10/Venus> <http://example.com/id> "10"^^<http://www.w3.org/2001/XMLSchema#integer> .
---
> <http://example.com/10/Venus> <http://example.com/id> "10" .
```

**Differences Detected: core - RMLTC0002g-JSON**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rmlio/spec/core/RMLTC0002g-JSON/mapping.ttl remote_tests/resources/rmlio/spec/core/RMLTC0002g-JSON/mapping.ttl
12c12
<     rml:source "student.json";
---
>     rml:source "student2.json";
14c14
<     rml:iterator "$.students[*]"
---
>     rml:iterator "$.students[*]]"
```

**Differences Detected: core - RMLTC0007c-JSON**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rmlio/spec/core/RMLTC0007c-JSON/output.nq remote_tests/resources/rmlio/spec/core/RMLTC0007c-JSON/output.nq
2c2
< <http://example.com/Student/10/Venus> <http://example.com/id> "10"^^<http://www.w3.org/2001/XMLSchema#integer> .
---
> <http://example.com/Student/10/Venus> <http://example.com/id> "10" . 
```

**Differences Detected: core - RMLTC0007d-JSON**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rmlio/spec/core/RMLTC0007d-JSON/output.nq remote_tests/resources/rmlio/spec/core/RMLTC0007d-JSON/output.nq
2c2
< <http://example.com/Student/10/Venus> <http://example.com/id> "10"^^<http://www.w3.org/2001/XMLSchema#integer> .
---
> <http://example.com/Student/10/Venus> <http://example.com/id> "10" .
```

**Differences Detected: core - RMLTC0007e-JSON**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rmlio/spec/core/RMLTC0007e-JSON/output.nq remote_tests/resources/rmlio/spec/core/RMLTC0007e-JSON/output.nq
1c1
< <http://example.com/Student/10/Venus> <http://example.com/id> "10"^^<http://www.w3.org/2001/XMLSchema#integer> <http://example.com/PersonGraph> .
---
> <http://example.com/Student/10/Venus> <http://example.com/id> "10" <http://example.com/PersonGraph> .
```

**Differences Detected: core - RMLTC0007f-JSON**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rmlio/spec/core/RMLTC0007f-JSON/output.nq remote_tests/resources/rmlio/spec/core/RMLTC0007f-JSON/output.nq
3c3
< <http://example.com/Student/10/Venus> <http://example.com/id> "10"^^<http://www.w3.org/2001/XMLSchema#integer> <http://example.com/PersonGraph> .
---
> <http://example.com/Student/10/Venus> <http://example.com/id> "10" <http://example.com/PersonGraph> .
```

**Differences Detected: core - RMLTC0008a-JSON**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rmlio/spec/core/RMLTC0008a-JSON/output.nq remote_tests/resources/rmlio/spec/core/RMLTC0008a-JSON/output.nq
3c3
< <http://example.com/Student/10/Venus%20Williams> <http://example.com/id> "10"^^<http://www.w3.org/2001/XMLSchema#integer> <http://example.com/graph/Student/10/Venus%20Williams> .
---
> <http://example.com/Student/10/Venus%20Williams> <http://example.com/id> "10" <http://example.com/graph/Student/10/Venus%20Williams> . 
```

**Differences Detected: core - RMLTC0008b-JSON**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rmlio/spec/core/RMLTC0008b-JSON/output.nq remote_tests/resources/rmlio/spec/core/RMLTC0008b-JSON/output.nq
3c3
< <http://example.com/Student/10/Venus%20Williams> <http://example.com/id> "10"^^<http://www.w3.org/2001/XMLSchema#integer> .
---
> <http://example.com/Student/10/Venus%20Williams> <http://example.com/id> "10" . 
```

**Differences Detected: core - RMLTC0011b-JSON**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rmlio/spec/core/RMLTC0011b-JSON/output.nq remote_tests/resources/rmlio/spec/core/RMLTC0011b-JSON/output.nq
8c8
< <http://example.com/sport/110> <http://example.com/id> "110"^^<http://www.w3.org/2001/XMLSchema#integer> .
---
> <http://example.com/sport/110> <http://example.com/id> "110" .
10c10
< <http://example.com/sport/111> <http://example.com/id> "111"^^<http://www.w3.org/2001/XMLSchema#integer> .
---
> <http://example.com/sport/111> <http://example.com/id> "111" .
12c12
< <http://example.com/sport/112> <http://example.com/id> "112"^^<http://www.w3.org/2001/XMLSchema#integer> .
---
> <http://example.com/sport/112> <http://example.com/id> "112" .
```

**Differences Detected: core - RMLTC0012a-JSON**

```diff
diff -rwB -X /tmp/tmp.k7HxSdaf4D src/test/resources/rmlio/spec/core/RMLTC0012a-JSON/output.nq remote_tests/resources/rmlio/spec/core/RMLTC0012a-JSON/output.nq
1c1
< _:BobSmith30 <http://example.com/amount> "30"^^<http://www.w3.org/2001/XMLSchema#integer> .
---
> _:BobSmith30 <http://example.com/amount> "30" .
3c3
< _:SueJones20 <http://example.com/amount> "20"^^<http://www.w3.org/2001/XMLSchema#integer> .
---
> _:SueJones20 <http://example.com/amount> "20" .
```

_Removing remote rmlio tests..._


## Consulted rmlio repositories and commits: 

**Repository/commit of module fno:**
https://github.com/RMLio/rml-fno-test-cases
 & 
2026-08-17 19:33:07: https://github.com/RMLio/rml-fno-test-cases/commit/7474c6596f4d821996b46b74d837bec611a6ad8f

**Repository/commit of module core:**
https://github.com/kg-construct/rml-test-cases
 & 
2026-08-17 19:32:36: https://github.com/kg-construct/rml-test-cases/commit/803dd3ec6b7185801cf19ebecaa18513baf78613

