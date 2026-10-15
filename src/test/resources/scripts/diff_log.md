# Compared rmlio tests - LOG


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

