# RML-KGC module tests

`ModuleTest` runs each folder `<module>/<group>/` here: one mapping that merges kg-construct test cases MappingWeaver passes, and their expected outputs (HANDBOOK: Test resource organization). Per case, the input files are in a folder named after the case, its named resources (triples maps, sources, views, tables) have the suffix `-<case id>`, and the relative paths of its sources start with its folder. An input file identical, apart from line endings, to one of an earlier case in the group (in the order of the table below) is kept once, in that earlier case's folder, and both cases read it there. `lv/iterables` keeps a copy per case: when logical views with different fields read one file, MappingWeaver gives one view the fields of another (`RMLLVTC0002b` misses `item.weight`). `mapping.ttl` lists the resources per case in the order of the table below, each case's triples maps first. `output.nq` is the union of the expected outputs of the cases, each with its own blank nodes. Each group covers one feature of its module. Cases that generate blank nodes share a group only when they generate them from different values.

The cases come from the suites at the commits that mapping-test-runner 0.1.1 pins. A case is added to a group, or a group to a module, once MappingWeaver passes the case in the conformance run; RML-CC and RML-star have no group yet, since MappingWeaver passes none of their cases with an expected output.

| Group | Suite | Cases |
|---|---|---|
| `core/blank-nodes` | rml-core @ `82ab28d` | RMLTC0001b-JSON, RMLTC0002b-JSON, RMLTC0012b-JSON, RMLTC0012e-JSON |
| `core/graphs` | rml-core @ `82ab28d` | RMLTC0006a-JSON, RMLTC0007b-JSON, RMLTC0007g-JSON, RMLTC0009b-JSON, RMLTC0028b-JSON |
| `core/joins` | rml-core @ `82ab28d` | RMLTC0009a-JSON, RMLTC0021a-JSON, RMLTC0030a-JSON, RMLTC0030b-JSON |
| `core/literals` | rml-core @ `82ab28d` | RMLTC0015a-JSON, RMLTC0022a-JSON, RMLTC0022b-JSON, RMLTC0022c-JSON, RMLTC0022d-JSON, RMLTC0022e-JSON, RMLTC0028a-JSON, RMLTC0028c-JSON, RMLTC0031a-JSON, RMLTC0031b-JSON, RMLTC0031c-JSON |
| `core/terms` | rml-core @ `82ab28d` | RMLTC0000-JSON, RMLTC0001a-JSON, RMLTC0003c-JSON, RMLTC0004a-JSON, RMLTC0005a-JSON, RMLTC0007a-JSON, RMLTC0008c-JSON, RMLTC0010a-JSON, RMLTC0010b-JSON, RMLTC0010c-JSON, RMLTC0013a-JSON, RMLTC0019a-JSON, RMLTC0020a-JSON, RMLTC0023f-JSON, RMLTC0027a-JSON, RMLTC0029a-JSON |
| `fnml/objects` | rml-fnml @ `dc9ac9a` | RMLFNMLTC0001-CSV, RMLFNMLTC0002-CSV, RMLFNMLTC0003-CSV, RMLFNMLTC0004-CSV, RMLFNMLTC0005-CSV, RMLFNMLTC0007-CSV, RMLFNMLTC0008-CSV, RMLFNMLTC0021-CSV, RMLFNMLTC0041-CSV, RMLFNMLTC0051-CSV |
| `fnml/term-maps` | rml-fnml @ `dc9ac9a` | RMLFNMLTC0011-CSV, RMLFNMLTC0031-CSV, RMLFNMLTC0061-CSV, RMLFNMLTC0071-CSV, RMLFNMLTC0081-CSV |
| `io/complex` | rml-io @ `c0c5902` | RMLSTC0011a, RMLSTC0011b, RMLSTC0011c, RMLSTC0011d, RMLSTC0011e, RMLSTC0012a, RMLSTC0012e |
| `io/csvw` | rml-io-registry @ `3fc5e76` | RMLIOREGTC0012b, RMLIOREGTC0012c, RMLIOREGTC0012d, RMLIOREGTC0012e, RMLIOREGTC0012f, RMLIOREGTC0012g, RMLIOREGTC0012h |
| `io/registry` | rml-io-registry @ `3fc5e76` | RMLIOREGTC0001a, RMLIOREGTC0002a, RMLIOREGTC0002d, RMLIOREGTC0003a, RMLIOREGTC0003e |
| `io/sources` | rml-io @ `c0c5902` | RMLSTC0004b, RMLSTC0004c, RMLSTC0006b, RMLSTC0007b, RMLSTC0007c, RMLSTC0007d |
| `lv/fields` | rml-lv @ `1f3d44e` | RMLLVTC0000a, RMLLVTC0000b, RMLLVTC0000c, RMLLVTC0001a, RMLLVTC0001b, RMLLVTC0001c, RMLLVTC0001d |
| `lv/iterables` | rml-lv @ `1f3d44e` | RMLLVTC0002a, RMLLVTC0002b, RMLLVTC0002c, RMLLVTC0003a, RMLLVTC0003b, RMLLVTC0003c |
