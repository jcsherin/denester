# Introduction

Converts nested records into columnar format in which all values which map to
the same key are stored contiguously.

Schema:

```protobuf
message ExampleNested {
  optional group a {
    required group b {
      optional string c;
      required bool d;
    }
  }
}
```

Nested Values:

```
{ a: { b: { c: "hello", d: false } } }
{ a: { b: { c: "world", d: false } } }
{ a: { b: { d: true } } }
```

All the values of the nested field `a.b.c` are stored contiguously. So `a.b.c`
can be retrieved without having to read `a.b.d`.

```
["hello", "world", null]
```

The columnar format also stores metadata (repetition levels and definition
levels) which preserves the structural information and can be used to
reconstruct the original nested records.
