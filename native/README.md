# Native build inputs

The canonical C API implementation is in the sibling Tailcat Go module:
`cmd/libtailcat` and `internal/capi`. `hatch_build.py` copies an exact snapshot of
that module into `native/tailcat/` when creating an sdist. The snapshot is generated
and ignored by version control; edit the original Go module instead.

Build source selection: explicit `PYTAILCAT_SOURCE`, then a sibling checkout with
the C API, then the source snapshot bundled in an sdist. An invalid explicit
override is an error. Wheel builds copy the public C header alongside the native
library; CFFI reads the marked declarations from this same header at runtime.
