// Shim header exposing xdelta3 to Swift via CXDelta3 module.
// Vendored from https://github.com/jmacd/xdelta (Apache 2.0)
//
// Forward-declare xd3_decode_memory with concrete uint64_t sizes rather than
// the usize_t typedef, which depends on SIZEOF_* platform macros that Clang's
// Swift-module pass may not see.  The ABI is identical on all Apple 64-bit
// targets (uint64_t == unsigned long == 8 bytes).  The full xdelta3.h is still
// picked up by xdelta3.c via the headerSearchPath cSetting, so the
// implementation compiles correctly against the original signature.
#include <stdint.h>

int xd3_decode_memory(const uint8_t *input, uint64_t input_size,
                      const uint8_t *source, uint64_t source_size,
                      uint8_t *output, uint64_t *output_size,
                      uint64_t avail_output, int flags);

int xd3_encode_memory(const uint8_t *input, uint64_t input_size,
                      const uint8_t *source, uint64_t source_size,
                      uint8_t *output, uint64_t *output_size,
                      uint64_t avail_output, int flags);
