# Contributing to Chronon

Thank you for your interest in contributing to Chronon! This document explains how to get started.

## Getting Started

1. Fork the repository and clone your fork
2. Create a branch for your work: `git checkout -b my-feature`
3. Build and run tests to make sure everything works:

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug
make -j$(nproc)
ctest --output-on-failure
```

## Development Environment

- **C++20** compiler required (GCC 12+ or Clang 20+)
- **CMake 3.25+**
- **clang-format** for code formatting (config in `.clang-format`)
- **clang-tidy** for static analysis (config in `.clang-tidy`)

System dependencies: `yaml-cpp`, `fmt`, and optionally `zstd`. Other dependencies (stdexec, FlatBuffers) are fetched automatically via CPM.

## Code Style

- Run `clang-format` before committing. The pre-commit hook enforces this automatically.
- All code lives in the `chronon::` namespace.
- Headers use `.hpp` extension.
- Use the single include `#include "chronon/Chronon.hpp"` for public API.
- Prefer `TickableUnit` for new simulation units.
- Use the macro-free observability API (`EventCounter`, `event<>()`, `TimelineLane`, `debug<>()`) instead of raw print statements.

## Making Changes

### Bug Fixes

- Include a test that reproduces the bug before fixing it.
- Reference the issue number in your PR description if one exists.

### New Features

- Open an issue first to discuss the design, especially for larger changes.
- Add tests covering the new functionality.
- Update documentation in `website/docs/` if the feature affects the public API.

### Tests

Tests use a simple assertion-based pattern:

```cpp
#include <cassert>
#include <iostream>

int main() {
    // Test case
    assert(condition && "description of what failed");
    std::cout << "PASSED: test description" << std::endl;
    return 0;
}
```

Run a specific test with:

```bash
ctest -R test_name --output-on-failure
```

## Submitting a Pull Request

1. Make sure all tests pass locally.
2. Keep commits focused - one logical change per commit.
3. Write a clear PR description explaining **what** changed and **why**.
4. The CI will run multi-compiler builds, sanitizers (ASan, TSan, UBSan), and clang-tidy.

## License

By contributing, you agree that your contributions will be licensed under the [Mozilla Public License 2.0](LICENSE).
