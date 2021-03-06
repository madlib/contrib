#include <madlib/ports/postgres/PGInterface.hpp>
#include <madlib/ports/postgres/PGAllocator.hpp>

namespace madlib {

namespace ports {

namespace postgres {

inline AllocatorSPtr PGInterface::allocator(
    AbstractAllocator::Context inMemContext) {
    
    return AllocatorSPtr(new PGAllocator(this, inMemContext));
}

} // namespace postgres

} // namespace ports

} // namespace madlib
