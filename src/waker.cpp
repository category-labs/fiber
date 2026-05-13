
#include "boost/fiber/waker.hpp"
#include "boost/fiber/context.hpp"

namespace boost {
namespace fibers {

bool
waker::wake() const noexcept {
    BOOST_ASSERT(epoch_ > 0);
    BOOST_ASSERT(ctx_ != nullptr);

    return ctx_->wake(epoch_);
}

void
wait_queue::suspend_and_wait( detail::spinlock_lock & lk, context * active_ctx) {
    waker_with_hook w{ active_ctx->create_waker() };
    slist_.push_back(w);
    // suspend this fiber
    active_ctx->suspend( lk);
    BOOST_ASSERT( ! w.is_linked() );
}

void
wait_queue::notify_one() {
    while ( ! slist_.empty() ) {
        waker & w = slist_.front();
        slist_.pop_front();
        if ( w.wake()) {
            break;
        }
    }
}

void
wait_queue::notify_all() {
    while ( ! slist_.empty() ) {
        waker & w = slist_.front();
        slist_.pop_front();
        w.wake();
    }
}

bool
wait_queue::empty() const {
    return slist_.empty();
}

}
}
