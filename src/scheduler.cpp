
//          Copyright Oliver Kowalke 2013.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "boost/fiber/scheduler.hpp"

#include <mutex>

#include <boost/assert.hpp>

#include "boost/fiber/context.hpp"
#include "boost/fiber/exceptions.hpp"

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_PREFIX
#endif

namespace boost {
namespace fibers {

void
scheduler::release_terminated_() noexcept {
    while ( ! terminated_queue_.empty() ) {
        context * ctx = & terminated_queue_.front();
        terminated_queue_.pop_front();
        BOOST_ASSERT( ctx->is_context( type::worker_context) );
        BOOST_ASSERT( ! ctx->is_context( type::pinned_context) );
        BOOST_ASSERT( this == ctx->get_scheduler() );
        BOOST_ASSERT( ctx->is_resumable() );
        BOOST_ASSERT( ! ctx->worker_is_linked() );
        BOOST_ASSERT( ! ctx->ready_is_linked() );
#if ! defined(BOOST_FIBERS_NO_ATOMICS)
        BOOST_ASSERT( ! ctx->remote_ready_is_linked() );
#endif
        BOOST_ASSERT( ctx->wait_queue_.empty() );
        BOOST_ASSERT( ctx->terminated_);
        // if last reference, e.g. fiber::join() or fiber::detach()
        // have been already called, this will call ~context(),
        // the context is automatically removeid from worker-queue
        intrusive_ptr_release( ctx);
    }
}

#if ! defined(BOOST_FIBERS_NO_ATOMICS)
void
scheduler::remote_ready2ready_() noexcept {
    remote_ready_queue_type tmp;
    detail::spinlock_lock lk{ remote_ready_splk_ };
    remote_ready_queue_.swap( tmp);
    lk.unlock();
    // get context from remote ready-queue
    while ( ! tmp.empty() ) {
        context * ctx = & tmp.front();
        tmp.pop_front();
        // store context in local queues
        schedule( ctx);
    }
}
#endif

scheduler::scheduler(algo::algorithm::ptr_t algo) noexcept :
    algo_{algo} {
}

scheduler::~scheduler() {
    BOOST_ASSERT( nullptr != main_ctx_);
    BOOST_ASSERT( nullptr != dispatcher_ctx_.get() );
    BOOST_ASSERT( context::active() == main_ctx_);
    // signal dispatcher-context termination
    shutdown_ = true;
    // resume pending fibers
    // by resuming dispatcher-context
    context::active()->suspend();
    // no context' in worker-queue
    BOOST_ASSERT( worker_queue_.empty() );
    BOOST_ASSERT( terminated_queue_.empty() );
    // set active context to nullptr
    context::reset_active();
    // deallocate dispatcher-context
    BOOST_ASSERT( ! dispatcher_ctx_->ready_is_linked() );
    dispatcher_ctx_.reset();
    // set main-context to nullptr
    main_ctx_ = nullptr;
}

boost::context::fiber
scheduler::dispatch() noexcept {
    BOOST_ASSERT( context::active() == dispatcher_ctx_);
    for (;;) {
        if ( shutdown_) {
            // notify sched-algorithm about termination
            algo_->notify();
            if ( worker_queue_.empty() ) {
                break;
            }
        }
        // release terminated context'
        release_terminated_();
#if ! defined(BOOST_FIBERS_NO_ATOMICS)
        // get context' from remote ready-queue
        remote_ready2ready_();
#endif
        // get next ready context
        context * ctx = algo_->pick_next();
        if ( nullptr != ctx) {
            BOOST_ASSERT( ctx->is_resumable() );
            BOOST_ASSERT( ! ctx->ready_is_linked() );
#if ! defined(BOOST_FIBERS_NO_ATOMICS)
            BOOST_ASSERT( ! ctx->remote_ready_is_linked() );
#endif
            BOOST_ASSERT( ! ctx->terminated_is_linked() );
            // push dispatcher-context to ready-queue
            // so that ready-queue never becomes empty
            ctx->resume( dispatcher_ctx_.get() );
            BOOST_ASSERT( context::active() == dispatcher_ctx_.get() );
        } else {
            // no ready context, wait till signaled
            algo_->suspend();
        }
    }
    // release termianted context'
    release_terminated_();
    // return to main-context
    return main_ctx_->suspend_with_cc();
}

void
scheduler::schedule( context * ctx) noexcept {
    BOOST_ASSERT( nullptr != ctx);
    BOOST_ASSERT( ! ctx->ready_is_linked() );
#if ! defined(BOOST_FIBERS_NO_ATOMICS)
    BOOST_ASSERT( ! ctx->remote_ready_is_linked() );
#endif
    BOOST_ASSERT( ! ctx->terminated_is_linked() );
    // push new context to ready-queue
    algo_->awakened( ctx);
}

#if ! defined(BOOST_FIBERS_NO_ATOMICS)
void
scheduler::schedule_from_remote( context * ctx) noexcept {
    BOOST_ASSERT( nullptr != ctx);
    // another thread might signal the main-context of this thread
    BOOST_ASSERT( ! ctx->is_context( type::dispatcher_context) );
    BOOST_ASSERT( this == ctx->get_scheduler() );
    BOOST_ASSERT( ! ctx->ready_is_linked() );
    BOOST_ASSERT( ! ctx->remote_ready_is_linked() );
    BOOST_ASSERT( ! ctx->terminated_is_linked() );
    // protect for concurrent access
    detail::spinlock_lock lk{ remote_ready_splk_ };
    BOOST_ASSERT( ! shutdown_);
    BOOST_ASSERT( nullptr != main_ctx_);
    BOOST_ASSERT( nullptr != dispatcher_ctx_.get() );
    // push new context to remote ready-queue
    ctx->remote_ready_link( remote_ready_queue_);
    lk.unlock();
    // notify scheduler
    algo_->notify();
}
#endif

boost::context::fiber
scheduler::terminate( detail::spinlock_lock & lk, context * ctx) noexcept {
    BOOST_ASSERT( nullptr != ctx);
    BOOST_ASSERT( context::active() == ctx);
    BOOST_ASSERT( this == ctx->get_scheduler() );
    BOOST_ASSERT( ctx->is_context( type::worker_context) );
    BOOST_ASSERT( ! ctx->is_context( type::pinned_context) );
    BOOST_ASSERT( ! ctx->ready_is_linked() );
#if ! defined(BOOST_FIBERS_NO_ATOMICS)
    BOOST_ASSERT( ! ctx->remote_ready_is_linked() );
#endif
    BOOST_ASSERT( ! ctx->terminated_is_linked() );
    BOOST_ASSERT( ctx->wait_queue_.empty() );
    // store the terminated fiber in the terminated-queue
    // the dispatcher-context will call
    ctx->terminated_link( terminated_queue_);
    // remove from the worker-queue
    ctx->worker_unlink();
    // release lock
    lk.unlock();
    // resume another fiber
    return algo_->pick_next()->suspend_with_cc();
}

void
scheduler::yield( context * ctx) noexcept {
    BOOST_ASSERT( nullptr != ctx);
    BOOST_ASSERT( context::active() == ctx);
    BOOST_ASSERT( ctx->is_context( type::worker_context) || ctx->is_context( type::main_context) );
    BOOST_ASSERT( ! ctx->ready_is_linked() );
#if ! defined(BOOST_FIBERS_NO_ATOMICS)
    BOOST_ASSERT( ! ctx->remote_ready_is_linked() );
#endif
    BOOST_ASSERT( ! ctx->terminated_is_linked() );
    // resume another fiber
    algo_->pick_next()->resume( ctx);
}

void
scheduler::suspend() noexcept {
    // resume another context
    algo_->pick_next()->resume();
}

void
scheduler::suspend( detail::spinlock_lock & lk) noexcept {
    // resume another context
    algo_->pick_next()->resume( lk);
}

bool
scheduler::has_ready_fibers() const noexcept {
    return algo_->has_ready_fibers();
}

void
scheduler::set_algo( algo::algorithm::ptr_t algo) noexcept {
    // move remaining context in current scheduler to new one
    while ( algo_->has_ready_fibers() ) {
        algo->awakened( algo_->pick_next() );
    }
    algo_ = std::move( algo);
}

void
scheduler::attach_main_context( context * ctx) noexcept {
    BOOST_ASSERT( nullptr != ctx);
    // main-context represents the execution context created
    // by the system, e.g. main()- or thread-context
    // should not be in worker-queue
    main_ctx_ = ctx;
    main_ctx_->scheduler_ = this;
}

void
scheduler::attach_dispatcher_context( intrusive_ptr< context > ctx) noexcept {
    BOOST_ASSERT( ctx);
    // dispatcher context has to handle
    //    - remote ready context'
    //    - extern event-loops
    //    - suspending the thread if ready-queue is empty (waiting on external event)
    // should not be in worker-queue
    dispatcher_ctx_.swap( ctx);
    // add dispatcher-context to ready-queue
    // so it is the first element in the ready-queue
    // if the main context tries to suspend the first time
    // the dispatcher-context is resumed and
    // scheduler::dispatch() is executed
    dispatcher_ctx_->scheduler_ = this;
    algo_->awakened( dispatcher_ctx_.get() );
}

void
scheduler::attach_worker_context( context * ctx) noexcept {
    BOOST_ASSERT( nullptr != ctx);
    BOOST_ASSERT( nullptr == ctx->get_scheduler() );
    BOOST_ASSERT( ! ctx->ready_is_linked() );
#if ! defined(BOOST_FIBERS_NO_ATOMICS)
    BOOST_ASSERT( ! ctx->remote_ready_is_linked() );
#endif
    BOOST_ASSERT( ! ctx->terminated_is_linked() );
    BOOST_ASSERT( ! ctx->worker_is_linked() );
    ctx->worker_link( worker_queue_);
    ctx->scheduler_ = this;
    // an attached context must belong at least to worker-queue
}

void
scheduler::detach_worker_context( context * ctx) noexcept {
    BOOST_ASSERT( nullptr != ctx);
    BOOST_ASSERT( ! ctx->ready_is_linked() );
#if ! defined(BOOST_FIBERS_NO_ATOMICS)
    BOOST_ASSERT( ! ctx->remote_ready_is_linked() );
#endif
    BOOST_ASSERT( ! ctx->terminated_is_linked() );
    BOOST_ASSERT( ctx->worker_is_linked() );
    BOOST_ASSERT( ! ctx->is_context( type::pinned_context) );
    ctx->worker_unlink();
    BOOST_ASSERT( ! ctx->worker_is_linked() );
    ctx->scheduler_ = nullptr;
    // a detached context must not belong to any queue
}

}}

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_SUFFIX
#endif
