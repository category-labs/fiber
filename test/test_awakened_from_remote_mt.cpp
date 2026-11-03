// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <atomic>
#include <chrono>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <boost/fiber/all.hpp>
#include <boost/fiber/algo/round_robin.hpp>
#include <boost/test/unit_test.hpp>

namespace {

typedef std::chrono::steady_clock clock_type;
using boost::fibers::context;

// state shared by every thread that installs shared_queue
struct shared_state {
    std::mutex                  mtx{};
    std::deque< context * >     queue{};
    // wakes taken by awakened_from_remote()
    std::atomic< int >          taken{ 0 };
    // pinned contexts offered to it, which the scheduler must never do
    std::atomic< int >          pinned{ 0 };
};

// One ready-queue shared by all threads, with the main and dispatcher
// contexts in a queue of their own. Remote wakes go straight into the shared
// queue.
class shared_queue : public boost::fibers::algo::algorithm {
private:
    shared_state                                &   state_;
    boost::fibers::scheduler::ready_queue_type      lqueue_{};
    std::promise< void >                        *   parked_;

protected:
    void publish_remote( context * ctx) noexcept {
        ctx->detach();
        std::unique_lock< std::mutex > lk{ state_.mtx };
        state_.queue.push_back( ctx);
        ++state_.taken;
    }

public:
    explicit shared_queue( shared_state & state, std::promise< void > * parked = nullptr) :
        state_( state), parked_( parked) {
    }

    void awakened( context * ctx) noexcept override {
        if ( ctx->is_context( boost::fibers::type::pinned_context) ) {
            lqueue_.push_back( * ctx);
        } else {
            ctx->detach();
            std::unique_lock< std::mutex > lk{ state_.mtx };
            state_.queue.push_back( ctx);
        }
    }

    bool awakened_from_remote( context * ctx) noexcept override {
        if ( ctx->is_context( boost::fibers::type::pinned_context) ) {
            ++state_.pinned;
            return false;
        }
        publish_remote( ctx);
        return true;
    }

    context * pick_next() noexcept override {
        context * ctx = nullptr;
        {
            std::unique_lock< std::mutex > lk{ state_.mtx };
            if ( ! state_.queue.empty() ) {
                ctx = state_.queue.front();
                state_.queue.pop_front();
            }
        }
        if ( nullptr != ctx) {
            context::active()->attach( ctx);
        } else if ( ! lqueue_.empty() ) {
            ctx = & lqueue_.front();
            lqueue_.pop_front();
        }
        return ctx;
    }

    bool has_ready_fibers() const noexcept override {
        std::unique_lock< std::mutex > lk{ state_.mtx };
        return ! state_.queue.empty() || ! lqueue_.empty();
    }

    // threads poll the shared queue rather than sleep, so there is nothing
    // for notify() to wake
    void suspend() noexcept override {
        if ( nullptr != parked_) {
            parked_->set_value();
            parked_ = nullptr;
        }
        std::this_thread::yield();
    }

    void notify() noexcept override {
    }
};

}

// A fiber woken from a thread outside the pool is taken by the hook and
// resumes on another pool thread while its own thread is busy. Through the
// remote ready-queue it would wait for its own thread.
void test_taken_wake_resumes_elsewhere() {
    shared_state state;
    boost::fibers::promise< void > wake;
    std::atomic< bool > spinning{ false };
    std::atomic< int > resumed{ 0 };
    bool owner_timed_out = false;
    std::thread::id owner_id;
    std::thread::id resumed_on;

    std::thread owner( [&] {
        boost::fibers::use_scheduling_algorithm< shared_queue >( state);
        owner_id = std::this_thread::get_id();
        boost::fibers::future< void > woken = wake.get_future();
        // runs first and blocks, so this thread owns its context
        boost::fibers::fiber sleeper( [&] {
            woken.get();
            resumed_on = std::this_thread::get_id();
            ++resumed;
        });
        // then keeps this thread from switching fibers until the sleeper has
        // run elsewhere
        boost::fibers::fiber spinner( [&] {
            spinning = true;
            clock_type::time_point const deadline =
                clock_type::now() + std::chrono::seconds( 10);
            while ( 0 == resumed && clock_type::now() < deadline) {
            }
            owner_timed_out = 0 == resumed;
        });
        sleeper.join();
        spinner.join();
    });

    clock_type::time_point const deadline =
        clock_type::now() + std::chrono::seconds( 10);
    while ( ! spinning && clock_type::now() < deadline) {
        std::this_thread::yield();
    }
    // only now can another thread take fibers from the shared queue
    std::thread helper( [&] {
        boost::fibers::use_scheduling_algorithm< shared_queue >( state);
        clock_type::time_point const until =
            clock_type::now() + std::chrono::seconds( 10);
        while ( 0 == resumed && clock_type::now() < until) {
            boost::this_fiber::yield();
        }
    });
    wake.set_value();
    owner.join();
    helper.join();

    BOOST_CHECK( ! owner_timed_out);
    BOOST_CHECK_EQUAL( 1, resumed.load() );
    BOOST_CHECK( resumed_on != owner_id);
    BOOST_CHECK_EQUAL( 1, state.taken.load() );
    BOOST_CHECK_EQUAL( 0, state.pinned.load() );
}

// A pinned context, here a thread's main context, can only run on its own
// thread, so it never reaches the hook and resumes through the remote
// ready-queue.
void test_pinned_wake_stays_on_its_thread() {
    shared_state state;
    boost::fibers::promise< int > value;
    std::promise< void > parked;
    int result = 0;

    std::thread owner( [&] {
        boost::fibers::use_scheduling_algorithm< shared_queue >( state, & parked);
        boost::fibers::future< int > f = value.get_future();
        result = f.get();
    });
    // The dispatcher is idle, so the pinned main context is suspended in get().
    parked.get_future().wait();
    value.set_value( 42);
    owner.join();

    BOOST_CHECK_EQUAL( 42, result);
    BOOST_CHECK_EQUAL( 0, state.taken.load() );
    BOOST_CHECK_EQUAL( 0, state.pinned.load() );
}

#if ! defined(BOOST_FIBERS_NO_ATOMICS)
void test_suspended_lock_can_be_relocked() {
    boost::fibers::detail::spinlock mutex;
    boost::fibers::wait_queue waiters;
    boost::fibers::fiber notifier( [&] {
        boost::fibers::detail::spinlock_lock lock{ mutex };
        waiters.notify_one();
    });
    boost::fibers::detail::spinlock_lock lock{ mutex };
    waiters.suspend_and_wait( lock, context::active());
    BOOST_CHECK( ! lock.owns_lock() );
    BOOST_CHECK( lock.mutex() == & mutex);
    BOOST_CHECK_NO_THROW( lock.lock() );
    notifier.join();
}
#endif

// Fiber pairs on several threads pass a token back and forth while
// short-lived fibers start and finish, so remote detaches race attach and
// terminate on every thread. Meant to be run under ThreadSanitizer as well.
void test_remote_wakes_race_attach_and_terminate() {
    int const threads = 4;
    int const pairs = 16;
    int const rounds = 1000;
    struct channel_pair {
        boost::fibers::buffered_channel< int > to{ 2 };
        boost::fibers::buffered_channel< int > from{ 2 };
    };
    shared_state state;
    std::vector< std::unique_ptr< channel_pair > > channels;
    for ( int i = 0; i < pairs; ++i) {
        channels.emplace_back( new channel_pair() );
    }
    std::atomic< int > finished{ 0 };
    std::atomic< int > bad_tokens{ 0 };
    std::atomic< int > receivers_ready{ 0 };

    std::vector< std::thread > pool;
    for ( int t = 0; t < threads; ++t) {
        pool.emplace_back( [&, t] {
            boost::fibers::use_scheduling_algorithm< shared_queue >( state);
            std::vector< boost::fibers::fiber > fibers;
            for ( int i = t; i < pairs; i += threads) {
                channel_pair & ch = * channels[ i];
                fibers.emplace_back( boost::fibers::launch::dispatch, [&ch, &finished, rounds] {
                    for ( int r = 0; r < rounds; ++r) {
                        int v = -1;
                        ch.to.pop( v);
                        ch.from.push( v);
                    }
                    ++finished;
                });
            }
            // Every receiver is parked on its creating thread. Dispatch the
            // matching sender on its neighbor, so the first wake is remote
            // even if the whole test runs on one CPU.
            ++receivers_ready;
            while ( receivers_ready != threads) {
                std::this_thread::yield();
            }
            for ( int i = ( t + 1) % threads; i < pairs; i += threads) {
                channel_pair & ch = * channels[ i];
                fibers.emplace_back( boost::fibers::launch::dispatch, [&ch, &finished, &bad_tokens, rounds] {
                    for ( int r = 0; r < rounds; ++r) {
                        ch.to.push( r);
                        int v = -1;
                        ch.from.pop( v);
                        if ( v != r) {
                            ++bad_tokens;
                        }
                        if ( 0 == r % 50) {
                            // starts and terminates on whichever thread
                            // picks it up
                            boost::fibers::fiber( [] {}).join();
                        }
                    }
                    ++finished;
                });
            }
            // keep running fibers from the shared queue until every pair is
            // done
            while ( finished < 2 * pairs) {
                boost::this_fiber::yield();
            }
            for ( boost::fibers::fiber & f : fibers) {
                f.join();
            }
        });
    }
    for ( std::thread & t : pool) {
        t.join();
    }

    BOOST_CHECK_EQUAL( 2 * pairs, finished.load() );
    BOOST_CHECK_EQUAL( 0, bad_tokens.load() );
    BOOST_CHECK_GE( state.taken.load(), pairs);
    BOOST_CHECK_EQUAL( 0, state.pinned.load() );
}

void test_dummy() {}

boost::unit_test_framework::test_suite* init_unit_test_suite(int, char*[]) {
    boost::unit_test_framework::test_suite* test =
        BOOST_TEST_SUITE("Boost.Fiber: awakened_from_remote test suite");

#if ! defined(BOOST_FIBERS_NO_ATOMICS)
    test->add(BOOST_TEST_CASE(test_taken_wake_resumes_elsewhere));
    test->add(BOOST_TEST_CASE(test_pinned_wake_stays_on_its_thread));
    test->add(BOOST_TEST_CASE(test_suspended_lock_can_be_relocked));
    test->add(BOOST_TEST_CASE(test_remote_wakes_race_attach_and_terminate));
#else
    test->add(BOOST_TEST_CASE(test_dummy));
#endif

    return test;
}
