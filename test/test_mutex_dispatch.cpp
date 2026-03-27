
//          Copyright Oliver Kowalke 2013.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
//
// This test is based on the tests of Boost.Thread

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>
#include <mutex>
#include <stdexcept>
#include <vector>

#include <boost/test/unit_test.hpp>

#include <boost/fiber/all.hpp>

typedef std::chrono::nanoseconds  ns;
typedef std::chrono::milliseconds ms;

int value1 = 0;
int value2 = 0;

template< typename M >
void fn1( M & mtx) {
    typedef M mutex_type;
	typename std::unique_lock< mutex_type > lk( mtx);
	++value1;
	for ( int i = 0; i < 3; ++i)
		boost::this_fiber::yield();
}

template< typename M >
void fn2( M & mtx) {
    typedef M mutex_type;
	++value2;
	typename std::unique_lock< mutex_type > lk( mtx);
	++value2;
}

void fn17( boost::fibers::mutex & m) {
    std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();
    m.lock();
    std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
    m.unlock();
    ns d = t1 - t0 - ms(250);
    BOOST_CHECK(d < ms(2500)+ms(2000)); // within 2.5 ms
}

void fn18( boost::fibers::mutex & m) {
    std::chrono::steady_clock::time_point t0 = std::chrono::steady_clock::now();
    while (!m.try_lock()) ;
    std::chrono::steady_clock::time_point t1 = std::chrono::steady_clock::now();
    m.unlock();
    ns d = t1 - t0 - ms(250);
    BOOST_CHECK(d < ns(50000000)+ms(2000)); // within 50 ms
}

template< typename M >
struct test_lock {
    typedef M mutex_type;
    typedef typename std::unique_lock< M > lock_type;

    void operator()() {
        mutex_type mtx;

        // Test the lock's constructors.
        {
            lock_type lk(mtx, std::defer_lock);
            BOOST_CHECK(!lk);
        }
        lock_type lk(mtx);
        BOOST_CHECK(lk ? true : false);

        // Test the lock and unlock methods.
        lk.unlock();
        BOOST_CHECK(!lk);
        lk.lock();
        BOOST_CHECK(lk ? true : false);
    }
};

template< typename M >
struct test_exclusive {
    typedef M mutex_type;
    typedef typename std::unique_lock< M > lock_type;

    void operator()() {
        value1 = 0;
        value2 = 0;
        BOOST_CHECK_EQUAL( 0, value1);
        BOOST_CHECK_EQUAL( 0, value2);

        mutex_type mtx;
        boost::fibers::fiber f1( boost::fibers::launch::dispatch, & fn1< mutex_type >, std::ref( mtx) );
        boost::fibers::fiber f2( boost::fibers::launch::dispatch, & fn2< mutex_type >, std::ref( mtx) );
        BOOST_ASSERT( f1.joinable() );
        BOOST_ASSERT( f2.joinable() );

        f1.join();
        f2.join();
        BOOST_CHECK_EQUAL( 1, value1);
        BOOST_CHECK_EQUAL( 2, value2);
    }
};

void do_test_mutex() {
    test_lock< boost::fibers::mutex >()();
    test_exclusive< boost::fibers::mutex >()();

    {
        boost::fibers::mutex mtx;
        mtx.lock();
        boost::fibers::fiber f( boost::fibers::launch::dispatch, & fn17, std::ref( mtx) );
        boost::this_fiber::sleep_for( ms(250) );
        mtx.unlock();
        f.join();
    }

    {
        boost::fibers::mutex mtx;
        mtx.lock();
        boost::fibers::fiber f( boost::fibers::launch::dispatch, & fn18, std::ref( mtx) );
        boost::this_fiber::sleep_for( ms(250) );
        mtx.unlock();
        f.join();
    }
}

void test_mutex() {
    boost::fibers::fiber( boost::fibers::launch::dispatch, & do_test_mutex).join();
}

boost::unit_test::test_suite * init_unit_test_suite( int, char* []) {
    boost::unit_test::test_suite * test =
        BOOST_TEST_SUITE("Boost.Fiber: mutex test suite");

    test->add( BOOST_TEST_CASE( & test_mutex) );

	return test;
}
