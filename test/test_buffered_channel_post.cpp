
//          Copyright Oliver Kowalke 2013.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <atomic>
#include <chrono>
#include <sstream>
#include <string>
#include <vector>

#include <boost/assert.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/parameterized_test.hpp>

#include <boost/fiber/all.hpp>

struct moveable {
    bool    state;
    int     value;

    moveable() :
        state( false),
        value( -1) {
    }

    moveable( int v) :
        state( true),
        value( v) {
    }

    moveable( moveable && other) :
        state( other.state),
        value( other.value) {
        other.state = false;
        other.value = -1;
    }

    moveable & operator=( moveable && other) {
        if ( this == & other) return * this;
        state = other.state;
        other.state = false;
        value = other.value;
        other.value = -1;
        return * this;
    }
};

void test_zero_wm() {
    bool thrown = false;
    try {
        boost::fibers::buffered_channel< int > c( 0);
    } catch ( boost::fibers::fiber_error const&) {
        thrown = true;
    }
    BOOST_CHECK( thrown);
}

void test_push(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( 1) );
}

void test_push_closed(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    c.close();
    BOOST_CHECK( boost::fibers::channel_op_status::closed == c.push( 1) );
}

void test_try_push(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( 1) );
}

void test_try_push_closed(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.try_push( 1) );
    c.close();
    BOOST_CHECK( boost::fibers::channel_op_status::closed == c.try_push( 2) );
}

void test_try_push_full(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    for (int i = 1; i != static_cast<int>( channel_size); ++i) {
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.try_push( i) );
    }
    BOOST_CHECK( boost::fibers::channel_op_status::full == c.try_push( channel_size) );
}

void test_push_wait_for(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.push_wait_for( 1, std::chrono::seconds( 1) ) );
}

void test_push_wait_for_closed(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    c.close();
    BOOST_CHECK( boost::fibers::channel_op_status::closed == c.push_wait_for( 1, std::chrono::seconds( 1) ) );
}

void test_push_wait_for_timeout(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    for (int i = 1; i != static_cast<int>( channel_size); ++i) {
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push_wait_for( i, std::chrono::seconds( 1) ) );
    }
    BOOST_CHECK( boost::fibers::channel_op_status::timeout == c.push_wait_for( channel_size, std::chrono::seconds( 1) ) );
}

void test_push_wait_until(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.push_wait_until( 1,
                    std::chrono::system_clock::now() + std::chrono::seconds( 1) ) );
}

void test_push_wait_until_closed(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    c.close();
    BOOST_CHECK( boost::fibers::channel_op_status::closed == c.push_wait_until( 1,
                    std::chrono::system_clock::now() + std::chrono::seconds( 1) ) );
}

void test_push_wait_until_timeout(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    for (int i = 1; i != static_cast<int>( channel_size); ++i) {
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push_wait_until( i,
                    std::chrono::system_clock::now() + std::chrono::seconds( 1) ) );
    }
    BOOST_CHECK( boost::fibers::channel_op_status::timeout == c.push_wait_until( channel_size,
                    std::chrono::system_clock::now() + std::chrono::seconds( 1) ) );
}

void test_pop(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v1 = 2, v2 = 0;
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( v1) );
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.pop( v2) );
    BOOST_CHECK_EQUAL( v1, v2);
}

void test_pop_closed(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v1 = 2, v2 = 0;
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( v1) );
    c.close();
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.pop( v2) );
    BOOST_CHECK_EQUAL( v1, v2);
    BOOST_CHECK( boost::fibers::channel_op_status::closed == c.pop( v2) );
}

void test_pop_success(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v1 = 2, v2 = 0;
    boost::fibers::fiber f1( boost::fibers::launch::post, [&c,&v2](){
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.pop( v2) );
    });
    boost::fibers::fiber f2( boost::fibers::launch::post, [&c,v1](){
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( v1) );
    });
    f1.join();
    f2.join();
    BOOST_CHECK_EQUAL( v1, v2);
}

void test_value_pop(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v1 = 2, v2 = 0;
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( v1) );
    v2 = c.value_pop();
    BOOST_CHECK_EQUAL( v1, v2);
}

void test_value_pop_closed(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v1 = 2, v2 = 0;
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( v1) );
    c.close();
    v2 = c.value_pop();
    BOOST_CHECK_EQUAL( v1, v2);
    bool thrown = false;
    try {
        c.value_pop();
    } catch ( boost::fibers::fiber_error const&) {
        thrown = true;
    }
    BOOST_CHECK( thrown);
}

void test_value_pop_success(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v1 = 2, v2 = 0;
    boost::fibers::fiber f1( boost::fibers::launch::post, [&c,&v2](){
        v2 = c.value_pop();
    });
    boost::fibers::fiber f2( boost::fibers::launch::post, [&c,v1](){
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( v1) );
    });
    f1.join();
    f2.join();
    BOOST_CHECK_EQUAL( v1, v2);
}

void test_try_pop(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v1 = 2, v2 = 0;
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( v1) );
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.try_pop( v2) );
    BOOST_CHECK_EQUAL( v1, v2);
}

void test_try_pop_closed(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v1 = 2, v2 = 0;
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( v1) );
    c.close();
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.try_pop( v2) );
    BOOST_CHECK_EQUAL( v1, v2);
    BOOST_CHECK( boost::fibers::channel_op_status::closed == c.try_pop( v2) );
}

void test_try_pop_success(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v1 = 2, v2 = 0;
    boost::fibers::fiber f1( boost::fibers::launch::post, [&c,&v2](){
        while ( boost::fibers::channel_op_status::success != c.try_pop( v2) ) {
            boost::this_fiber::yield();
        }
    });
    boost::fibers::fiber f2( boost::fibers::launch::post, [&c,v1](){
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( v1) );
    });
    f1.join();
    f2.join();
    BOOST_CHECK_EQUAL( v1, v2);
}

void test_pop_wait_for(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v1 = 2, v2 = 0;
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( v1) );
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.pop_wait_for( v2, std::chrono::seconds( 1) ) );
    BOOST_CHECK_EQUAL( v1, v2);
}

void test_pop_wait_for_closed(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v1 = 2, v2 = 0;
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( v1) );
    c.close();
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.pop_wait_for( v2, std::chrono::seconds( 1) ) );
    BOOST_CHECK_EQUAL( v1, v2);
    BOOST_CHECK( boost::fibers::channel_op_status::closed == c.pop_wait_for( v2, std::chrono::seconds( 1) ) );
}

void test_pop_wait_for_success(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v1 = 2, v2 = 0;
    boost::fibers::fiber f1( boost::fibers::launch::post, [&c,&v2](){
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.pop_wait_for( v2, std::chrono::seconds( 1) ) );
    });
    boost::fibers::fiber f2( boost::fibers::launch::post, [&c,v1](){
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( v1) );
    });
    f1.join();
    f2.join();
    BOOST_CHECK_EQUAL( v1, v2);
}

void test_pop_wait_for_timeout(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v = 0;
    boost::fibers::fiber f( boost::fibers::launch::post, [&c,&v](){
        BOOST_CHECK( boost::fibers::channel_op_status::timeout == c.pop_wait_for( v, std::chrono::seconds( 1) ) );
    });
    f.join();
}

void test_pop_wait_until(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v1 = 2, v2 = 0;
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( v1) );
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.pop_wait_until( v2,
            std::chrono::system_clock::now() + std::chrono::seconds( 1) ) );
    BOOST_CHECK_EQUAL( v1, v2);
}

void test_pop_wait_until_closed(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v1 = 2, v2 = 0;
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( v1) );
    c.close();
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.pop_wait_until( v2,
            std::chrono::system_clock::now() + std::chrono::seconds( 1) ) );
    BOOST_CHECK_EQUAL( v1, v2);
    BOOST_CHECK( boost::fibers::channel_op_status::closed == c.pop_wait_until( v2,
            std::chrono::system_clock::now() + std::chrono::seconds( 1) ) );
}

void test_pop_wait_until_success(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v1 = 2, v2 = 0;
    boost::fibers::fiber f1( boost::fibers::launch::post, [&c,&v2](){
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.pop_wait_until( v2,
                    std::chrono::system_clock::now() + std::chrono::seconds( 1) ) );
    });
    boost::fibers::fiber f2( boost::fibers::launch::post, [&c,v1](){
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( v1) );
    });
    f1.join();
    f2.join();
    BOOST_CHECK_EQUAL( v1, v2);
}

void test_pop_wait_until_timeout(size_t channel_size) {
    boost::fibers::buffered_channel< int > c ( channel_size);
    int v = 0;
    boost::fibers::fiber f( boost::fibers::launch::post, [&c,&v](){
        BOOST_CHECK( boost::fibers::channel_op_status::timeout == c.pop_wait_until( v,
                    std::chrono::system_clock::now() + std::chrono::seconds( 1) ) );
    });
    f.join();
}

void test_wm_1() {
    boost::fibers::buffered_channel< int > c( 4);
    std::vector< boost::fibers::fiber::id > ids;
    boost::fibers::fiber f1( boost::fibers::launch::post, [&c,&ids](){
        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( 1) );

        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( 2) );

        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( 3) );

        ids.push_back( boost::this_fiber::get_id() );
        // would be blocked because channel is full
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( 4) );

        ids.push_back( boost::this_fiber::get_id() );
        // would be blocked because channel is full
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( 5) );

        ids.push_back( boost::this_fiber::get_id() );
    });
    boost::fibers::fiber f2( boost::fibers::launch::post, [&c,&ids](){
        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK_EQUAL( 1, c.value_pop() );

        // let other fiber run
        boost::this_fiber::yield();

        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK_EQUAL( 2, c.value_pop() );

        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK_EQUAL( 3, c.value_pop() );

        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK_EQUAL( 4, c.value_pop() );

        ids.push_back( boost::this_fiber::get_id() );
        // would block because channel is empty
        BOOST_CHECK_EQUAL( 5, c.value_pop() );

        ids.push_back( boost::this_fiber::get_id() );
    });
    boost::fibers::fiber::id id1 = f1.get_id();
    boost::fibers::fiber::id id2 = f2.get_id();
    f1.join();
    f2.join();
    BOOST_CHECK_EQUAL( (std::size_t)12, ids.size() );
    BOOST_CHECK_EQUAL( id1, ids[0]);
    BOOST_CHECK_EQUAL( id1, ids[1]);
    BOOST_CHECK_EQUAL( id1, ids[2]);
    BOOST_CHECK_EQUAL( id1, ids[3]);
    BOOST_CHECK_EQUAL( id2, ids[4]);
    BOOST_CHECK_EQUAL( id1, ids[5]);
    BOOST_CHECK_EQUAL( id2, ids[6]);
    BOOST_CHECK_EQUAL( id2, ids[7]);
    BOOST_CHECK_EQUAL( id2, ids[8]);
    BOOST_CHECK_EQUAL( id2, ids[9]);
    BOOST_CHECK_EQUAL( id1, ids[10]);
    BOOST_CHECK_EQUAL( id2, ids[11]);
}

void test_wm_2() {
    boost::fibers::buffered_channel< int > c( 8);
    std::vector< boost::fibers::fiber::id > ids;
    boost::fibers::fiber f1( boost::fibers::launch::post, [&c,&ids](){
        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( 1) );

        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( 2) );

        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( 3) );

        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( 4) );

        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( 5) );

        ids.push_back( boost::this_fiber::get_id() );
    });
    boost::fibers::fiber f2( boost::fibers::launch::post, [&c,&ids](){
        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK_EQUAL( 1, c.value_pop() );

        // let other fiber run
        boost::this_fiber::yield();

        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK_EQUAL( 2, c.value_pop() );

        // let other fiber run
        boost::this_fiber::yield();

        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK_EQUAL( 3, c.value_pop() );

        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK_EQUAL( 4, c.value_pop() );

        ids.push_back( boost::this_fiber::get_id() );
        BOOST_CHECK_EQUAL( 5, c.value_pop() );

        ids.push_back( boost::this_fiber::get_id() );
    });
    boost::fibers::fiber::id id1 = f1.get_id();
    boost::fibers::fiber::id id2 = f2.get_id();
    f1.join();
    f2.join();
    BOOST_CHECK_EQUAL( (std::size_t)12, ids.size() );
    BOOST_CHECK_EQUAL( id1, ids[0]);
    BOOST_CHECK_EQUAL( id1, ids[1]);
    BOOST_CHECK_EQUAL( id1, ids[2]);
    BOOST_CHECK_EQUAL( id1, ids[3]);
    BOOST_CHECK_EQUAL( id1, ids[4]);
    BOOST_CHECK_EQUAL( id1, ids[5]);
    BOOST_CHECK_EQUAL( id2, ids[6]);
    BOOST_CHECK_EQUAL( id2, ids[7]);
    BOOST_CHECK_EQUAL( id2, ids[8]);
    BOOST_CHECK_EQUAL( id2, ids[9]);
    BOOST_CHECK_EQUAL( id2, ids[10]);
    BOOST_CHECK_EQUAL( id2, ids[11]);
}

void test_moveable(size_t channel_size) {
    boost::fibers::buffered_channel< moveable > c ( channel_size);
    moveable m1( 3), m2;
    BOOST_CHECK( m1.state);
    BOOST_CHECK_EQUAL( 3, m1.value);
    BOOST_CHECK( ! m2.state);
    BOOST_CHECK_EQUAL( -1, m2.value);
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.push( std::move( m1) ) );
    BOOST_CHECK( ! m1.state);
    BOOST_CHECK( ! m2.state);
    BOOST_CHECK( boost::fibers::channel_op_status::success == c.pop( m2) );
    BOOST_CHECK( ! m1.state);
    BOOST_CHECK_EQUAL( -1, m1.value);
    BOOST_CHECK( m2.state);
    BOOST_CHECK_EQUAL( 3, m2.value);
}

void test_rangefor(size_t channel_size) {
    boost::fibers::buffered_channel< int > chan{ 2 };
    std::vector< int > vec;
    boost::fibers::fiber f1([&chan]{
        chan.push( 1);
        chan.push( 1);
        chan.push( 2);
        chan.push( 3);
        chan.push( 5);
        chan.push( 8);
        chan.push( 12);
        chan.close();
    });
    boost::fibers::fiber f2([&vec,&chan]{
        for ( int value : chan) {
            vec.push_back( value);
        }
    });
    f1.join();
    f2.join();
    BOOST_CHECK_EQUAL( 1, vec[0]);
    BOOST_CHECK_EQUAL( 1, vec[1]);
    BOOST_CHECK_EQUAL( 2, vec[2]);
    BOOST_CHECK_EQUAL( 3, vec[3]);
    BOOST_CHECK_EQUAL( 5, vec[4]);
    BOOST_CHECK_EQUAL( 8, vec[5]);
    BOOST_CHECK_EQUAL( 12, vec[6]);
}

void test_rangefor_non_trivial_dtor() {
    {
        boost::fibers::buffered_channel< counted_type > chan{ 2 };
        std::vector< int > vec;
        boost::fibers::fiber f1([&chan]{
            chan.push( counted_type{ 1} );
            chan.push( counted_type{ 1} );
            chan.push( counted_type{ 2} );
            chan.push( counted_type{ 3} );
            chan.push( counted_type{ 5} );
            chan.push( counted_type{ 8} );
            chan.push( counted_type{ 12} );
            chan.close();
        });
        boost::fibers::fiber f2([&vec,&chan]{
            for ( counted_type val : chan) {
                vec.push_back( val.value);
            }
        });
        f1.join();
        f2.join();
        BOOST_CHECK_EQUAL( 1, vec[0]);
        BOOST_CHECK_EQUAL( 1, vec[1]);
        BOOST_CHECK_EQUAL( 2, vec[2]);
        BOOST_CHECK_EQUAL( 3, vec[3]);
        BOOST_CHECK_EQUAL( 5, vec[4]);
        BOOST_CHECK_EQUAL( 8, vec[5]);
        BOOST_CHECK_EQUAL( 12, vec[6]);
    }
    BOOST_CHECK_EQUAL( (std::size_t)0, counted_type::count);
}

boost::unit_test::test_suite * init_unit_test_suite( int, char* []) {
    boost::unit_test::test_suite * test =
        BOOST_TEST_SUITE("Boost.Fiber: buffered_channel test suite");

    auto size_2 = { 2, 3 };
    auto size_16 = { 16, 17 };

     test->add( BOOST_TEST_CASE( & test_zero_wm) );
     test->add( BOOST_PARAM_TEST_CASE( & test_push, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_push_closed, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_try_push, size_2.begin(), size_2.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_try_push_closed, size_2.begin(), size_2.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_try_push_full, size_2.begin(), size_2.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_push_wait_for, size_2.begin(), size_2.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_push_wait_for_closed, size_2.begin(), size_2.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_push_wait_for_timeout, size_2.begin(), size_2.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_push_wait_until, size_2.begin(), size_2.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_push_wait_until_closed, size_2.begin(), size_2.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_push_wait_until_timeout, size_2.begin(), size_2.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_pop, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_pop_closed, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_pop_success, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_value_pop, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_value_pop_closed, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_value_pop_success, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_try_pop, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_try_pop_closed, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_try_pop_success, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_pop_wait_for, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_pop_wait_for_closed, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_pop_wait_for_success, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_pop_wait_for_timeout, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_pop_wait_until, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_pop_wait_until_closed, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_pop_wait_until_success, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_pop_wait_until_timeout, size_16.begin(), size_16.end()) );
     test->add( BOOST_TEST_CASE( & test_wm_1) );
     test->add( BOOST_TEST_CASE( & test_wm_2) );
     test->add( BOOST_PARAM_TEST_CASE( & test_moveable, size_16.begin(), size_16.end()) );
     test->add( BOOST_PARAM_TEST_CASE( & test_rangefor, size_2.begin(), size_2.end()) );

    return test;
}
