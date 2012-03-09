require 'spec.helper'

context('Set', function()

  before(function()
    local params = { host = '127.0.0.1', port = 6379 }
    local redis  = Redis.connect(params)

    redis:select(9) -- for testing purposes
    redis:flushdb()
    redis:sadd('key1', '1')
    redis:sadd('key1', '2')
    redis:sadd('key2', '1')
    redis:sadd('key2', '3')

    set = redpear.store.Set:new('key1', redis)
  end)

  test('is correctly setup', function()
    assert_equal('redpear.store.Set', redpear.store.Set.name)
  end)

  test('is a Store.Base', function()
    assert_equal(redpear.store.Base, redpear.store.Set.super)
  end)

  test('returns all members', function()
    assert_equal('1', set:members()[1])
    assert_equal('2', set:members()[2])
  end)

  test('adds members', function()
    set:add('3')
    assert_equal('3', set:members()[3])
  end)

  test('counts members', function()
    assert_equal(2, set:length())
  end)

  test('deletes members', function()
    set:delete('2')
    assert_nil(set:members()[2])
  end)

  test('checks member inclusion', function()
    assert_true(set:include('1'))
    assert_false(set:include('other'))
  end)

  test('pops random member', function()
    local e = set:pop()

    assert_true(e == '1' or e == '2')
    assert_equal(1, set:length())
  end)

  test('returns random member', function()
    local e = set:random()

    assert_true(e == '1' or e == '2')
    assert_equal(2, set:length())
  end)

  test('creates intersections', function()
    local tab = set:inter('key2')
    assert_tables({'1'}, tab)
  end)

  test('creates unions', function()
    local tab = set:union('key2')
    assert_tables({'1', '2', '3'}, tab)
  end)

  test('creates and stores intersections', function()
    local set = set:interstore('key3', 'key2')
    assert(instanceOf(redpear.store.Set, set))
    assert_equal('key3', set.key)

    local tab = set:members()
    assert_tables({'1'}, tab)
  end)

  test('creates and stores unions', function()
    local set = set:unionstore('key3', 'key2')
    assert(instanceOf(redpear.store.Set, set))
    assert_equal('key3', set.key)

    local tab = set:members()
    assert_tables({'1', '2', '3'}, tab)
  end)


end)
