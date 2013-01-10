require 'spec.helper'

context('redpear.store.set', function()

  before(function()
    klass   = require("redpear.store").set
    subject = klass:new('key1', redis)

    redis:sadd('key1', '1')
    redis:sadd('key1', '2')
    redis:sadd('key2', '1')
    redis:sadd('key2', '3')
  end)

  test('is a base', function()
    assert_equal(subject:exists(), true)
  end)

  test('returns all members', function()
    assert_equal(subject:members()[1], '1')
    assert_equal(subject:members()[2], '2')
  end)

  test('adds members', function()
    subject:add('3')
    assert_equal(subject:members()[3], '3')
  end)

  test('counts members', function()
    assert_equal(subject:length(), 2)
    assert_equal(subject:count(), 2)
  end)

  test('deletes members', function()
    subject:delete('2')
    assert_nil(subject:members()[2])
  end)

  test('checks member inclusion', function()
    assert_true(subject:include('1'))
    assert_false(subject:include('other'))
  end)

  test('pops random member', function()
    local e = subject:pop()

    assert_true(e == '1' or e == '2')
    assert_equal(subject:length(), 1)
  end)

  test('returns random member', function()
    local e = subject:random()

    assert_true(e == '1' or e == '2')
    assert_equal(subject:length(), 2)
  end)

  test('creates intersections', function()
    local tab = subject:inter('key2')
    assert_tables(tab, {'1'})
  end)

  test('creates unions', function()
    local tab = subject:union('key2')
    assert_tables(tab, {'1', '2', '3'})
  end)

  test('creates diffs', function()
    local tab = subject:diff('key2')
    assert_tables(tab, {'2'})
  end)

  test('creates and stores intersections', function()
    local set = subject:interstore('key3', 'key2')
    assert_equal(set.key, 'key3')
    assert_tables(set:members(), {'1'})
  end)

  test('creates and stores unions', function()
    local set = subject:unionstore('key3', 'key2')
    assert_equal(set.key, 'key3')
    assert_tables(set:members(), {'1', '2', '3'})
  end)

  test('creates and stores diffs', function()
    local set = subject:diffstore('key3', 'key2')
    assert_equal(set.key, 'key3')
    assert_tables(set:members(), {'2'})
  end)

end)
