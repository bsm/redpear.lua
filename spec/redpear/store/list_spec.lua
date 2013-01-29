require 'spec.helper'

context('redpear.store.list', function()

  before(function()
    klass   = require("redpear.store").list
    subject = klass:new('key1', redis)

    redis:rpush('key1', "1")
    redis:rpush('key1', "2")
    redis:rpush('key1', "3")
  end)

  test('is a base', function()
    assert_equal(subject:exists(), true)
  end)

  test('returns all ', function()
    assert_tables(subject:all(), {'1', '2', '3'})
  end)

  test('returns range', function()
    assert_tables(subject:range(1,1), {'2'})
    assert_tables(subject:range(1,2), {'2', '3'})
    assert_tables(subject:range(3,1), {})
    assert_tables(subject:range('invalid','invalid'), {})
  end)

  test('trim the list', function()
    assert_true(subject:trim(1,-1))
    assert_tables(subject:all(), {'2', '3'})
  end)

  test('return the length', function()
    assert_equal(subject:length(), 3)
  end)

  test('push an item to the list', function()
    assert_tables(subject:push('4'), {'1', '2', '3', '4'})
    assert_tables(subject:push('5'), {'1', '2', '3', '4', '5'})
  end)

  test('pop the list', function()
    assert_equal(subject:pop(), '3')
    assert_equal(subject:pop(), '2')
  end)

  test('unshift an item to the list', function()
    assert_tables(subject:unshift('4'), {'4', '1', '2', '3',})
    assert_tables(subject:unshift('5'), {'5', '4', '1', '2', '3',})
  end)

  test('shift the list', function()
    assert_equal(subject:shift(), '1')
    assert_equal(subject:shift(), '2')
  end)

  test('delete from the list', function()
    redis:rpush('key1', "1")
    redis:rpush('key1', "2")

    assert_equal(subject:delete('1'), 2)
    assert_equal(subject:delete('2', 1), 1)
    assert_tables(subject:all(), {'3', '2'})
  end)

end)