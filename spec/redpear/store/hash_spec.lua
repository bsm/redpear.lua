require 'spec.helper'

context('redpear.store.hash', function()
  local redis = require('redis'):new()
  local klass = require("redpear.store").hash
  local subject

  before(function()
    redis:connect('127.0.0.1', 6379)
    redis:select(9)  -- for testing purposes
    redis:hmset('key1', 'f1', 10, 'f2', 20, 'f3', 30)
    redis:hmset('key2', 'f1', 10, 'f4', 40)

    subject = klass:new('key1', redis)
  end)

  after(function()
    redis:flushdb()
  end)

  test('inherits from base', function()
    assert_true(subject:exists())
  end)

  test('length', function()
    assert_equal(subject:length(), 3)
  end)

  test('all', function()
    assert_tables(subject:all(), {f1='10', f2='20', f3='30'})
  end)

  test('get', function()
    assert_equal(subject:get("f1"), '10')
    assert_tables(subject:get("f1", "f3"), {'10', '30'})
    assert_equal(subject:get("f4"), null)
    assert_tables(subject:get("f4", "f2"), {null, '20'})
  end)

  test('value', function()
    assert_tables(subject:value("f1"), '10')
  end)

  test('values_at', function()
    assert_tables(subject:values_at("f1"), {'10'})
    assert_tables(subject:values_at("f4", "f2"), {null, '20'})
    assert_tables(subject:values_at({"f4", "f2"}), {null, '20'})
  end)

  test('set', function()
    assert_false(subject:set("f1", 25))
    assert_tables(subject:all(), {f1='25', f2='20', f3='30'})

    assert_true(subject:set("f1", 25, "f4", 45))
    assert_tables(subject:all(), {f1='25', f2='20', f3='30', f4='45'})

    assert_true(subject:set({f5=50}))
    assert_tables(subject:all(), {f1='25', f2='20', f3='30', f4='45', f5='50'})
  end)

  test('empty', function()
    assert_false(subject:empty())
    assert_true(klass:new('key3', redis):empty())
  end)

  test('has_key', function()
    assert_true(subject:has_key("f1"))
    assert_false(subject:has_key("f4"))
  end)

  test('keys', function()
    assert_tables(subject:keys(), {"f1", "f2", "f3"})
  end)

  test('values', function()
    assert_tables(subject:values(), {"10", "20", "30"})
  end)

  test('delete', function()
    assert_true(subject:delete("f1"))
    assert_equal(subject:length(), 2)
    assert_false(subject:delete("f4"))
  end)

  test('increment', function()
    assert_equal(subject:increment("f1"), 11)
    assert_equal(subject:increment("f1", 4), 15)
    assert_equal(subject:increment("f0", 3), 3)
  end)

  test('decrement', function()
    assert_equal(subject:decrement("f1"), 9)
    assert_equal(subject:decrement("f1", 4), 5)
    assert_equal(subject:decrement("f0", 3), -3)
  end)

end)