require "spec.helper"

context('redpear.store.sorted_set', function()
  local redis = require('redis'):new()
  local klass = require("redpear.store").sorted_set
  local subject

  before(function()
    redis:connect('127.0.0.1', 6379)
    redis:select(9)  -- for testing purposes
    redis:zadd("key1", 100, "1")
    redis:zadd("key1", 200, "2")
    redis:zadd("key2", 1000, "1")
    redis:zadd("key2", 3000, "3")

    subject = klass:new('key1', redis)
  end)

  after(function()
    redis:flushdb()
  end)

  test('is a base', function()
    assert_true(subject:exists())
  end)

  test("counts members", function()
    assert_equal(subject:length(), 2)
  end)

  test("counts members within range", function()
    assert_equal(subject:count("-inf", "+inf"), 2)
    assert_equal(subject:count(0, 1000), 2)
    assert_equal(subject:count(50, 150), 1)
    assert_equal(subject:count(150,250), 1)
    assert_equal(subject:count(250,350), 0)
  end)

  test("adds members", function()
    assert_equal(subject:length(), 2)
    subject:add("3", 300)
    subject:add("4", 400)
    assert_equal(subject:length(), 4)
    assert_equal(subject:score(3), 300)
  end)

  test("deletes members", function()
    assert_equal(subject:length(), 2)
    subject:delete(2)
    assert_equal(subject:length(), 1)
  end)

  test("scores members", function()
    assert_equal(subject:score(3), null)
    assert_equal(subject:score(2), 200)
  end)

  test("left index", function()
    assert_equal(subject:index(1), 0)
    assert_equal(subject:index(2), 1)
    assert_equal(subject:index(3), null)
  end)

  test("right index", function()
    assert_equal(subject:rindex(1), 1)
    assert_equal(subject:rindex(2), 0)
    assert_equal(subject:rindex(3), null)
  end)

  test("inclusion", function()
    assert_true(subject:included(1))
    assert_false(subject:included(3))
  end)

  test("emptiness", function()
    assert_false(subject:empty())
    subject:delete(1)
    subject:delete(2)
    assert_true(subject:empty())
  end)

  test("left slice", function()
    assert_tables(subject:slice(0, 0), {"1"})
    assert_tables(subject:slice(0, 1), {"1", "2"})
    assert_tables(subject:slice(3, 5), {})
    assert_tables(subject:slice(1, 2, "withscores"), {"2", "200"})
    assert_tables(subject:slice(0, 2, "withscores"), {"1", "100", "2", "200"})
  end)

  test("right slice", function()
    assert_tables(subject:rslice(0, 0), {"2"})
    assert_tables(subject:rslice(0, 1), {"2", "1"})
    assert_tables(subject:rslice(3, 5), {})
    assert_tables(subject:rslice(1, 2, "withscores"), {"1", "100"})
    assert_tables(subject:rslice(0, 2, "withscores"), {"2", "200", "1", "100"})
  end)

  test("left select", function()
    assert_tables(subject:select(50, 150), {"1"})
    assert_tables(subject:select(50, 250), {"1", "2"})
    assert_tables(subject:select(300, 500), {})
    assert_tables(subject:select(150, 250, "withscores"), {"2", "200"})
    assert_tables(subject:select(50, 250, "withscores"), {"1", "100", "2", "200"})
  end)

  test("right select", function()
    assert_tables(subject:rselect(150, 50), {"1"})
    assert_tables(subject:rselect(250, 50), {"2", "1"})
    assert_tables(subject:rselect(500, 300), {})
    assert_tables(subject:rselect(250, 150, "withscores"), {"2", "200"})
    assert_tables(subject:rselect(250, 50, "withscores"), {"2", "200", "1", "100"})
  end)

  test("members at index", function()
    assert_equal(subject:at(0), "1")
    assert_equal(subject:at(1), "2")
    assert_equal(subject:at(2), nil)
  end)

  test("first member", function()
    assert_equal(subject:first(), "1")
  end)

  test("last member", function()
    assert_equal(subject:last(), "2")
  end)

  test('creates and stores intersections', function()
    local set = subject:interstore('key3', 1, 'key2')
    assert_equal(set.key, 'key3')
    assert_tables(set:slice(0, -1, 'withscores'), {"1", "1100"})
  end)

  test('creates and stores unions', function()
    local set = subject:unionstore('key3', 1, 'key2')
    assert_equal(set.key, 'key3')
    assert_tables(set:slice(0, -1, 'withscores'), {"2", "200", "1", "1100", "3", "3000"})
  end)

end)
