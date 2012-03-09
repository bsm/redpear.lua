require "spec.helper"

context("SortedSet", function()

  before(function()
    local params = { host = "127.0.0.1", port = 6379 }
    local redis  = Redis.connect(params)

    redis:select(9) -- for testing purposes
    redis:flushdb()
    redis:zadd("key1", 100, "1")
    redis:zadd("key1", 200, "2")
    redis:zadd("key2", 1000, "1")
    redis:zadd("key2", 3000, "3")

    set = redpear.store.SortedSet:new("key1", redis)
  end)

  test("is correctly setup", function()
    assert_equal("redpear.store.SortedSet", redpear.store.SortedSet.name)
  end)

  test("is a Base", function()
    assert_equal(redpear.store.Base, redpear.store.SortedSet.super)
  end)

  test("counts members", function()
    assert_equal(2, set:length())
  end)

  test("counts members within range", function()
    assert_equal(2, set:count("-inf", "+inf"))
    assert_equal(2, set:count(0, 1000))
    assert_equal(1, set:count(50, 150))
    assert_equal(1, set:count(150,250))
    assert_equal(0, set:count(250,350))
  end)

  test("adds members", function()
    assert_equal(2, set:length())
    set:add("3", 300)
    set:add("4", 400)
    assert_equal(4, set:length())
    assert_equal(300, set:score(3))
  end)

  test("deletes members", function()
    assert_equal(2, set:length())
    set:delete(2)
    assert_equal(1, set:length())
  end)

  test("scores members", function()
    assert_equal(nil, set:score(3))
    assert_equal(200, set:score(2))
  end)

  test("left index", function()
    assert_equal(0, set:index(1))
    assert_equal(1, set:index(2))
    assert_equal(nil, set:index(3))
  end)

  test("right index", function()
    assert_equal(1, set:rindex(1))
    assert_equal(0, set:rindex(2))
    assert_equal(nil, set:index(3))
  end)

  test("inclusion", function()
    assert_equal(true, set:included(1))
    assert_equal(false, set:included(3))
  end)

  test("emptiness", function()
    assert_false(set:empty())
    set:delete(1)
    set:delete(2)
    assert_true(set:empty())
  end)

  test("left slice", function()
    assert_tables({"1"}, set:slice(0, 0))
    assert_tables({"1", "2"}, set:slice(0, 1))
    assert_tables({}, set:slice(3, 5))
    assert_tables({["2"] = 200}, set:slice(1, 2, "withscores"))
    assert_tables({["1"] = 100, ["2"] = 200}, set:slice(0, 2, "withscores"))
    assert_equal(200, set:slice(1, 2, "withscores")["2"])
  end)

  test("right slice", function()
    assert_tables({"2"}, set:rslice(0, 0))
    assert_tables({"2", "1"}, set:rslice(0, 1))
    assert_tables({}, set:rslice(3, 5))
    assert_tables({["1"] = 100}, set:rslice(1, 2, "withscores"))
    assert_tables({["2"] = 200, ["1"] = 100}, set:rslice(0, 2, "withscores"))
    assert_equal(100, set:rslice(1, 2, "withscores")["1"])
  end)

  test("left select", function()
    assert_tables({"1"}, set:select(50, 150))
    assert_tables({"1", "2"}, set:select(50, 250))
    assert_tables({}, set:select(300, 500))
    assert_tables({["2"] = 200}, set:select(150, 250, "withscores"))
    assert_tables({["1"] = 100, ["2"] = 200}, set:select(50, 250, "withscores"))
    assert_equal(200, set:select(150, 250, "withscores")["2"])
  end)

  test("right select", function()
    assert_tables({"1"}, set:rselect(150, 50))
    assert_tables({"2", "1"}, set:rselect(250, 50))
    assert_tables({}, set:rselect(500, 300))
    assert_tables({["2"] = 200}, set:rselect(250, 150, "withscores"))
    assert_tables({["2"] = 200, ["1"] = 100}, set:rselect(250, 50, "withscores"))
    assert_equal(200, set:rselect(250, 150, "withscores")["2"])
  end)

  test("members at index", function()
    assert_tables("1", set:at(0))
    assert_tables("2", set:at(1))
    assert_tables(nil, set:at(2))
  end)

  test("first member", function()
    assert_tables("1", set:first())
  end)

  test("last member", function()
    assert_tables("2", set:last())
  end)

  test('creates and stores intersections', function()
    local set = set:interstore('key3', 1, 'key2')
    assert(instanceOf(redpear.store.SortedSet, set))
    assert_equal('key3', set.key)

    local tab = set:slice(0, -1, 'withscores')
    assert_tables({["1"] = 1100}, tab)
  end)

  test('creates and stores unions', function()
    local set = set:unionstore('key3', 1, 'key2')
    assert(instanceOf(redpear.store.SortedSet, set))
    assert_equal('key3', set.key)

    local tab = set:slice(0, -1, 'withscores')
    assert_tables({["1"] = 1100, ["2"] = 200, ["3"] = 3000}, tab)
  end)

end)
