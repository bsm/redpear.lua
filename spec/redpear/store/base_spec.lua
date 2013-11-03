require 'spec.helper'

context('redpear.store.base', function()
  local redis = require('redis'):new()
  local klass = require("redpear.store").base
  local subject

  before(function()
    redis:connect('127.0.0.1', 6379)
    redis:select(9)  -- for testing purposes

    subject = klass:new('key', redis)
  end)

  after(function()
    redis:flushdb()
  end)

  test('constructor accepts key and connection', function()
    assert_equal(subject.key, 'key')
    assert_equal(subject.conn, redis)
  end)

  test('can purge keys from the DB', function()
    redis:set('key', 'value')
    assert_not_nil(redis:get('key'))
    subject:purge()
    assert_equal(redis:get('key'), null)
  end)

  test('check existence', function()
    assert_false(subject:exists())
    redis:set('key', 'value')
    assert_true(subject:exists())
  end)

  context('temporary', function()

    test('creates temporary keys', function()
      local key  = nil
      local temp = klass:temporary(redis, function(store)
        key = store.key
        assert_false(store:exists())
      end)

      assert_match("temp:%w+", key)
      assert_equal(key, temp.key)
      assert_false(temp:exists())
    end)

    test('performs redis block operations', function()
      local a, b = nil, nil
      klass:temporary(redis, function(store)
        a = store:exists()
        redis:set(store.key, "VALUE")
        b = store:exists()
      end)
      assert_false(a)
      assert_true(b)
    end)

    test('removes key afterwards', function()
      local key = nil
      klass:temporary(redis, function(store)
        key = store.key
        redis:set(store.key, "VALUE")
      end)
      assert_equal(redis:exists(key), 0)
    end)

    test('re-raises errors in the block', function()
      local key = nil
      local ok, err = pcall(function()
        klass:temporary(redis, function(store)
          key = store.key
          error("something!")
        end)
      end)

      assert_false(ok)
      assert_not_blank(err)

      assert_not_nil(key)
      assert_equal(redis:exists(key), 0)
    end)

    test('inheritable', function()
      local child = require("redpear.store").value
      child:temporary(redis, function(store)
        redis:set(store.key, "VALUE")
        assert_equal(store:get(), "VALUE")
      end)
    end)

  end)

end)
