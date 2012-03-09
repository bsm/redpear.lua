require 'redpear'

context('Base', function()

  before(function()
    local params = { host = '127.0.0.1', port = 6379 }
    redis = Redis.connect(params)
    redis:select(9)  -- for testing purposes
    redis:del('key') -- flush before each test
    store = redpear.store.Base:new('key', redis)
  end)

  test('is correctly setup', function()
    assert_equal('redpear.store.Base', store.class.name)
  end)

  test('constructor accepts key and connection', function()
    assert_equal(store.key, 'key')
    assert_equal(store.conn, redis)
  end)

  test('can purge keys from the DB', function()
    redis:set('key', 'value')
    assert_not_nil(redis:get('key'))
    store:purge()
    assert_nil(redis:get('key'))
  end)

  test('check existence', function()
    assert_false(store:exists())
    redis:set('key', 'value')
    assert_true(store:exists())
  end)

  context('temporary', function()

    test('creates temporary keys', function()
      local key  = nil
      local temp = redpear.store.Set:temporary(redis, function(store)
        key = store.key
      end)

      assert(instanceOf(redpear.store.Set, temp))
      assert_match("temp:%w+", key)
      assert_equal(key, temp.key)
    end)

    test('performs redis block operations', function()
      local a, b = nil, nil
      redpear.store.Set:temporary(redis, function(store)
        a = store:exists()
        redis:set(store.key, "VALUE")
        b = store:exists()
      end)
      assert_false(a)
      assert_true(b)
    end)

    test('removes key afterwards', function()
      local key = nil
      redpear.store.Set:temporary(redis, function(store)
        key = store.key
        redis:set(store.key, "VALUE")
      end)
      assert_false(redis:exists(key))
    end)

    test('re-raises errors in the block', function()
      local key = nil
      local ok, err = pcall(function()
        redpear.store.Set:temporary(redis, function(store)
          key = store.key
          error("something!")
        end)
      end)

      assert_false(ok)
      assert_not_blank(err)

      assert_not_nil(key)
      assert_false(redis:exists(key))
    end)

  end)

end)