require 'redpear'

context('Store.Set', function()

  test('is correctly setup', function()
    assert_equal(redpear.Store.Set.name, 'redpear.Store.Set')
  end)

  test('is a Store.Base', function()
    assert_equal(redpear.Store.Set.super, redpear.Store.Base)
  end)

  context('Redis access', function()

    before(function()
      local params = { host = '127.0.0.1', port = 6379 }
      local redis  = Redis.connect(params)

      redis:select(9) -- for testing purposes
      redis:del('key')
      redis:sadd('key', 'elem1')
      redis:sadd('key', 'elem2')

      set = redpear.Store.Set:new('key', redis)
    end)

    test('returns all members', function()
      assert_equal(set:members()[1], 'elem1')
      assert_equal(set:members()[2], 'elem2')
    end)

    test('adds member', function()
      set:add('elem3')
      assert_equal(set:members()[3], 'elem3')
    end)

    test('number of members', function()
      assert_equal(set:length(), 2)
    end)

    test('deletes members', function()
      set:delete('elem2')
      assert_equal(set:members()[2], nil)
    end)

    test('member inclusion', function()
      assert_equal(set:include('elem1'), true)
      assert_equal(set:include('other'), false)
    end)

    test('pops random member', function()
      local e = set:pop()

      assert_equal(e == 'elem1' or e == 'elem2', true)
      assert_equal(set:length(), 1)
    end)

    test('returns random member', function()
      local e = set:random()

      assert_equal(e == 'elem1' or e == 'elem2', true)
      assert_equal(set:length(), 2)
    end)

  end)
end)
