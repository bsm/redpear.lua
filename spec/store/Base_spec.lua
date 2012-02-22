require 'redpear'

context('Store', function()

  test('is correctly setup', function()

    assert_equal(Store.name, 'redpear.Store')

  end)
end)

context('Store.Base', function()
  test('is correctly setup', function()

    assert_equal(Store.Base.name, 'redpear.Store.Base')

  end)

  context('constructor', function()

    test('accepts key and connection', function()

      local inst = Store.Base:new('key', 'connection')
      assert_equal(inst.key, 'key')
      assert_equal(inst.conn, 'connection')

    end)

  end)

end)