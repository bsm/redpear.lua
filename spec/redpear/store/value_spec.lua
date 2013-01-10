require 'spec.helper'

context('redpear.store.value', function()

  before(function()
    klass   = require("redpear.store").value
    subject = klass:new('key', redis)
  end)

  test('inherits from base', function()
    assert_false(subject:exists())
    redis:set('key', 'value')
    assert_true(subject:exists())
  end)

  test('get', function()
    redis:set('key', 'value')
    assert_equal(subject:get(), 'value')
  end)

  test('set', function()
    subject:set('new_val')
    assert_equal(redis:get('key'), 'new_val')
  end)

end)