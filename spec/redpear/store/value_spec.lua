require 'spec.helper'

context('redpear.store.value', function()
  local redis = require('redis'):new()
  local klass = require("redpear.store").value
  local subject

  before(function()
    redis:connect('127.0.0.1', 6379)
    redis:select(9)  -- for testing purposes
    subject = klass:new('key', redis)
  end)

  after(function()
    redis:flushdb()
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