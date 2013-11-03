require 'spec.helper'

context('redpear.conn', function()
  local redis = require('redis'):new()
  local klass = require("redpear.conn")
  local subject

  before(function()
    redis:connect('127.0.0.1', 6379)
    redis:select(9)  -- for testing purposes
    subject = klass:new(redis)
  end)

  after(function()
    redis:flushdb()
  end)

  test('returns values', function()
    redis:set('key', 'value')
    assert_equal(subject:value('key'):get(), 'value')
  end)

  test('returns sets', function()
    redis:sadd('key', '1')
    redis:sadd('key', '2')
    assert_equal(subject:set('key'):length(), 2)
  end)

  test('returns sorted sets', function()
    redis:zadd('key', 1, '1')
    redis:zadd('key', 2, '2')
    assert_equal(subject:sorted_set('key'):length(), 2)
    assert_equal(subject:zset('key'):score(1), 1)
  end)

  test('returns hashes', function()
    redis:hset('key', 'f1', '1')
    assert_equal(subject:hash('key'):length(), 1)
  end)

end)