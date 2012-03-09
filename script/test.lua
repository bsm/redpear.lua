require 'redpear'

local params = {
  host = '127.0.0.1',
  port = 6379,
}

local redis = Redis.connect(params)

set = Store.Set:new('key', redis)
set:add('1')

print(inspect(set:members()))