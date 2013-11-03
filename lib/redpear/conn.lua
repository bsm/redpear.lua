-------------------------------------
-- module redpear.conn
-------------------------------------
local store = require 'redpear.store'
local _M    = {}

-- Constructor
-- @param [table] redis redis connection
function _M:new(redis)
  return setmetatable({ conn = redis }, { __index = self })
end

-- @return [redis.store.value] a value store
function _M:value(key)
  return store.value:new(key, self.conn)
end

-- @return [redis.store.set] a set store
function _M:set(key)
  return store.set:new(key, self.conn)
end

-- @return [redis.store.sorted_set] a sorted set store
function _M:sorted_set(key)
  return store.sorted_set:new(key, self.conn)
end
function _M:zset(key)
  return self:sorted_set(key)
end

-- @return [redis.store.hash] a hash store
function _M:hash(key)
  return store.hash:new(key, self.conn)
end

return _M