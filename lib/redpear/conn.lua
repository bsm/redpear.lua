-------------------------------------
-- module redpear.conn
-------------------------------------
local store = require 'redpear.store'
local M     = {}

-- Constructor
-- @param [table] redis redis connection
function M:new(redis)
  return setmetatable({ conn = redis }, { __index = self })
end

-- @return [redis.store.value] a value store
function M:value(key)
  return store.value:new(key, self.conn)
end

-- @return [redis.store.set] a set store
function M:set(key)
  return store.set:new(key, self.conn)
end

-- @return [redis.store.sorted_set] a sorted set store
function M:sorted_set(key)
  return store.sorted_set:new(key, self.conn)
end
function M:zset(key)
  return self:sorted_set(key)
end

return M