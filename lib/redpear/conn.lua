local setmetatable = setmetatable
local rps = require 'redpear.store'
local rp_list = rps.list
local rp_hash = rps.hash
local rp_set  = rps.set
local rp_zset = rps.sorted_set
local rp_val  = rps.value

-------------------------------------
-- module redpear.conn
-------------------------------------
local _M = {}
local mt = { __index = _M }

-- Constructor
-- @param [table] redis redis connection
function _M:new(redis)
  return setmetatable({ conn = redis }, mt)
end

-- @return [redis.store.value] a value store
function _M:value(key)
  return rp_val:new(key, self.conn)
end

-- @return [redis.store.set] a set store
function _M:set(key)
  return rp_set:new(key, self.conn)
end

-- @return [redis.store.sorted_set] a sorted set store
function _M:sorted_set(key)
  return rp_zset:new(key, self.conn)
end
function _M:zset(key)
  return self:sorted_set(key)
end

-- @return [redis.store.hash] a hash store
function _M:hash(key)
  return rp_hash:new(key, self.conn)
end

-- @return [redis.store.hash] a hash store
function _M:list(key)
  return rp_list:new(key, self.conn)
end

return _M
