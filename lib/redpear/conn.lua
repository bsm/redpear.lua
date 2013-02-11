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
module(...)

-- Constructor
-- @param [table] redis redis connection
function new(self, redis)
  return setmetatable({ conn = redis }, { __index = self })
end

-- @return [redis.store.value] a value store
function value(self, key)
  return rp_val:new(key, self.conn)
end

-- @return [redis.store.set] a set store
function set(self, key)
  return rp_set:new(key, self.conn)
end

-- @return [redis.store.sorted_set] a sorted set store
function sorted_set(self, key)
  return rp_zset:new(key, self.conn)
end
function zset(self, key)
  return self:sorted_set(key)
end

-- @return [redis.store.hash] a hash store
function hash(self, key)
  return rp_hash:new(key, self.conn)
end

-- @return [redis.store.hash] a hash store
function list(self, key)
  return rp_list:new(key, self.conn)
end
