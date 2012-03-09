require 'middleclass'
local Redis = require 'redis'

local urandom = io.open("/dev/urandom", "rb")
local function random_string(length)
  local result = ""
  local bytes  = urandom:read(length)
  for b in string.gfind(bytes, ".") do
    result = result .. string.format("%02x", string.byte(b))
  end
  return result
end

module('redpear', package.seeall)

Store = {}

-- ########### Store.Base ############

-- Abstract store
Store.Base = class('redpear.Store.Base')

-- Constructor
function Store.Base:initialize(key, conn)
  self.key, self.conn = key, conn
end

-- Check existence
function Store.Base:exists()
  return self.conn:exists(self.key)
end

-- Deletes the key
function Store.Base:purge()
  self.conn:del(self.key)
end

-- Creates and yields over a temporary key.
-- Useful in combination with e.g. `interstore`, `unionstore`, etc.
-- @param conn Redis connection
-- @param fun(key) function to perform on that key
function Store.Base:temporary(conn, fun)
  local key     = "temp:" .. random_string(20)
  local store   = self:new(key, conn)
  local ok, err = pcall(fun, store)
  store:purge()
  if not ok then error(err) end
  return store
end

-- ########### Store.Set ############

-- Set store
Store.Set = class('redpear.Store.Set', Store.Base)

-- @return the array of members
function Store.Set:members()
  return self.conn:smembers(self.key)
end

--- Adds a single value. Chainable example:
-- @param value A value to add
function Store.Set:add(value)
  return self.conn:sadd(self.key, value)
end

-- @return the number of items in the set
function Store.Set:length()
  return self.conn:scard(self.key)
end

-- @param value A value to delete
function Store.Set:delete(value)
  return self.conn:srem(self.key, value)
end

-- @return true, if value is included
function Store.Set:include(value)
  return self.conn:sismember(self.key, value)
end

--- Removes a random value
-- @return the removed value
function Store.Set:pop()
  return self.conn:spop(self.key)
end

-- @return a random member
function Store.Set:random()
  return self.conn:srandmember(self.key)
end
