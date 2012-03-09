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

module('redpear.store', package.seeall)

-- ########### Base ############

-- Abstract store
Base = class('redpear.store.Base')

-- Constructor
function Base:initialize(key, conn)
  self.key, self.conn = key, conn
end

-- Check existence
function Base:exists()
  return self.conn:exists(self.key)
end

-- Deletes the key
function Base:purge()
  self.conn:del(self.key)
end

-- Creates and yields over a temporary key.
-- Useful in combination with e.g. `interstore`, `unionstore`, etc.
-- @param conn Redis connection
-- @param fun(key) function to perform on that key
function Base.static:temporary(conn, fun)
  local key     = "temp:" .. random_string(20)
  local store   = self:new(key, conn)
  local ok, err = pcall(fun, store)
  store:purge()
  if not ok then error(err) end
  return store
end

-- ########### Set ############

-- Set store
Set = class('redpear.store.Set', Base)

-- @return the array of members
function Set:members()
  return self.conn:smembers(self.key)
end

--- Adds a single value. Chainable example:
-- @param value A value to add
function Set:add(value)
  return self.conn:sadd(self.key, value)
end

-- @return the number of items in the set
function Set:length()
  return self.conn:scard(self.key)
end

-- @param value A value to delete
function Set:delete(value)
  return self.conn:srem(self.key, value)
end

-- @return true, if value is included
function Set:include(value)
  return self.conn:sismember(self.key, value)
end

--- Removes a random value
-- @return the removed value
function Set:pop()
  return self.conn:spop(self.key)
end

-- @return a random member
function Set:random()
  return self.conn:srandmember(self.key)
end

-- @param other a string key
-- @return an intersection table with `other`
function Set:inter(other)
  return self.conn:sinter(self.key, other)
end

-- @param other a string key
-- @return a union table with `other`
function Set:union(other)
  return self.conn:sunion(self.key, other)
end

-- @param multiple(string) other keys
-- @return an intersection Set stored in `target`
function Set:interstore(target, ...)
  self.conn:sinterstore(target, self.key, ...)
  return self.class:new(target, self.conn)
end

-- @param multiple(string) other keys
-- @return an union Set stored in `target`
function Set:unionstore(target, ...)
  self.conn:sunionstore(target, self.key, ...)
  return self.class:new(target, self.conn)
end
