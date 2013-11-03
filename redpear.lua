require 'middleclass'

local urandom = io.open("/dev/urandom", "rb")
local function random_string(length)
  local result = ""
  local bytes  = urandom:read(length)
  for b in string.gfind(bytes, ".") do
    result = result .. string.format("%02x", string.byte(b))
  end
  return result
end


-------------------------------------
-- module redpear.store
-------------------------------------

module('redpear.store', package.seeall)


-------------------------------------
-- class Base
-------------------------------------

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
function Base:temporary(conn, fun)
  local key     = "temp:" .. random_string(20)
  local store   = self:new(key, conn)
  local ok, err = pcall(fun, store)
  store:purge()
  if not ok then error(err) end
  return store
end


-------------------------------------
-- class Set
-------------------------------------

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

-- @param other a string key
-- @return a diff table with `other`
function Set:diff(other)
  return self.conn:sdiff(self.key, other)
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

-- @param multiple(string) other keys
-- @return an diff Set stored in `target`
function Set:diffstore(target, ...)
  self.conn:sdiffstore(target, self.key, ...)
  return self.class:new(target, self.conn)
end

-------------------------------------
-- class SortedSet
-------------------------------------

local function _sorted_members_table(members)
  if type(members[1]) ~= "table" then return members end

  local result = {}
  for _, v in pairs(members) do
    result[v[1]] = tonumber(v[2])
  end
  return result
end

-- Sorted set store
SortedSet = class('redpear.store.SortedSet', Base)

-- @return the number of items in the set
function SortedSet:length()
  return self.conn:zcard(self.key)
end

-- @return the number of items in the set within range
function SortedSet:count(min, max)
  return self.conn:zcount(self.key, min, max)
end

-- @param member The member to add
-- @param score The score to set
function SortedSet:add(member, score)
  return self.conn:zadd(self.key, score, member)
end

-- @param member The member to delete
function SortedSet:delete(member)
  return self.conn:zrem(self.key, member)
end

-- @param member The member to check
-- @return the `member's` score
function SortedSet:score(member)
  local val = self.conn:zscore(self.key, member)
  return tonumber(val)
end

-- @param member The member to check
-- @return the (left) index of the given `member`
function SortedSet:index(member)
  return self.conn:zrank(self.key, member)
end

-- @param member The member to check
-- @return the right index of the given `member`
function SortedSet:rindex(member)
  return self.conn:zrevrank(self.key, member)
end

-- @param member The member to check
-- @return true if `member` is included
function SortedSet:included(member)
  return self:score(member) ~= nil
end

-- @return true, if empty
function SortedSet:empty()
  return self:length() == 0
end

-- @return the (left) slice from `start` to `finish`
function SortedSet:slice(start, finish, ...)
  return _sorted_members_table(self.conn:zrange(self.key, start, finish, ...))
end

-- @return the right slice from `start` to `finish`
function SortedSet:rslice(start, finish, ...)
  return _sorted_members_table(self.conn:zrevrange(self.key, start, finish, ...))
end

-- @return select from `min` to `max`
function SortedSet:select(min, max, ...)
  return _sorted_members_table(self.conn:zrangebyscore(self.key, min, max, ...))
end

-- @return select from `max` to `min`
function SortedSet:rselect(max, min, ...)
  return _sorted_members_table(self.conn:zrevrangebyscore(self.key, max, min, ...))
end

-- @return the member at `index`
function SortedSet:at(index)
  return self:slice(index, index)[1]
end

-- @return the first member
function SortedSet:first()
  return self:at(0)
end

-- @return the last member
function SortedSet:last()
  return self:at(-1)
end

-- @param multiple(string) other keys
-- @return an intersection SortedSet stored in `target`
function SortedSet:interstore(target, count, ...)
  self.conn:zinterstore(target, count+1, self.key, ...)
  return self.class:new(target, self.conn)
end

-- @param multiple(string) other keys
-- @return an union SortedSet stored in `target`
function SortedSet:unionstore(target, count, ...)
  self.conn:zunionstore(target, count+1, self.key, ...)
  return self.class:new(target, self.conn)
end
