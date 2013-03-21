local setmetatable = setmetatable
local tonumber = tonumber
local strfmt   = string.format
local strbyte  = string.byte
local gfind    = string.gfind
local pcall    = pcall
local type     = type
local pairs    = pairs
local assert   = assert
local unpack   = unpack
local urandom  = io.open("/dev/urandom", "rb")

-------------------------------------
-- module redpear.store
-------------------------------------
module(...)

-------------------------------------
-- Local helper functions
-------------------------------------
local random_string = function(length)
  local result = ""
  local bytes  = urandom:read(length)
  for b in gfind(bytes, ".") do
    result = result .. strfmt("%02x", strbyte(b))
  end
  return result
end

-------------------------------------
-- class redpear.store.base
-------------------------------------
base = {}

function base:new(key, conn)
  local this = { ["key"] = key, ["conn"] = conn }
  return setmetatable(this, { __index = self })
end

-- Check existence
function base:exists()
  local res, err = self.conn:exists(self.key)
  if res == true or tonumber(res) == 1 then
    return true, err
  elseif res == false or tonumber(res) == 0 then
    return false, err
  end
  return res, err
end

-- Deletes the key
function base:purge()
  return self.conn:del(self.key)
end

-- Alias for purge
function base:clear()
  return self:purge()
end

-- Expire in `seconds`
function base:expire(seconds)
  return self.conn:expire(self.key, seconds)
end

-- Alias for expire
function base:expire_in(seconds)
  return self:expire(seconds)
end

-- Expire at `timestamp`
function base:expire_at(timestamp)
  return self.conn:expireat(self.key, timestamp)
end

-- Returns the `ttl` in seconds
function base:ttl()
  return self.conn:ttl(self.key)
end

-- Creates and yields over a temporary key.
-- Useful in combination with e.g. `interstore`, `unionstore`, etc.
-- @param conn Redis connection
-- @param fun(key) function to perform on that key
function base:temporary(conn, fun)
  local key     = "temp:" .. random_string(20)
  local store   = self:new(key, conn)
  local ok, err = pcall(fun, store)
  store:purge()
  if not ok then error(err) end
  return store
end


-------------------------------------
-- class redpear.store.value
-------------------------------------
value = setmetatable({}, { __index = base })

-- Get value
function value:get()
  return self.conn:get(self.key)
end

-- Set value
function value:set(val)
  return self.conn:set(self.key, val)
end


-------------------------------------
-- class redpear.store.set
-------------------------------------
set = setmetatable({}, { __index = base })

-- @return the array of members
function set:members()
  return self.conn:smembers(self.key)
end

--- Adds a single value. Chainable example:
-- @param value A value to add
function set:add(value)
  return self.conn:sadd(self.key, value)
end

-- @return the number of items in the set
function set:length()
  return self.conn:scard(self.key)
end
function set:count()
  return self:length()
end

-- @param value A value to delete
function set:delete(value)
  return self.conn:srem(self.key, value)
end

-- @return true, if value is included
function set:include(value)
  return self.conn:sismember(self.key, value)
end

--- Removes a random value
-- @return the removed value
function set:pop()
  return self.conn:spop(self.key)
end

-- @return a random member
function set:random()
  return self.conn:srandmember(self.key)
end

-- @param other a string key
-- @return an intersection table with `other`
function set:inter(other)
  return self.conn:sinter(self.key, other)
end

-- @param other a string key
-- @return a union table with `other`
function set:union(other)
  return self.conn:sunion(self.key, other)
end

-- @param other a string key
-- @return a diff table with `other`
function set:diff(other)
  return self.conn:sdiff(self.key, other)
end

-- @param multiple(string) other keys
-- @return an intersection set stored in `target`
function set:interstore(target, ...)
  self.conn:sinterstore(target, self.key, ...)
  return self:new(target, self.conn)
end

-- @param multiple(string) other keys
-- @return an union set stored in `target`
function set:unionstore(target, ...)
  self.conn:sunionstore(target, self.key, ...)
  return self:new(target, self.conn)
end

-- @param multiple(string) other keys
-- @return an diff set stored in `target`
function set:diffstore(target, ...)
  self.conn:sdiffstore(target, self.key, ...)
  return self:new(target, self.conn)
end


-------------------------------------
-- class redpear.store.sorted_set
-------------------------------------
sorted_set = setmetatable({}, { __index = base })

local to_sorted_table = function(members)
  if type(members[1]) ~= "table" then return members end

  local result = {}
  for i=1,#members do
    local v = members[i]
    result[v[1]] = tonumber(v[2])
  end
  return result
end

-- @return the number of items in the set
function sorted_set:length()
  return self.conn:zcard(self.key)
end

-- @return the number of items in the set within range
function sorted_set:count(min, max)
  return self.conn:zcount(self.key, min, max)
end

-- @param member The member to add
-- @param score The score to set
function sorted_set:add(member, score)
  return self.conn:zadd(self.key, score, member)
end

-- @param member The member to delete
function sorted_set:delete(member)
  return self.conn:zrem(self.key, member)
end

-- @param member The member to check
-- @return the `member's` score
function sorted_set:score(member)
  local val = self.conn:zscore(self.key, member)
  return tonumber(val)
end

-- @param member The member to check
-- @return the (left) index of the given `member`
function sorted_set:index(member)
  return self.conn:zrank(self.key, member)
end

-- @param member The member to check
-- @return the right index of the given `member`
function sorted_set:rindex(member)
  return self.conn:zrevrank(self.key, member)
end

-- @param member The member to check
-- @return true if `member` is included
function sorted_set:included(member)
  return self:score(member) ~= nil
end

-- @return true, if empty
function sorted_set:empty()
  return self:length() == 0
end

-- @return the (left) slice from `start` to `finish`
function sorted_set:slice(start, finish, ...)
  return to_sorted_table(self.conn:zrange(self.key, start, finish, ...))
end

-- @return the right slice from `start` to `finish`
function sorted_set:rslice(start, finish, ...)
  return to_sorted_table(self.conn:zrevrange(self.key, start, finish, ...))
end

-- @return select from `min` to `max`
function sorted_set:select(min, max, ...)
  return to_sorted_table(self.conn:zrangebyscore(self.key, min, max, ...))
end

-- @return select from `max` to `min`
function sorted_set:rselect(max, min, ...)
  return to_sorted_table(self.conn:zrevrangebyscore(self.key, max, min, ...))
end

-- @return the member at `index`
function sorted_set:at(index)
  return self:slice(index, index)[1]
end

-- @return the first member
function sorted_set:first()
  return self:at(0)
end

-- @return the last member
function sorted_set:last()
  return self:at(-1)
end

-- @param multiple(string) other keys
-- @return an intersection SortedSet stored in `target`
function sorted_set:interstore(target, count, ...)
  self.conn:zinterstore(target, count+1, self.key, ...)
  return self:new(target, self.conn)
end

-- @param multiple(string) other keys
-- @return an union SortedSet stored in `target`
function sorted_set:unionstore(target, count, ...)
  self.conn:zunionstore(target, count+1, self.key, ...)
  return self:new(target, self.conn)
end


-------------------------------------
-- class redpear.store.hash
-------------------------------------
hash = setmetatable({}, { __index = base })

-- @return the number of items in the hash
function hash:length()
  return self.conn:hlen(self.key)
end

-- @return the table with all keys and values
function hash:all()
  return self.conn:hgetall(self.key)
end

-- @return true, if empty
function hash:empty()
  return self:length() == 0
end

-- @param key hash key to check
-- @return true, if key exists
function hash:has_key(key)
  return self.conn:hexists(self.key, key)
end

-- @param kek hash key to delete
function hash:delete(key)
  return self.conn:hdel(self.key, key) == 1
end

-- @see value/1 and values_at/n
-- @return value for given key
function hash:get(...)
  local keys = {...}
  if #keys == 1 then
    return self:value(keys[1])
  else
    return self:values_at(...)
  end
end

-- @param key hash key to fetch the value for
-- @return value for given key
function hash:value(key)
  return self.conn:hget(self.key, key)
end

-- @param keys hash keys to fetch the values for
-- @return table of values
function hash:values_at(...)
  return self.conn:hmget(self.key, ...)
end

-- @param key hash key to fetch the value for
-- @return value for given key
function hash:set(...)
  local args = {...}
  assert(type(args[1]) == 'table' or #args % 2 == 0, "invalid arguments")

  if #args == 2 then
    return self.conn:hset(self.key, ...)
  elseif #args % 2 == 0 then
    return self.conn:hmset(self.key, ...)
  else
    local seq = {}
    for k,v in pairs(args[1]) do
      seq[#seq+1] = k
      seq[#seq+1] = v
    end

    return self.conn:hmset(self.key, unpack(seq))
  end
end
function hash:update(...)
  return self:set(...)
end

-- @return table of all keys
function hash:keys()
  return self.conn:hkeys(self.key)
end

-- @return table of all values
function hash:values()
  return self.conn:hvals(self.key)
end

-- @param key hash key to increment
-- @param value the increment value, defaults to 1
function hash:increment(key, value)
  value = value or 1
  return self.conn:hincrby(self.key, key, value)
end

-- @param key hash key to decrement
-- @param value the decrement value, defaults to 1
function hash:decrement(key, value)
  value = value or 1
  return self:increment(key, -value)
end

-------------------------------------
-- class redpear.store.list
-------------------------------------
list = setmetatable({}, { __index = base })

-- @return [table] all the items in the list
function list:all()
  return self:range(0, - 1)
end

-- @param [number] start
-- @param [number] finish
-- @return [table] items
function list:range(start, finish)
  return self.conn:lrange(self.key, tonumber(start), tonumber(finish))
end

-- @param [number] start
-- @param [number] finish
-- @return [boolean] true if the results where removed
function list:trim(start, finish)
  return self.conn:ltrim(self.key, start, finish)
end

-- @return [number] the numbers of items in the list
function list:length()
  return self.conn:llen(self.key)
end

-- Add an item to the end of the list
-- @return [table] all the values
function list:push(item)
  return self.conn:rpush(self.key, item)
end

-- Removes and returns the last item in the list
-- return [string]
function list:pop()
  return self.conn:rpop(self.key)
end

-- Prepends a single item
-- @return [table] all the items in the list
function list:unshift(item)
  return self.conn:lpush(self.key, item)
end

-- Removes and returns the first item in the list
-- return [string]
function list:shift()
  return self.conn:lpop(self.key)
end

-- Removes item from the list
-- @param [string] item, the item to remove
-- @param [number] count, number of instances to delete
-- @return [number] the number of items removed
function list:delete(item, count)
  return self.conn:lrem(self.key, tonumber(count) or 0, item)
end

return M