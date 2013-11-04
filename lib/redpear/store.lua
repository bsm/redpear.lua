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
local error    = error
local urandom  = io.open("/dev/urandom", "rb")

-------------------------------------
-- module redpear.store
-------------------------------------
local _M = {}

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

local to_bool = function(res)
  if res == true or res == 'OK' or tonumber(res) == 1 then
    return true
  elseif res == false or tonumber(res) == 0 then
    return false
  end
  return res
end

-------------------------------------
-- class redpear.store.base
-------------------------------------
_M.base = {}

function _M.base:new(key, conn)
  local this = { ["key"] = key, ["conn"] = conn }
  return setmetatable(this, { __index = self })
end

-- Check existence
function _M.base:exists()
  local res, err = self.conn:exists(self.key)
  return to_bool(res), err
end

-- Deletes the key
function _M.base:purge()
  return self.conn:del(self.key)
end

-- Alias for purge
function _M.base:clear()
  return self:purge()
end

-- Expire in `seconds`
function _M.base:expire(seconds)
  return self.conn:expire(self.key, seconds)
end

-- Alias for expire
function _M.base:expire_in(seconds)
  return self:expire(seconds)
end

-- Expire at `timestamp`
function _M.base:expire_at(timestamp)
  local res, err = self.conn:expireat(self.key, timestamp)
  return to_bool(res), err
end

function _M.base:ttl()
  return self.conn:ttl(self.key)
end

-- Creates and yields over a temporary key.
-- Useful in combination with e.g. `interstore`, `unionstore`, etc.
-- @param conn Redis connection
-- @param fun(key) function to perform on that key
function _M.base:temporary(conn, fun)
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
_M.value = setmetatable({}, { __index = _M.base })

-- Get value
function _M.value:get()
  return self.conn:get(self.key)
end

-- Set value
function _M.value:set(val)
  return self.conn:set(self.key, val)
end


-------------------------------------
-- class redpear.store.set
-------------------------------------
_M.set = setmetatable({}, { __index = _M.base })

-- @return the array of members
function _M.set:members()
  return self.conn:smembers(self.key)
end

--- Adds a single value. Chainable example:
-- @param value A value to add
function _M.set:add(value)
  return self.conn:sadd(self.key, value)
end

-- @return the number of items in the set
function _M.set:length()
  return self.conn:scard(self.key)
end
function _M.set:count()
  return self:length()
end

-- @param value A value to delete
function _M.set:delete(value)
  return self.conn:srem(self.key, value)
end

-- @return true, if value is included
function _M.set:include(value)
  local res, err = self.conn:sismember(self.key, value)
  return to_bool(res), err
end

--- Removes a random value
-- @return the removed value
function _M.set:pop()
  return self.conn:spop(self.key)
end

-- @return a random member
function _M.set:random()
  return self.conn:srandmember(self.key)
end

-- @param other a string key
-- @return an intersection table with `other`
function _M.set:inter(other)
  return self.conn:sinter(self.key, other)
end

-- @param other a string key
-- @return a union table with `other`
function _M.set:union(other)
  return self.conn:sunion(self.key, other)
end

-- @param other a string key
-- @return a diff table with `other`
function _M.set:diff(other)
  return self.conn:sdiff(self.key, other)
end

-- @param multiple(string) other keys
-- @return an intersection set stored in `target`
function _M.set:interstore(target, ...)
  self.conn:sinterstore(target, self.key, ...)
  return self:new(target, self.conn)
end

-- @param multiple(string) other keys
-- @return an union set stored in `target`
function _M.set:unionstore(target, ...)
  self.conn:sunionstore(target, self.key, ...)
  return self:new(target, self.conn)
end

-- @param multiple(string) other keys
-- @return an diff set stored in `target`
function _M.set:diffstore(target, ...)
  self.conn:sdiffstore(target, self.key, ...)
  return self:new(target, self.conn)
end



-------------------------------------
-- class redpear.store.sorted_set
-------------------------------------
_M.sorted_set = setmetatable({}, { __index = _M.base })

-- @return the number of items in the set
function _M.sorted_set:length()
  return self.conn:zcard(self.key)
end

-- @return the number of items in the set within range
function _M.sorted_set:count(min, max)
  return self.conn:zcount(self.key, min, max)
end

-- @param member The member to add
-- @param score The score to set
function _M.sorted_set:add(member, score)
  return self.conn:zadd(self.key, score, member)
end

-- @param member The member to delete
function _M.sorted_set:delete(member)
  return self.conn:zrem(self.key, member)
end

-- @param member The member to check
-- @return the `member's` score
function _M.sorted_set:score(member)
  local res, err = self.conn:zscore(self.key, member)
  if type(res) == "string" then res = tonumber(res) or res end
  return res, err
end

-- @param member The member to check
-- @return the (left) index of the given `member`
function _M.sorted_set:index(member)
  return self.conn:zrank(self.key, member)
end

-- @param member The member to check
-- @return the right index of the given `member`
function _M.sorted_set:rindex(member)
  return self.conn:zrevrank(self.key, member)
end

-- @param member The member to check
-- @return true if `member` is included
function _M.sorted_set:included(member)
  local res, err = self:score(member)
  return type(res) == "number", err
end

-- @return true, if empty
function _M.sorted_set:empty()
  return self:length() == 0
end

-- @return the (left) slice from `start` to `finish`
function _M.sorted_set:slice(start, finish, ...)
  return self.conn:zrange(self.key, start, finish, ...)
end

-- @return the right slice from `start` to `finish`
function _M.sorted_set:rslice(start, finish, ...)
  return self.conn:zrevrange(self.key, start, finish, ...)
end

-- @return select from `min` to `max`
function _M.sorted_set:select(min, max, ...)
  return self.conn:zrangebyscore(self.key, min, max, ...)
end

-- @return select from `max` to `min`
function _M.sorted_set:rselect(max, min, ...)
  return self.conn:zrevrangebyscore(self.key, max, min, ...)
end

-- @return the member at `index`
function _M.sorted_set:at(index)
  local res, err = self:slice(index, index)
  if type(res) == "table" then
    return res[1], err
  end
  return res, err
end

-- @return the first member
function _M.sorted_set:first()
  return self:at(0)
end

-- @return the last member
function _M.sorted_set:last()
  return self:at(-1)
end

-- @param multiple(string) other keys
-- @return an intersection SortedSet stored in `target`
function _M.sorted_set:interstore(target, count, ...)
  self.conn:zinterstore(target, count+1, self.key, ...)
  return self:new(target, self.conn)
end

-- @param multiple(string) other keys
-- @return an union SortedSet stored in `target`
function _M.sorted_set:unionstore(target, count, ...)
  self.conn:zunionstore(target, count+1, self.key, ...)
  return self:new(target, self.conn)
end


-------------------------------------
-- class redpear.store.hash
-------------------------------------
_M.hash = setmetatable({}, { __index = _M.base })

-- @return the number of items in the hash
function _M.hash:length()
  return self.conn:hlen(self.key)
end

-- @return the table with all keys and values
function _M.hash:all()
  local res, err = self.conn:hgetall(self.key)
  if type(res) == "table" then
    local tab = {}
    for i=1,#res,2 do
      tab[res[i]] = res[i+1]
    end
    return tab, err
  end
  return res, err
end

-- @return true, if empty
function _M.hash:empty()
  return self:length() == 0
end

-- @param key hash key to check
-- @return true, if key exists
function _M.hash:has_key(key)
  local res, err = self.conn:hexists(self.key, key)
  return to_bool(res), err
end

-- @param kek hash key to delete
function _M.hash:delete(key)
  return self.conn:hdel(self.key, key) == 1
end

-- @see value/1 and values_at/n
-- @return value for given key
function _M.hash:get(...)
  local keys = {...}
  if #keys == 1 then
    return self:value(keys[1])
  else
    return self:values_at(...)
  end
end

-- @param key hash key to fetch the value for
-- @return value for given key
function _M.hash:value(key)
  return self.conn:hget(self.key, key)
end

-- @param keys hash keys to fetch the values for
-- @return table of values
function _M.hash:values_at(...)
  local args = {...}
  return self.conn:hmget(self.key, unpack(type(args[1]) == 'table' and args[1] or args))
end

-- @param key hash key to fetch the value for
-- @return value for given key
function _M.hash:set(...)
  local args = {...}
  assert(type(args[1]) == 'table' or #args % 2 == 0, "invalid arguments")

  local res, err
  if #args == 2 then
    res, err = self.conn:hset(self.key, ...)
  elseif #args % 2 == 0 then
    res, err = self.conn:hmset(self.key, ...)
  else
    local seq = {}
    for k,v in pairs(args[1]) do
      seq[#seq+1] = k
      seq[#seq+1] = v
    end

    res, err = self.conn:hmset(self.key, unpack(seq))
  end

  return to_bool(res), err
end

function _M.hash:update(...)
  return self:set(...)
end

-- @return table of all keys
function _M.hash:keys()
  return self.conn:hkeys(self.key)
end

-- @return table of all values
function _M.hash:values()
  return self.conn:hvals(self.key)
end

-- @param key hash key to increment
-- @param value the increment value, defaults to 1
function _M.hash:increment(key, value)
  value = value or 1
  return self.conn:hincrby(self.key, key, value)
end

-- @param key hash key to decrement
-- @param value the decrement value, defaults to 1
function _M.hash:decrement(key, value)
  value = value or 1
  return self:increment(key, -value)
end

-------------------------------------
-- class redpear.store.list
-------------------------------------
_M.list = setmetatable({}, { __index = _M.base })

-- @return [table] all the items in the list
function _M.list:all()
  return self:range(0, - 1)
end

-- @param [number] start
-- @param [number] finish
-- @return [table] items
function _M.list:range(start, finish)
  return self.conn:lrange(self.key, tonumber(start), tonumber(finish))
end

-- @param [number] start
-- @param [number] finish
-- @return [boolean] true if the results where removed
function _M.list:trim(start, finish)
  local res, err = self.conn:ltrim(self.key, start, finish)
  return to_bool(res), err
end

-- @return [number] the numbers of items in the list
function _M.list:length()
  return self.conn:llen(self.key)
end

-- Add an item to the end of the list
-- @return [table] all the values
function _M.list:push(item)
  return self.conn:rpush(self.key, item)
end

-- Removes and returns the last item in the list
-- return [string]
function _M.list:pop()
  return self.conn:rpop(self.key)
end

-- Prepends a single item
-- @return [table] all the items in the list
function _M.list:unshift(item)
  return self.conn:lpush(self.key, item)
end

-- Removes and returns the first item in the list
-- return [string]
function _M.list:shift()
  return self.conn:lpop(self.key)
end

-- Removes item from the list
-- @param [string] item, the item to remove
-- @param [number] count, number of instances to delete
-- @return [number] the number of items removed
function _M.list:delete(item, count)
  return self.conn:lrem(self.key, tonumber(count) or 0, item)
end

-------------------------------------
-- return module
-------------------------------------
return _M
