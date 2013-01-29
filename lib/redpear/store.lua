-------------------------------------
-- module redpear.store
-------------------------------------
local M = {}

-------------------------------------
-- Local helper functions
-------------------------------------
local urandom = io.open("/dev/urandom", "rb")
local random_string = function(length)
  local result = ""
  local bytes  = urandom:read(length)
  for b in string.gfind(bytes, ".") do
    result = result .. string.format("%02x", string.byte(b))
  end
  return result
end

-------------------------------------
-- class redpear.store.base
-------------------------------------
M.base = {}

function M.base:new(key, conn)
  local this = { ["key"] = key, ["conn"] = conn }
  return setmetatable(this, { __index = self })
end

-- Check existence
function M.base:exists()
  local res, err = self.conn:exists(self.key)
  if res == true or tonumber(res) == 1 then
    return true, err
  elseif res == false or tonumber(res) == 0 then
    return false, err
  end
  return res, err
end

-- Deletes the key
function M.base:purge()
  self.conn:del(self.key)
end

-- Alias for purge
function M.base:clear()
  self:purge()
end

-- Creates and yields over a temporary key.
-- Useful in combination with e.g. `interstore`, `unionstore`, etc.
-- @param conn Redis connection
-- @param fun(key) function to perform on that key
function M.base:temporary(conn, fun)
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
M.value = setmetatable({}, { __index = M.base })

-- Get value
function M.value:get()
  return self.conn:get(self.key)
end

-- Set value
function M.value:set(val)
  return self.conn:set(self.key, val)
end


-------------------------------------
-- class redpear.store.set
-------------------------------------
M.set = setmetatable({}, { __index = M.base })

-- @return the array of members
function M.set:members()
  return self.conn:smembers(self.key)
end

--- Adds a single value. Chainable example:
-- @param value A value to add
function M.set:add(value)
  return self.conn:sadd(self.key, value)
end

-- @return the number of items in the set
function M.set:length()
  return self.conn:scard(self.key)
end
function M.set:count()
  return self:length()
end

-- @param value A value to delete
function M.set:delete(value)
  return self.conn:srem(self.key, value)
end

-- @return true, if value is included
function M.set:include(value)
  return self.conn:sismember(self.key, value)
end

--- Removes a random value
-- @return the removed value
function M.set:pop()
  return self.conn:spop(self.key)
end

-- @return a random member
function M.set:random()
  return self.conn:srandmember(self.key)
end

-- @param other a string key
-- @return an intersection table with `other`
function M.set:inter(other)
  return self.conn:sinter(self.key, other)
end

-- @param other a string key
-- @return a union table with `other`
function M.set:union(other)
  return self.conn:sunion(self.key, other)
end

-- @param other a string key
-- @return a diff table with `other`
function M.set:diff(other)
  return self.conn:sdiff(self.key, other)
end

-- @param multiple(string) other keys
-- @return an intersection set stored in `target`
function M.set:interstore(target, ...)
  self.conn:sinterstore(target, self.key, ...)
  return self:new(target, self.conn)
end

-- @param multiple(string) other keys
-- @return an union set stored in `target`
function M.set:unionstore(target, ...)
  self.conn:sunionstore(target, self.key, ...)
  return self:new(target, self.conn)
end

-- @param multiple(string) other keys
-- @return an diff set stored in `target`
function M.set:diffstore(target, ...)
  self.conn:sdiffstore(target, self.key, ...)
  return self:new(target, self.conn)
end


-------------------------------------
-- class redpear.store.sorted_set
-------------------------------------
M.sorted_set = setmetatable({}, { __index = M.base })

local to_sorted_table = function(members)
  if type(members[1]) ~= "table" then return members end

  local result = {}
  for _, v in pairs(members) do
    result[v[1]] = tonumber(v[2])
  end
  return result
end

-- @return the number of items in the set
function M.sorted_set:length()
  return self.conn:zcard(self.key)
end

-- @return the number of items in the set within range
function M.sorted_set:count(min, max)
  return self.conn:zcount(self.key, min, max)
end

-- @param member The member to add
-- @param score The score to set
function M.sorted_set:add(member, score)
  return self.conn:zadd(self.key, score, member)
end

-- @param member The member to delete
function M.sorted_set:delete(member)
  return self.conn:zrem(self.key, member)
end

-- @param member The member to check
-- @return the `member's` score
function M.sorted_set:score(member)
  local val = self.conn:zscore(self.key, member)
  return tonumber(val)
end

-- @param member The member to check
-- @return the (left) index of the given `member`
function M.sorted_set:index(member)
  return self.conn:zrank(self.key, member)
end

-- @param member The member to check
-- @return the right index of the given `member`
function M.sorted_set:rindex(member)
  return self.conn:zrevrank(self.key, member)
end

-- @param member The member to check
-- @return true if `member` is included
function M.sorted_set:included(member)
  return self:score(member) ~= nil
end

-- @return true, if empty
function M.sorted_set:empty()
  return self:length() == 0
end

-- @return the (left) slice from `start` to `finish`
function M.sorted_set:slice(start, finish, ...)
  return to_sorted_table(self.conn:zrange(self.key, start, finish, ...))
end

-- @return the right slice from `start` to `finish`
function M.sorted_set:rslice(start, finish, ...)
  return to_sorted_table(self.conn:zrevrange(self.key, start, finish, ...))
end

-- @return select from `min` to `max`
function M.sorted_set:select(min, max, ...)
  return to_sorted_table(self.conn:zrangebyscore(self.key, min, max, ...))
end

-- @return select from `max` to `min`
function M.sorted_set:rselect(max, min, ...)
  return to_sorted_table(self.conn:zrevrangebyscore(self.key, max, min, ...))
end

-- @return the member at `index`
function M.sorted_set:at(index)
  return self:slice(index, index)[1]
end

-- @return the first member
function M.sorted_set:first()
  return self:at(0)
end

-- @return the last member
function M.sorted_set:last()
  return self:at(-1)
end

-- @param multiple(string) other keys
-- @return an intersection SortedSet stored in `target`
function M.sorted_set:interstore(target, count, ...)
  self.conn:zinterstore(target, count+1, self.key, ...)
  return self:new(target, self.conn)
end

-- @param multiple(string) other keys
-- @return an union SortedSet stored in `target`
function M.sorted_set:unionstore(target, count, ...)
  self.conn:zunionstore(target, count+1, self.key, ...)
  return self:new(target, self.conn)
end


-------------------------------------
-- class redpear.store.hash
-------------------------------------
M.hash = setmetatable({}, { __index = M.base })

-- @return the number of items in the hash
function M.hash:length()
  return self.conn:hlen(self.key)
end

-- @return the table with all keys and values
function M.hash:all()
  return self.conn:hgetall(self.key)
end

-- @return true, if empty
function M.hash:empty()
  return self:length() == 0
end

-- @param key hash key to check
-- @return true, if key exists
function M.hash:has_key(key)
  return self.conn:hexists(self.key, key)
end

-- @param kek hash key to delete
function M.hash:delete(key)
  return self.conn:hdel(self.key, key) == 1
end

-- @see value/1 and values_at/n
-- @return value for given key
function M.hash:get(...)
  local keys = {...}
  if #keys == 1 then
    return self:value(keys[1])
  else
    return self:values_at(...)
  end
end

-- @param key hash key to fetch the value for
-- @return value for given key
function M.hash:value(key)
  return self.conn:hget(self.key, key)
end

-- @param keys hash keys to fetch the values for
-- @return table of values
function M.hash:values_at(keys, ...)
  if type(keys) ~= "table" then keys = {keys, ...} end
  return self.conn:hmget(self.key, keys)
end

-- @param key hash key to fetch the value for
-- @return value for given key
function M.hash:set(...)
  local args = {...}
  assert(type(args[1]) == 'table' or #args % 2 == 0, "invalid arguments")

  if #args == 2 then
    return self.conn:hset(self.key, ...)
  elseif #args % 2 == 0 then
    return self.conn:hmset(self.key, ...)
  else
    local kvkv = {}
    for k,v in pairs(args[1]) do
      kvkv[#kvkv+1] = k
      kvkv[#kvkv+1] = v
    end

    return self.conn:hmset(self.key, unpack(kvkv))
  end
end
function M.hash:update(...)
  return self:set(...)
end

-- @return table of all keys
function M.hash:keys()
  return self.conn:hkeys(self.key)
end

-- @return table of all values
function M.hash:values()
  return self.conn:hvals(self.key)
end

-- @param key hash key to increment
-- @param value the increment value, defaults to 1
function M.hash:increment(key, value)
  value = value or 1
  return self.conn:hincrby(self.key, key, value)
end

-- @param key hash key to decrement
-- @param value the decrement value, defaults to 1
function M.hash:decrement(key, value)
  value = value or 1
  return self:increment(key, -value)
end

-------------------------------------
-- class redpear.store.list
-------------------------------------
M.list = setmetatable({}, { __index = M.base })

-- @return [table] all the items in the list
function M.list:all()
  return self:range(0, - 1)
end

-- @param [number] start
-- @param [number] finish
-- @return [table] items
function M.list:range(start, finish)
  return self.conn:lrange(self.key, tonumber(start), tonumber(finish))
end

-- @param [number] start
-- @param [number] finish
-- @return [boolean] true if the results where removed
function M.list:trim(start, finish)
  return self.conn:ltrim(self.key, start, finish)
end

-- @return [number] the numbers of items in the list
function M.list:length()
  return self.conn:llen(self.key)
end

-- Add an item to the end of the list
-- @return [table] all the values
function M.list:push(item)
  return self.conn:rpush(self.key, item)
end

-- Removes and returns the last item in the list
-- return [string]
function M.list:pop()
  return self.conn:rpop(self.key)
end

-- Prepends a single item
-- @return [table] all the items in the list
function M.list:unshift(item)
  return self.conn:lpush(self.key, item)
end

-- Removes and returns the first item in the list
-- return [string]
function M.list:shift()
  return self.conn:lpop(self.key)
end

-- Removes item from the list
-- @param [string] item, the item to remove
-- @param [number] count, number of instances to delete
-- @return [number] the number of items removed
function M.list:delete(item, count)
  return self.conn:lrem(self.key, tonumber(count) or 0, item)
end

return M