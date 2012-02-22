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
