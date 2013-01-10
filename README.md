# Redpear (Lua Edition)

Simplistic object mapper for Redis/Lua

## Usage

Initialize:

    local redis   = require('redis').connect({ host = '127.0.0.1', port = 6379 })
    local redpear = require('redpear.conn'):new(redis)

Simple values:

    local val = redpear.value("hello")
    val.set("world") -- => true
    val.get()        -- => "world"

Sets:

    local set = redpear.set("fruit")
    set.add("apple")
    set.add("pear")
    set.members()    -- => {"apple", "pear"}
    set.length()     -- => 2

Sorted Sets:

    local zset = redpear.zset("fruit:favourites")
    zset.add("apple", 1)
    zset.add("pear", 2)
    zset.count(2, 3) -- => 1
    zset.length()    -- => 2

Hashes:

    local hash = redpear.hash("fruit:sizes")
    hash.set("apple", "small")
    hash.set("watermelon", "large")
    hash.length()    -- => 2
    hash.keys()      -- => {"apple", "watermelon"}

## Tests

Install dependencies (via APT):

    sudo apt-get install lua5.1 luarocks liblua5.1-socket2
    sudo luarocks install telescope

Run tests:

    tsc -f spec/**/*.lua spec/*/**/*.lua

## Licence

The MIT License (MIT)

Copyright (c) 2013 Black Square Media Ltd

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

