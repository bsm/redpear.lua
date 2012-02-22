Get Lua

   wget http://www.lua.org/ftp/lua-5.1.5.tar.gz && tar zxfv lua-5.1.5.tar.gz && cd lua-5.1.5 && make linux && sudo make install

Get LuaRocks (package manager)

   wget http://luarocks.org/releases/luarocks-2.0.8-rc1.tar.gz && tar zxfv luarocks-2.0.8-rc1.tar.gz && cd luarocks-2.0.8 && ./configure && make && sudo make install

Get Telescope (spec tool)

   sudo luarocks install telescope

Get LuaSocket

   wget http://files.luaforge.net/releases/luasocket/luasocket/luasocket-2.0.2/luasocket-2.0.2.tar.gz && tar zxfv luasocket-2.0.2.tar.gz && cd luasocket-2.0.2/ && make && sudo make install

Run specs with

   tsc -f spec/**/*.lua

