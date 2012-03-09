### Install Lua

Via APT:

   sudo apt-get install lua5.1 luarocks liblua5.1-socket2

Via sources:

   wget http://www.lua.org/ftp/lua-5.1.5.tar.gz && tar zxfv lua-5.1.5.tar.gz && cd lua-5.1.5 && make linux && sudo make install
   wget http://luarocks.org/releases/luarocks-2.0.8-rc1.tar.gz && tar zxfv luarocks-2.0.8-rc1.tar.gz && cd luarocks-2.0.8 && ./configure && make && sudo make install
   wget http://files.luaforge.net/releases/luasocket/luasocket/luasocket-2.0.2/luasocket-2.0.2.tar.gz && tar zxfv luasocket-2.0.2.tar.gz && cd luasocket-2.0.2/ && make && sudo make install

### Get Rocks (Lua's Gems)

   sudo luarocks install telescope

## Run specs

   tsc -f spec/**/*.lua
