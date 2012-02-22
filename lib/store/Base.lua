Store.Base = class('redpear.Store.Base')

function Store.Base:initialize(key, conn)
  self.key, self.conn = key, conn
end