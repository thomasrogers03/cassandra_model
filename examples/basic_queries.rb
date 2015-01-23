require_relative 'spike/common'

class ImageData < Record
  ImageData.primary_key = [:path, :data]
end

Record.config = { hosts: %w(me), keyspace: 'pcrawler' }
puts ImageData.first({})