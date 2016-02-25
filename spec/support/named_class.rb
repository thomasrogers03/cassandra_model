class NamedClass
  def self.create(name, *args)
    klass = Class.new(*args) {}
    klass.define_singleton_method(:name) { name }
    klass.define_singleton_method(:to_s) { name.to_s }
    klass
  end
end
