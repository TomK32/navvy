$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'navvy'
require 'spec'
require 'spec/autorun'

Spec::Runner.configure do |config|
end

class Cow
  def self.speak
    'moo'
  end

  def self.broken
    raise 'this method is broken'
  end
end

class RailsLogger
  def self.info(text)
  end
end

class Justlogging
  def self.log(text)
  end
end

RAILS_DEFAULT_LOGGER = RailsLogger
Navvy::Log.quiet = true
