# Mongobackup

require 'rubygems'
gem 'ey-flex'
require 'ey-flex'
Dir.glob( File.join( File.dirname(__FILE__), 'mongobackup', '*.rb') ){ |f| require f }
