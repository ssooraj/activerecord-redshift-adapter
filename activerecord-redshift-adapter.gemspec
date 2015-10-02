$LOAD_PATH.push File.expand_path("../lib", __FILE__)

# Maintain your gem's version:
require "activerecord_redshift_adapter/version"

Gem::Specification.new do |s|
 s.name        = 'activerecord-redshift-adapter'
 s.version     = ActiverecordRedshiftAdapter::VERSION
 s.license     = 'New BSD License'
 s.date        = '2015-10-02'
 s.summary     = "Rails 3 database adapter support for AWS RedShift."
 s.description = "This gem provides the Rails 3 with database adapter for AWS RedShift."
 s.authors     = ["Keith Gabryelski"]
 s.email       = 'keith@fiksu.com'
 s.files       = `git ls-files`.split("\n")
 s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
 s.require_path = 'lib'
 s.homepage    = 'http://github.com/fiksu/activerecord-redshift-adapter'
 s.add_dependency "pg"
 s.add_dependency "activerecord", '~> 3.0'
 s.add_dependency "activesupport", '~> 3.0'
 s.add_dependency "arel", '~> 3.0'
 s.add_development_dependency 'rspec', '~> 3.0'
end
