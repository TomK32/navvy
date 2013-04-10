Gem::Specification.new do |gem|
  gem.name    = 'navvy'
  gem.version = '0.3.2'
  gem.date    = '2012-09-10'

  gem.summary = "Simple background job processor inspired by delayed_job, but aiming for database agnosticism."
  gem.description = "Simple background job processor inspired by delayed_job, but aiming for database agnosticism."

  gem.authors  = ['Jeff Kreeftmeijer']
  gem.email    = 'jeff@kreeftmeijer.nl'
  gem.homepage = 'http://github.com/jeffkreeftmeijer/navvy'

  gem.files = Dir['{generators,lib,spec}/**/*', 'README*', 'LICENSE*'] & `git ls-files -z`.split("\0")

  gem.add_development_dependency('rake')
  gem.add_development_dependency('sqlite3-ruby')
  gem.add_development_dependency('rspec', ["~> 2.0"])
  gem.add_development_dependency('yard', [">= 0.5.2"])
  gem.add_development_dependency('timecop', [">= 0.3.5"])
  gem.add_development_dependency('fuubar', ["~> 0.0"])
end
