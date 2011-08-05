task :environment

namespace :navvy do
  desc "Clear the Navvy queue."
  task :clear => :environment do
    Navvy::Job.delete_all
  end

  desc "Start a Navvy worker."
  task :work => :environment do
    `echo "#{Process.pid}" > #{Rails.root.join('tmp','pids','navvy.pid')}`  
    `echo "#{Process.ppid}" > #{Rails.root.join('tmp','pids','navvy.ppid')}`
    puts "Starting worker..."
    Navvy::Worker.start
  end
end

# heroku background jobs use jobs:work
task 'jobs:work' => 'navvy:work'