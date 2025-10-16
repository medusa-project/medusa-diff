# frozen_string_literal: true

require 'rake/testtask'
require 'simplecov'

Rake::TestTask.new(:test) do |t|
  t.libs << 'lib' << 'test'
  t.test_files = FileList['test/*_test.rb']
end
