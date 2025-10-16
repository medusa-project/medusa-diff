# frozen_string_literal: true

require 'simplecov'
SimpleCov.start

require 'minitest/autorun'
require 'mocha/minitest'
require_relative '../lib/find_diff'

Mocha.configure do |c|
  c.strict_keyword_argument_matching = true
end
