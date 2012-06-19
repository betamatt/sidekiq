require 'aws-sdk'

module Sidekiq
  class SQSConnection
    def self.create(options={})
      AWS::SQS.new(
        #:access_key_id => options[:aws_access_key] || ENV['AWS_ACCESS_KEY'],
        #:secret_key => options[:aws_secret_key] || ENV['AWS_SECRET_KEY'],
        :persistent => true)
    end
  end
end
