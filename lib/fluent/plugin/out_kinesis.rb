require 'aws-sdk-core'
require 'base64'
require 'multi_json'
require 'logger'
require 'securerandom'
require 'fluent/plugin/version'

module FluentPluginKinesis
  class OutputFilter < Fluent::BufferedOutput

    include Fluent::DetachMultiProcessMixin
    include Fluent::SetTimeKeyMixin
    include Fluent::SetTagKeyMixin

    USER_AGENT_NAME = 'fluent-plugin-kinesis-output-filter'
    MANDATORY_PARAMS = [:region, :stream_name]
    PROC_BASE_STR = 'proc {|record| %s }'

    Fluent::Plugin.register_output('kinesis',self)

    config_set_default :include_time_key, false
    config_set_default :include_tag_key,  false

    config_param :aws_key_id,    :string,  default: nil
    config_param :aws_sec_key,   :string,  default: nil
    config_param :region,        :string,  default: nil
    config_param :kinesis_chunk, :integer, default: 37000

    config_param :stream_name,      :string, default: nil
    config_param :random_partition_key, :bool, default: false
    config_param :partition_key,      :string, default: nil
    config_param :partition_key_expr,   :string, default: nil
    config_param :explicit_hash_key,    :string, default: nil
    config_param :explicit_hash_key_expr, :string, default: nil
    config_param :order_events, :bool, default: false

    config_param :debug, :bool, default: false

    def configure(conf)
      super
      validate_params

      if @detach_process or (@num_threads > 1)
        @parallel_mode = true
        if @detach_process
          @use_detach_multi_process_mixin = true
        end   
      else
        @parallel_mode = false
      end

      if @parallel_mode
        if @order_events
          log.warn 'You have set "order_events" to true, however this configuration will be ignored due to "detach_process" and/or "num_threads".'
        end
        @order_events = false
      end

      if @partition_key_expr
        partition_key_proc_str = sprintf(
          PROC_BASE_STR, @partition_key_expr
        )
        @partition_key_proc = eval(partition_key_proc_str)
      end

      if @explicit_hash_key_expr
        explicit_hash_key_proc_str = sprintf(
          PROC_BASE_STR, @explicit_hash_key_expr
        )
        @explicit_hash_key_proc = eval(explicit_hash_key_proc_str)
      end
    end

    def start
      detach_multi_process do
        super
        load_client
        check_connection_to_stream
      end
    end

    def format(tag, time, record)
      data = record['message'].to_msgpack
      return data
    end

    def package_for_kinesis(arr, stream)
      random_number = SecureRandom.uuid
      big_chunk = Base64.strict_encode64('[' + arr.join(",") + ']')
      kinesis_to_send = { :data => big_chunk, :partition_key => random_number, :stream_name => stream }
      return kinesis_to_send
    end

    def write(chunk)
      kinesis_chunks = Array.new
      chunk_size = 2 # we're packing the chunks into a json array - start with '[]'
      chunk.msgpack_each do |data|
        data_to_put = data
        len = (data.length+1)
        if (len + chunk_size) > @kinesis_chunk
          kinesis_to_send = package_for_kinesis(kinesis_chunks, @stream_name)
          result = @client.put_record(kinesis_to_send)
          chunk_size = 0
          kinesis_chunks = Array.new
        else
          kinesis_chunks.push(data)
          chunk_size += len 
        end
      end
      if kinesis_chunks.length > 0
        kinesis_to_send = package_for_kinesis(kinesis_chunks, @stream_name)
        result = @client.put_record(kinesis_to_send)
      end
    end

    private
    def validate_params

      MANDATORY_PARAMS.each do |name|
        unless instance_variable_get("@#{name}")
          raise ConfigError, "'#{name}' is required"
        end
      end

      unless @random_partition_key or @partition_key or @partition_key_expr
        raise ConfigError, "'random_partition_key' or 'partition_key' or 'partition_key_expr' is required"
      end
    end

    def load_client

      user_agent_suffix = "#{USER_AGENT_NAME}/#{FluentPluginKinesis::VERSION}"

      options = {
        region: @region,
        user_agent_suffix: user_agent_suffix
      }

      if @aws_key_id && @aws_sec_key
        options.update(
          access_key_id: @aws_key_id,
          secret_access_key: @aws_sec_key,
        )
      end

      if @debug
        options.update(
          logger: Logger.new(log.out),
          log_level: :debug
        )
        # XXX: Add the following options, if necessary
        # :http_wire_trace => true
      end

      @client = Aws::Kinesis::Client.new(options)

    end

    def check_connection_to_stream
      @client.describe_stream(stream_name: @stream_name)
    end

    def get_key(name, record)
      if @random_partition_key
        SecureRandom.uuid 
      else
        key = instance_variable_get("@#{name}")
        key_proc = instance_variable_get("@#{name}_proc")

        value = key ? record[key] : record

        if key_proc
          value = key_proc.call(value)
        end

        value.to_s
      end
    end

    def build_data_to_put(data)
      Hash[data.map{ |k, v| [k.to_sym, v] }]
    end
  end
end
