require 'csv'

class ParallelCSV
  DEFAULTS = {
    chunk_size: 5_000,
    col_sep: '¦',
    process_num: 4 # Number of parallel processes
  }.freeze

  attr_reader :path, :options

  def initialize(path, options = {})
    @options = DEFAULTS.merge(options)
    @path = path
  end

  def process(&block)
    # Process exceptions in main process if somehing is wrong with the file
    File.open(path) { }

    offset = 0

    # Pipe for getting EOF from child processes
    reader, writer = IO.pipe
    eof = false

    until eof do
      pids = []
      options[:process_num].times do
        pids << fork_unless_test do
          single_process(offset, writer, &block)
        end

        offset += options[:chunk_size]
      end

      pids.compact.each { |pid| Process.wait(pid) }

      # Reading pipe in a non-blocking way
      # If any process send 'stop', then stop
      eof = (IO.select([reader], [writer])[0].length > 0 && reader.gets == "stop\n")
    end
  end

  private

  def single_process(offset, writer, &block)
    io = IO.new(IO.sysopen(path, 'r'), 'r')
    csv = CSV.new(io, csv_options)
    chunk = []

    # Fast forward
    offset.times { io.readline }

    # Read chunk_size of lines or until EOF
    options[:chunk_size].times do
      line = csv.readline
      chunk << line.fields if line

      if io.eof?
        writer.puts 'stop'
        break
      end
    end
  rescue StandardError => e
    writer.puts 'stop'
  ensure
    # Try to pass the chunk anyway
    yield chunk if block_given? && !chunk.empty?
  end

  def csv_options
    options.except(:process_num, :chunk_size)
  end

  # For RSpec
  def fork_unless_test
    if Rails.env == 'test'
      yield
      return nil
    else
      fork { yield }
    end
  end
end
