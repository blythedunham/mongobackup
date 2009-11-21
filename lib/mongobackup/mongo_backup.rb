module EyBackup
  class MongoBackup < MysqlBackup
    def initialize( opts = {} )
      super opts
      @timestamp = "#{Time.now.strftime("%Y-%m-%dT%H:%M:%S").gsub(/:/, '-')}"
      @tmpname ="#{@timestamp}.tgz"
      @dbpath = opts[:dbpath]
      @dbport = opts[:dbport]
      @dbhost = opts[:dbhost]
      @shutdown_command = opts[:shutdown_command]  
      @start_command = opts[:start_command] 
      @startup_options = opts[:startup_options]
      @shutdown = opts[:shutdown].to_s == 'true'
      @is_running_command = if opts[:is_running_command].to_s.downcase == 'true'
        'pgrep mongod'
      else
        opts[:is_running_command]
      end
    end

    def backup_database(database)
      # create a new directory to export
      timestamped_backup_dir = "#{self.backup_dir}/#{database}.#{@timestamp}"
      dump_directory = "#{timestamped_backup_dir}/dump"
      full_path_to_backup = "#{timestamped_backup_dir}/backup.tgz"
      FileUtils.mkdir_p timestamped_backup_dir

      run_with_mongodb do
        if system(mongodumpcmd)
          return false if compress_dump( dump_directory, full_path_to_backup )
          AWS::S3::S3Object.store(
             "/#{@id}.#{database}/#{database}.#{@tmpname}",
             open(full_path_to_backup),
             @bucket,
             :access => :private
          ) 
          puts "Successful backup: #{database}.#{@tmpname}"
        else
          raise "Unable to dump database#{database} wtf?"
        end
      end
    ensure
      FileUtils.rm_r full_path_to_backup
      FileUtils.rm_r dump_directory
    end

    def restore(index)
      temporary_dir = 'restored'
      name = download(index)
      return false unless uncompress_dump( name, restored )

      db = name.split('.').first
      cmd = "mongorestore #{mongocommand_parameters( db )} #{temporary_dir}"
      run_with_mongodb do
        if system(cmd)
          puts "successfully restored backup: #{name}"
        else
          puts "FAIL"
        end
      end
    ensure
      FileUtils.rm_r temporary_dir if defined?( temporary_dir ) && temporary_dir
    end
    
    protected
    
    def compress_dump( dump_directory, full_path_to_backup )
      system "cd #{dump_directory} && tar cvzvf #{full_path_to_backup} *"
    end

    def uncompress_dump( file_name, dirname )
      FileUtils.mkdir_p dirname
      unless system "cd #{temporary_dir} && tar xfz #{File.expand_path(name)}"
        puts "Unable to unzip file:#{ name }"
        return false 
      end
      dirname
    end

    def run_with_mongodb( full_shutdown = false )
      if @shutdown
        with_full_shutdown( &block )
      else
        yield
      end
    end

    def with_full_shutdown( &block )
      puts "Unable to shutdown database" and return false unless stop_database
      yield
    ensure
      restart_database
    end
    
    def stop_database
      return true if @shutdown_command.to_s == 'none'
  
      if result = system( @shutdown_command || 'pkill -15 mongod' )
        sleep( 2 ) and return true if ['', 'none'].include?( @is_running_command.to_s )
        15.times do
          return true if `#{@is_running_command}` == '' 
          sleep( 1 ) 
        end
      end
      false
    end

    def restart_database
      return true if @start_command.to_s == 'none'
      unless system( @start_command || "mongod #{mongocommand_parameters(nil, true)} #{additional_startup_options} &" )
        puts "ut oh. unable to restart mongo db"
      end
    end

    def additional_startup_options
      case @startup_options
        when Hash then @startup_options.collect{|k,v| "-#{'-' unless k.to_s.length == 1}#{k} #{v}" }.join(" ")
        when Array then @startup_options.join(" ")
        else @startup_options.to_s
      end
    end

    def mongocommand_parameters( database = nil, use_db_path = false)
      command = ''
      command << " --host #{@dbhost}" if @dbhost
      command << " --port #{@dbport}" if @dbport
      command << " --dbpath #{@dbpath}" if @dbpath && (use_db_path || @shutdown)
      command << " --db #{database}" if database
      command << " --username #{@dbuser}" if @dbuser
      command << " --password #{@dbpass}" if @dbpass
      command
    end
  end
end

