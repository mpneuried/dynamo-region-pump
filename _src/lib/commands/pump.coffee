cli = require('cli').enable('help', 'status', "version")

config = require( "../../lib/config" )

cli.setApp( "Dynamo Region Pump" )

multimeter = require( "multimeter" )
multi = multimeter(process);
charm = multi.charm;
charm.on('^C', process.exit);
charm.reset();

exports.run = ->
	cli.parse(
		domain: [ "d", "Domain to migrate", "string" ]
		since: [ "s", "Read Source DynamoDB content since modified. As timestamp in seconds.", "number" ]
		source_accesskey: [ "sa", "AWS Source DynamoDB Access Key", "string" ]
		source_secret:  [ "ss", "AWS Source DynamoDB Access Key", "string" ]
		source_region:  [ "sr", "AWS Source DynamoDB Region", "string", "" ]
		source_table:	[ "st", "AWS Source DynamoDB Table", "string", "" ]
		target_accesskey: [ "ta", "AWS Target DynamoDB Access Key", "string" ]
		target_secret:  [ "ts", "AWS Target DynamoDB Access Key", "string" ]
		target_region:  [ "tr", "AWS Target DynamoDB Region", "string", "" ]
		target_table:  [ "tt", "AWS Target DynamoDB Table", "string", "" ]
	)
	cli.main ( args, options )->
		process.stdout.write '\u001B[2J\u001B[0;0f'
		loadbar = multi(20,7, { width: 70 })
		writebar = multi(20,8, { width: 70 })
		_cnf =
			pump:
				since: options.since

		_cnf.source = {} if not _cnf.source
		_cnf.source.accessKeyId = options.source_accesskey if options.source_accesskey?.length
		_cnf.source.secretAccessKey = options.source_secret if options.source_secret?.length
		_cnf.source.region = options.source_region if options.source_region?.length
		_cnf.source.table = options.source_table if options.source_table?.length

		_cnf.target = {} if not _cnf.target
		_cnf.target.accessKeyId = options.target_accesskey if options.target_accesskey?.length
		_cnf.target.secretAccessKey = options.target_secret if options.target_secret?.length
		_cnf.target.region = options.target_region if options.target_region?.length
		if options.target_table?.length
			_cnf.target.table = options.target_table 
		else if options.source_table?.length
			_cnf.target.table = options.source_table
		config.init( _cnf )


		Pump = require( "../../" )

		try
			_mig = new Pump()

			_process_count = 0
			_process_loaded = 0
			_mig.on "loadAllDataStart", ( count )=>
				_process_count = count
				cli.ok "Load all #{count} items ...\nLoading source:\nDynamo write cache:"
				#cli.progress( 0 )
				return 

			_mig.on "loadAllDataStop", =>
				_process_count = 0
				return 

			_mig.on "loadAllDataReceived", ( loaded )=>
				_process_loaded = _process_loaded + loaded
				#cli.progress( _process_loaded / _process_count )
				_prec = ( _process_loaded / _process_count )*100
				loadbar.percent( _prec, "#{Math.round(_prec)} % - LOADED:#{_process_loaded}" )
				return 

			_mig.on "writeData", ( maxSize, open, dynamoState )=>
				if maxSize > 0
					_prec = ( open / maxSize )*100
					#_mig.error( "prec", _prec, open , maxSize )
					writebar.percent( _prec, "#{Math.round(_prec)} % - TODO:#{open}/#{maxSize} CUNITS:#{dynamoState.capacityUnits}  " )
				else
					writebar.percent( 0, "0 % - TODO:#{open}/#{maxSize} CUNITS:#{dynamoState.capacityUnits}  " )
				return

			_mig.start ( err, resp )=>
				if err
					console.log _err.stack
					cli.error( err )
				else
					cli.ok( JSON.stringify( resp, 1, 2 ) )
				process.exit()
				return
		catch _err
			console.log _err.stack
			cli.error( _err )

		return
	return

process.on "uncaughtException", ( _err )=>
	cli.error( _err )
	console.log _err.stack
	process.exit()
	return