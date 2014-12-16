AWS = require('aws-sdk')
_ = require 'lodash'
async = require "async"

config = require( "./config" )

class Target extends require( "mpbasic" )( config )
	defaults: =>
		return @extend {}, super, 
			accessKeyId: null
			secretAccessKey:  null
			region: "eu-central-1"
			apiVersion: "2012-08-10"
			ReturnConsumedCapacity: "TOTAL"

			pumps: 10
			batchsize: 25

	constructor: ->
		super
		@checked = false

		@wait = 0
		@stateinfo = 
			capacityUnits: 0
			Retries: 0
			Errors: 0
			#iTodo: 0
		@pumping = false

		@_configure()
		return

	_configure: =>
		@debug "check", @config

		if not @config.accessKeyId
			@_handleError( false, "EMISSINGAWSACCESSKEY" )
			return

		if not @config.secretAccessKey
			@_handleError( false, "EMISSINGAWSSECRET" )
			return

		@client = new AWS.DynamoDB( @config )

		@checked = true
		@emit "checked"
		return

	migrate: ( tuple, cb )=>
		@debug "migrate attrs `#{tuple.key}`", tuple
		@update tuple, ( err, result )=>
			if err
				cb( err )
				return
			cb( null, result )
			return
		return

	pump: ( fnAddData, fnGetData )=>

		if @pumping
			return

		@pumping = true
		@emit "start.pump"
		@debug "start pump", fnAddData, fnGetData
		aPumps = for i in [ 1..@config.pumps ]
			@_pump( fnAddData, fnGetData )

		async.parallel aPumps, ( err, res )=>
			@debug "async.parallel return"
			@pumping = false
			@debug "stop pump"
			@emit "stop.pump"
			return
		return

	throttle: =>
		@wait += 50
		@debug( "throttle #{ @wait }" )
		return

	accelerate: =>
		if @wait - 50 <= 0
			@wait = 0
		else
			@wait -= 50
		@debug( "accelerate #{ @wait }" )
		return

	_pump: ( fnAddData, fnGetData )=>
		return ( cba )=>
			@debug "_pump"
			_sett = fnGetData( @config.batchsize )
			if _sett.length > 0
				@writeBatch _sett, fnAddData, =>
					@emit "data.pumped"
					@debug "writeBatch return wait #{@wait}"	
					_.delay( @_pump( fnAddData, fnGetData ), @wait, cba )
					return
			else
				@debug "writeBatch empty"
				cba()
			return

	writeBatch: ( datas, fnAddData, cb )=>
		_datas = []
		for tuple in datas
			if tuple.PutRequest?
				_datas.push( tuple )
			else
				_datas.push( @_createItem( tuple ) )

		_dynReq = @_createDynamoRequest( _datas )
		#console.log "_dynReq", JSON.stringify( _dynReq, 1, 2 )
		
		@client.batchWriteItem _dynReq, ( err, res )=>
			@debug "dynamo written", @wait, res?.ConsumedCapacity[ 0 ]?.CapacityUnits, res?.UnprocessedItems[ @config.table ]?.length or 0, res
			if _.isNumber( res?.ConsumedCapacity?[ 0 ]?.CapacityUnits )
				@stateinfo.capacityUnits += res?.ConsumedCapacity[ 0 ].CapacityUnits

			if not _.isEmpty( res?.UnprocessedItems )
				@throttle()
				_putR = res?.UnprocessedItems[ @config.table ]
				#@stateinfo.iTodo += _putR.length
				#@stateinfo.capacityUnits -= _putR.length
				@stateinfo.Retries += _putR.length
				
				@debug "retry", _putR
				fnAddData( _putR )
				cb()
				return
			else
				@accelerate()

			if err? and err.statusCode isnt 200
				@stateinfo.Errors += 1
				@emit "data.error", err, _dynReq
				@error( err )
				cb()
				return
			cb()
			return
		return

	_createDynamoRequest: ( _datas )=>
		ret = 
			RequestItems: {}
			ReturnConsumedCapacity: "TOTAL"
			#ReturnItemCollectionMetrics: "SIZE"

		_dyn = []

		for data in _datas
			try
				if data.PutRequest?
					_dyn.push data
				else
					item = @_createItem( data )
					if item?
						_dyn.push 
							PutRequest:
								Item: item
			catch _err
				@error "error creating dynamo item", _err, JSON.stringify( data, 1,2 )

		ret.RequestItems[ @config.table ] = _dyn

		ret

	_createItem: ( data )=> 
		return PutRequest:
			Item:data

	update: ( data, cb )=>
		params = 
			TableName: @config.table
			Item: @_createItem( data )
			ReturnConsumedCapacity: @config.ReturnConsumedCapacity
			ReturnValues: "NONE"

		@_putItem( params, cb )
		return

	# Dynamo Helper Methods
	_getItem: ( params, cb )=>
		@debug "getItem", params
		@client.getItem params, @_processDynamoItemReturn( cb )
		return

	_putItem: ( params, cb )=>
		@debug "putItem", params
		@client.putItem params, @_processDynamoPutReturn( cb )
		return

	_processDynamoItemReturn: ( cb )=>
		return ( err, rawData )=>
			if err
				@_processDynamoError( cb, err )
				return

			@debug "_processDynamoItemReturn raw", rawData

			attrs = @_convertItem( rawData.Item )
			
			@debug "_processDynamoItemReturn", attrs
			cb( null, attrs )
			return

	_processDynamoPutReturn: ( cb )=>
		return ( err, rawData )=>
			if err
				@_processDynamoError( cb, err )
				return

			#@debug "_processDynamoPutReturn raw", rawData
			
			attrs = {}

			@debug "_processDynamoPutReturn", attrs
			cb( null, attrs )
			return

	_convertItem: ( raw )=>
		attrs = {}
		for _k, _v of raw
			_type = Object.keys( _v )[ 0 ]
			switch _type
				when "S" 
					attrs[ _k ] = _v[ _type ]
				when "SS" 
					attrs[ _k ] = _v[ _type ]
				when "N" 
					attrs[ _k ] = parseFloat( _v[ _type ] )

		return attrs

	_processDynamoError: ( cb, err )=>
		if err.code is "ResourceNotFoundException"
			@_handleError( cb, "EDYNAMOMISSINGTABLE", err )
			return

		cb( err )
		return

	ERRORS: =>
		return @extend {}, super, 
			"EDYNAMOMISSINGTABLE": [ 400, "The dynamo table does not exist. Please generate it!" ]
			"EMISSINGAWSACCESSKEY": [ 401, "Missing AWS Access Key. Please define the option `--awsaccesskey` or it's shortcut -a`" ]
			"EMISSINGAWSSECRET": [ 401, "Missing AWS Secret. Please define the option `-awssecret` or it's shortcut `-s`" ]
			"ECHECKINVALID": [ 500, "Its not possible to run start if the check has been failed" ]

module.exports = new Target()