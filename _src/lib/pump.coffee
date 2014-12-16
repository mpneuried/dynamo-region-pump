path = require('path')

low = require('lowdb')
_ = require('lodash')
entities = require("entities")

config = require( "./config" )
sourcedb = require( "./source" )
targetdb = require( "./target" )

module.exports = class Pump extends require( "mpbasic" )( config )
	defaults: =>
		return @extend {}, super, 
			domain: null
			since: null

	constructor: ->
		super
		@checked = false
		@importedRevisions = []
		@duplicateRevisions = []

		@on "migrate", targetdb.migrate
		@_maxSourceLength = 0
		targetdb.on "data.pumped", @_updateWriteCacheBar
		@on "data.loaded", @_updateWriteCacheBar

		targetdb.on "data.error", ( err, data )=>
			low( "errors").insert( { err: err, data: data } )
			#low.save()
			return

		@_configure()
		return

	_configure: =>
		@debug "check"

		
		@data = low( "logs" )
		#@data.autoSave = false
		_path = path.resolve(__dirname + "/../" ) + "/db.json"
		low.path = _path
		try
			low.load()
		@debug "save data to received `#{_path}`"

		@checked = true
		@emit "checked"
		return


	middleware: =>
		_error = false
		[ fns..., cb ] = arguments
		
		if not _.isFunction( fns[ 0 ] )
			data = fns.splice( 0 , 1 )[ 0 ]
		else
			data = {}
		
		_errorFn = ( error )->
			fns = []
			_error = true
			cb( error, data )
			return
	
		run = ->
			if not fns.length
				cb( null, data )
			else
				fn = fns.splice( 0 , 1 )[ 0 ]
				
				fn( data, ->
					run() if not _error
					return
				, _errorFn, fns )
		run()
	
		return


	start: ( cb )=>
		console.time( "duration" )
		@shared = {
			sourcedata: []
		}
		fnEnd = @onEnd( cb )
		@middleware @shared, sourcedb.descibeTable, @loadData(), ( err, shared )=>
			if err
				fnEnd( err )
				return
			if targetdb.pumping
				targetdb.once "stop.pump", fnEnd
			else
				fnEnd()
			return
		return

	onEnd: ( cb )=>
		return ( err )=>
			if err
				cb( err )
				return
			@info "all data migrated in:"
			console.timeEnd( "duration" )
			@info "duplicateRevisions", @duplicateRevisions
			cb( null, targetdb: targetdb.stateinfo, migration: duplicates: @duplicateRevisions.length )
			return

	loadData: ( _nextToken )=>
		return ( shared, next, error, fns )=>
			@debug "NEXT", _nextToken
			@emit "data.loaded"
			if not low( "_state_").get( "logs" )? 
				low( "_state_").insert( { id: "logs", _modified: 0 } )

			sourcedb.loadData @config.since, _nextToken, ( err, data, nextToken )=>
				if err
					error( err )
					return
				@_collectLoadData( data, shared, fns )
				
				# append the loading of next data
				if nextToken?
					fns.push( @loadData( nextToken ) )

				next()
				return

			return

	pullSetting: ( batchsize )=>
		@debug "pull data", @shared.sourcedata.length
		return @shared.sourcedata.splice( 0, batchsize )

	pushSetting: ( datas )=>
		@shared.sourcedata = @shared.sourcedata.concat( datas )
		return

	_collectLoadData: ( data, shared, fns )=>
		@emit "loadAllDataReceived", data.length
		for tuple in data
			_revID = tuple.key.S + "_" + tuple.rev?.S
			@debug "cretae rev id", _revID, tuple
			#console.log _revID
			if _revID in @importedRevisions
				@duplicateRevisions.push( _revID )
			else
				@importedRevisions.push( _revID )
				shared.sourcedata.push tuple
				#@data.insert( tuple )
				_modified = tuple.modified

		if not targetdb.pumping
			targetdb.pump( @pushSetting, @pullSetting )

		low( "_domain_state_").update( "logs", { id: "main", _modified: _modified } )
		return

	_write2Dynamo: ( data )=>
		return ( shared, next, error, fns )=>
			targetdb.migrate data, ( err, data )=>
				if err
					low( "#{ @config.domain}_errors" ).insert( error: err, data: data )
					return
				next()
				return
			return

	_updateWriteCacheBar: =>
		@_maxSourceLength = @shared.sourcedata.length if @shared.sourcedata.length > @_maxSourceLength
		@emit "writeData", @_maxSourceLength, @shared.sourcedata.length, targetdb.stateinfo
		#@info "targetdb state - done: #{} todo: #{}"
		return


	ERRORS: =>
		return @extend {}, super, 
			"EMISSINGDOMAIN": [ 401, "Missing Domain. Please define the option `--domain` or it's shortcut -d`" ]
			"EDOMAINNOTFOUND": [ 404, "Domain not found. The given domain has not been found in source SimpleDB" ]
			"ECHECKINVALID": [ 500, "Its not possible to run start if the check has been failed" ]