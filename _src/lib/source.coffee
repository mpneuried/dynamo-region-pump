AWS = require('aws-sdk')
_ = require 'lodash'

config = require( "./config" )

class Source extends require( "mpbasic" )( config )
	defaults: =>
		return @extend {}, super, 
			accessKeyId: null
			secretAccessKey:  null
			region: "eu-west-1"
			apiVersion: "2012-08-10"

	constructor: ->
		super
		@checked = false
		@c_= []

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

	descibeTable: ( shared, next, error, fns )=>
		_params = 
			TableName: @config.table

		@client.describeTable _params, ( err, raw )=>
			if err
				error( err )
				return
			console.log "\nSOURCE:", "\nItems:",raw.Table.ItemCount, "\nSize:", raw.Table.TableSizeBytes ," bytes \n"
			next()
			return

		return


	_processData: ( data )=>
		return data

	loadData: ( since, ExclusiveStartKey, cb )=>
		@info "loadData", since, ExclusiveStartKey
		if not @checked
			cb( @_handleError( true, "ECHECKINVALID" ) )
			return 

		# TODO `since` filter
		_params = 
			TableName: @config.table
		_params.ExclusiveStartKey = ExclusiveStartKey if ExclusiveStartKey?

		@client.scan _params, ( err, raw )=>
			
			@debug "select: items received: #{raw.Items?.length or "-"}", raw.LastEvaluatedKey
			if err
				cb( err )
				return
			
			if raw.LastEvaluatedKey?
				cb( null, @_processData( raw.Items ), raw.LastEvaluatedKey )
			else
				cb( null, @_processData( raw.Items ) )
			return
		return 

	ERRORS: =>
		return @extend {}, super, 
			"EMISSINGAWSACCESSKEY": [ 401, "Missing AWS Access Key. Please define the option `--awsaccesskey` or it's shortcut -a`" ]
			"EMISSINGAWSSECRET": [ 401, "Missing AWS Secret. Please define the option `-awssecret` or it's shortcut `-s`" ]
			"ECHECKINVALID": [ 500, "Its not possible to run start if the check has been failed" ]

module.exports = new Source()