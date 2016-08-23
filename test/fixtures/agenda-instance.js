var dynamoConfig = {accessKeyId: 'AKID', secretAccessKey: 'SECRET', region: "us-west-2", endpoint: "http://localhost:8000"};
var tests = process.argv.slice(3);

var path = require('path'),
    Agenda = require( path.join(__dirname, '..', '..', 'index.js' ) ),
    addTests = require( path.join(__dirname, 'addTests.js') );

var agenda = new Agenda({ adapter: require('../../index.js'), db: { credentials: dynamoConfig } }, function(err, collection) {

	tests.forEach(function(test) {
	  addTests[test](agenda);
	});

	agenda.start();

	// Ensure we can shut down the process from tests
	process.on('message', function(msg) {
	  if( msg == 'exit' ) process.exit(0);
	});

	// Send default message of "notRan" after 400ms
	setTimeout(function() {
	  process.send('notRan');
	  process.exit(0);
	}, 400);

});
